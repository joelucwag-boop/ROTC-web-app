# availability.py
import os, io, json, re
import pandas as pd

# Optional: CSV fallback
import requests

# API auth
from google.oauth2.service_account import Credentials
import gspread

DAY_COLS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

# ---- time helpers ----
def _to_minutes(hhmm: str) -> int:
    hhmm = re.sub(r"[^\d]", "", str(hhmm))
    if len(hhmm) < 3:
        return None
    if len(hhmm) == 3:   # 900 -> 09:00
        hhmm = "0" + hhmm
    hh = int(hhmm[:2])
    mm = int(hhmm[2:4])
    return hh * 60 + mm

def _parse_block(block: str):
    """
    '0900-0930, 1000-1030' -> [(540, 570), (600, 630)]
    Accepts semi-colon/comma/space separated. Ignores garbage.
    """
    if not block:
        return []
    items = re.split(r"[;,]\s*|\s+\|\s+|\s{2,}", str(block).strip())
    out = []
    for it in items:
        m = re.match(r"^\s*(\d{3,4})\s*-\s*(\d{3,4})\s*$", it)
        if not m:
            # Single time like '1100' -> ignore as unusable range
            continue
        s = _to_minutes(m.group(1))
        e = _to_minutes(m.group(2))
        if s is None or e is None:
            continue
        if e <= s:
            continue
        out.append((s, e))
    return out

def _overlap(a, b):
    """ intervals (s1,e1) and (s2,e2) overlap if s1 < e2 and s2 < e1 """
    return a[0] < b[1] and b[0] < a[1]

def _row_is_free(row, day, start_min, end_min):
    # Busy intervals are what the form captured under the day’s column.
    # “Free” means there is **no** overlap with the requested window.
    txt = row.get(day, "")
    blocks = _parse_block(txt)
    req = (start_min, end_min)
    for b in blocks:
        if _overlap(b, req):
            return False
    return True

def _norm_ms(val):
    s = str(val).strip()
    m = re.search(r"\d+", s)
    return m.group(0) if m else s

# ---- API mode ----
# availability.py  (append at the end)
def _fetch_avail_df():
    mode = os.getenv("AVAIL_MODE", "api").lower()
    print(f"[availability] mode={mode}")
    try:
        if mode == "api":
            print("[availability] Fetching via Google API…")
            df = _fetch_api_df()
        else:
            print("[availability] Fetching via robust CSV fallback…")
            df = _fetch_csv_df()
        print(f"[availability] DataFrame shape: {df.shape}")
        print(f"[availability] Columns: {list(df.columns)}")
        return df
    except Exception as e:
        import traceback
        print("[availability] ERROR:", e)
        traceback.print_exc()
        return pd.DataFrame()
    mode = os.getenv("AVAIL_MODE", "api").lower()
    if mode == "api":
        return _fetch_api_df()
    return _fetch_csv_df()

def _col(df, *cands):
    cols = {c.lower(): c for c in df.columns}
    for cand in cands:
        if cand.lower() in cols:
            return cols[cand.lower()]
    # loose startswith
    for k in cols:
        for cand in cands:
            if k.startswith(cand.lower()):
                return cols[k]
    return None

def person_info(query: str, org: str | None = None):
    """
    query can be an email or 'First Last'. Returns a dict of fields.
    """
    df = _fetch_avail_df()

    first_c = _col(df, "First Name", "First", "First name")
    last_c  = _col(df, "Last Name", "Last", "Surname")
    email_c = _col(df, "School Email", "Email")
    phone_c = _col(df, "Phone Number", "Phone")
    ms_c    = _col(df, "MS level", "MS Level", "MS")
    school_c= _col(df, "Academic School", "School")
    major_c = _col(df, "Academic Major", "Major")
    contracted_c = _col(df, "Are you contracted?", "contracted")
    prior_c = _col(df, "Are you prior service? (Guard or otherwise)", "prior service")
    vehicle_c = _col(df, "Do you have a vehicle or reliable transportation to?", "vehicle")

    if org and school_c in df.columns:
        df = df[df[school_c].astype(str).str.contains(org, case=False, na=False)]

    q = str(query).strip().lower()

    hit = None
    if email_c and q and "@" in q:
        m = df[df[email_c].astype(str).str.lower() == q]
        if not m.empty:
            hit = m.iloc[0]
    if hit is None and first_c and last_c:
        # split "First Last"
        parts = q.split()
        if len(parts) >= 2:
            f, l = parts[0], parts[-1]
            m = df[(df[first_c].astype(str).str.lower() == f) &
                   (df[last_c].astype(str).str.lower() == l)]
            if not m.empty:
                hit = m.iloc[0]

    if hit is None:
        raise ValueError("No matching cadet found")

    # Build card (exclude Mon–Fri busy columns on purpose)
    def g(col): 
        return (str(hit.get(col, "")).strip() if col in df.columns else "")

    return {
        "first": g(first_c),
        "last": g(last_c),
        "ms": g(ms_c),
        "email": g(email_c),
        "phone": g(phone_c),
        "school": g(school_c),
        "major": g(major_c),
        "contracted": g(contracted_c),
        "prior_service": g(prior_c),
        "vehicle": g(vehicle_c),
    }



def _fetch_api_df():
    info = None
    if os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"):
        with open(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"), "r", encoding="utf-8") as f:
            info = json.load(f)
    else:
        info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "{}"))

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(os.getenv("AVAIL_SPREADSHEET_ID")).worksheet(os.getenv("AVAIL_WORKSHEET_NAME", "Form Responses 1"))
    # get_all_records -> header -> dict rows
    df = pd.DataFrame(ws.get_all_records())
    return df

# ---- CSV mode (fallback) ----
# robust_csv_loader.py
import os, io, time, csv, re, requests
import pandas as pd

# ---------------------------
# Public entry point
# ---------------------------
def fetch_csv_df_robust(
    url: str | None = None,
    *,
    required_columns: list[str] | None = None,
    max_mb: float = 15.0,
    timeout: int = 20,
    retries: int = 3,
    retry_backoff: float = 0.8,
) -> pd.DataFrame:
    """
    Ultra-robust CSV fetch + parse for Google Sheets 'export?format=csv&gid=...'.
    - Multi-strategy parsing: strict -> skip-bad -> repair
    - Handles encodings, BOM, delimiters, duplicate headers, bad rows
    - Optionally enforces `required_columns` (adds empty if missing)
    - Prints diagnostics; never throws on malformed content (returns empty df on hard failure)
    """
    url = url or os.getenv("GOOGLE_SHEET_URL")
    if not url:
        print("[csv] ERROR: missing url / GOOGLE_SHEET_URL")
        return pd.DataFrame()

    # 1) Fetch with retries
    text = _fetch_text_with_retries(url, timeout=timeout, retries=retries, backoff=retry_backoff)
    if text is None:
        return pd.DataFrame()

    # Hard size guard (protect against wrong endpoint returning HTML blob)
    approx_mb = len(text) / (1024 * 1024)
    if approx_mb > max_mb:
        print(f"[csv] WARNING: payload is {approx_mb:.2f} MB (> {max_mb} MB). Proceeding, but this is suspicious.")

    # Normalize newlines; handle BOM
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.lstrip("\ufeff")

    # 2) Header probe and delimiter detect
    delim = _detect_delimiter(text)
    header = _read_header(text, delim)
    if not header:
        print("[csv] ERROR: header not detected. Returning empty df.")
        return pd.DataFrame()

    header = _normalize_headers(header)
    n_cols = len(header)

    # 3) Strategy A: strict pandas parse
    df, skipped = _parse_with_pandas(text, delim, on_bad_lines="error")
    if df is not None:
        print(f"[csv] Parsed strictly with pandas (skipped=0), rows={len(df)} cols={df.shape[1]}")
        df.columns = _rehydrate_header(df.columns, header)
        df = _postprocess_df(df, required_columns)
        return df

    # 4) Strategy B: pandas with skip-bad-lines
    df, skipped = _parse_with_pandas(text, delim, on_bad_lines="skip")
    if df is not None:
        print(f"[csv] Parsed with skip-bad-lines (skipped≈{skipped}), rows={len(df)} cols={df.shape[1]}")
        df.columns = _rehydrate_header(df.columns, header)
        df = _postprocess_df(df, required_columns)
        return df

    # 5) Strategy C: heuristic row repair
    df = _repair_csv_to_df(text, header, delim)
    print(f"[csv] Heuristic repair parse, rows={len(df)} cols={df.shape[1]}")
    df = _postprocess_df(df, required_columns)
    return df


# ---------------------------
# Fetch helpers
# ---------------------------
def _fetch_text_with_retries(url: str, *, timeout: int, retries: int, backoff: float) -> str | None:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            ct = r.headers.get("content-type", "").lower()
            if "html" in ct:
                # Sometimes Google returns an HTML interstitial (permissions / auth)
                print(f"[csv] WARNING: content-type '{ct}'. This may be HTML, not CSV.")
            # Encoding handling
            enc = r.encoding or "utf-8"
            try:
                text = r.content.decode(enc, errors="replace")
            except LookupError:
                text = r.content.decode("utf-8", errors="replace")
            return text
        except Exception as e:
            last_err = e
            sleep = backoff * attempt
            print(f"[csv] Fetch attempt {attempt}/{retries} failed: {e}. Retrying in {sleep:.1f}s…")
            time.sleep(sleep)
    print(f"[csv] ERROR: all fetch attempts failed. Last error: {last_err}")
    return None


# ---------------------------
# Parsing helpers
# ---------------------------
def _detect_delimiter(text: str) -> str:
    # Probe first non-empty line to guess delimiter
    for line in text.split("\n"):
        if not line.strip():
            continue
        candidates = [",", ";", "\t", "|"]
        counts = {d: line.count(d) for d in candidates}
        delim = max(counts, key=counts.get)
        # If no delimiter appears, fall back to comma
        return delim if counts[delim] > 0 else ","
    return ","


def _read_header(text: str, delim: str) -> list[str]:
    sio = io.StringIO(text)
    reader = csv.reader(sio, delimiter=delim)
    for row in reader:
        if any(cell.strip() for cell in row):
            return row
    return []


def _normalize_headers(cols: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen = {}
    for c in cols:
        c = _clean_str(c)
        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        cleaned.append(c)
    return cleaned


def _rehydrate_header(existing_cols, target_header):
    """
    If pandas merged names oddly, apply the normalized header shape.
    """
    if len(existing_cols) != len(target_header):
        return target_header  # force header shape
    # keep existing but normalized names preferred from target
    return target_header


def _parse_with_pandas(text: str, delim: str, *, on_bad_lines: str):
    """
    Returns (df, skipped_estimate) or (None, None) on hard failure.
    """
    raw_lines = text.count("\n") + 1
    try:
        df = pd.read_csv(
            io.StringIO(text),
            dtype=str,
            sep=delim,
            engine="python",         # required for on_bad_lines
            on_bad_lines=on_bad_lines,
            keep_default_na=False,   # empty -> ""
            quoting=csv.QUOTE_MINIMAL
        )
        df.columns = [str(c) for c in df.columns]
        parsed = len(df.index) + 1  # + header
        skipped = max(raw_lines - parsed, 0)
        return df, skipped
    except Exception as e:
        print(f"[csv] pandas parse ({on_bad_lines=}) failed: {e}")
        return None, None


def _repair_csv_to_df(text: str, header: list[str], delim: str) -> pd.DataFrame:
    """
    Heuristic repair:
    - If row has fewer fields, right-pad with ""
    - If row has extra fields, merge extras into last column
    - Trims whitespace, drops fully empty rows
    """
    n = len(header)
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    rows = list(reader)[1:]  # skip original header
    fixed = []
    for r in rows:
        # If completely empty row, skip
        if not any((cell or "").strip() for cell in r):
            continue
        if len(r) < n:
            r = r + [""] * (n - len(r))
        elif len(r) > n:
            r = r[:n-1] + [delim.join(r[n-1:])]
        fixed.append([_clean_str(c) for c in r])

    df = pd.DataFrame(fixed, columns=_normalize_headers(header))
    
    
    try:
        repaired_path = "/tmp/repaired_availability.csv"
        df.to_csv(repaired_path, index=False)
        print(f"[csv] Saved repaired CSV to {repaired_path}")
    except Exception as e:
        print(f"[csv] Could not save repaired CSV: {e}")

    return df


# ---------------------------
# Post-processing
# ---------------------------
def _postprocess_df(df: pd.DataFrame, required_columns: list[str] | None) -> pd.DataFrame:
    # Trim whitespace on all strings
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Replace common "null" spellings with empty
    nullish = {"na", "n/a", "null", "none", "nil", "nan"}
    df = df.replace({c: {v: "" for v in nullish} for c in df.columns}, regex=False)

    # Drop fully empty rows
    df = df[~(df.astype(str).apply(lambda r: "".join(r), axis=1).str.strip() == "")]

    # De-duplicate rows
    before = len(df)
    df = df.drop_duplicates(keep="first").reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        print(f"[csv] Dropped {dropped} duplicate row(s).")

    # Enforce required schema
    if required_columns:
        for col in required_columns:
            if col not in df.columns:
                df[col] = ""
        # Reorder to required-first then the rest
        remainder = [c for c in df.columns if c not in required_columns]
        df = df[required_columns + remainder]

    return df


def _clean_str(s: str) -> str:
    # Remove invisible control chars, normalize spaces, trim
    if s is None:
        return ""
    s = re.sub(r"[\u200B-\u200D\uFEFF]", "", str(s))  # zero-width chars
    s = s.replace("\xa0", " ")
    return s.strip()


def find_available(day: str, start_hhmm: str, end_hhmm: str, org: str | None = None):
    day = day.strip().capitalize()
    if day not in DAY_COLS:
        raise ValueError(f"day must be one of {DAY_COLS}")

    s = _to_minutes(start_hhmm)
    e = _to_minutes(end_hhmm)
    if s is None or e is None or e <= s:
        raise ValueError("Bad time window; use HHMM (e.g., 0830 .. 1030) and end > start")

    mode = os.getenv("AVAIL_MODE", "api").lower()
    if mode == "api":
        df = _fetch_api_df()
    else:
        df = _fetch_csv_df()

    # Normalize headers we need
    # Guess columns (your sheet shows these exact headers):
    #   - "First Name" / "Last Name" (or similar)
    #   - "MS level" (exact)
    #   - "Academic School" (exact)
    #   - Day columns: Monday..Friday
    # Try to map flexible name variants:
    cols = {c.lower(): c for c in df.columns}

    def col_like(*cands):
        for c in cands:
            if c.lower() in cols:
                return cols[c.lower()]
        # try startswith match
        for k in cols:
            for c in cands:
                if k.startswith(c.lower()):
                    return cols[k]
        return None

    first_col = col_like("First Name", "First", "Given Name", "First name")
    last_col  = col_like("Last Name", "Last", "Surname", "Family name")
    ms_col    = col_like("MS level", "MS Level", "MS")
    school_col= col_like("Academic School", "School", "Campus")

    # If any required columns are missing, fail clearly:
    missing = [name for name, col in {
        "first": first_col, "last": last_col, "ms": ms_col
    }.items() if col is None]
    if missing:
        raise RuntimeError(f"Missing expected columns in availability sheet: {missing}. Present: {list(df.columns)}")

    # Make sure day column exists
    if day not in df.columns:
        # Try case-insensitive match
        for c in df.columns:
            if c.strip().lower() == day.lower():
                day = c
                break
    if day not in df.columns:
        raise RuntimeError(f"Day column '{day}' not found in sheet. Columns: {list(df.columns)}")

    # Optional org filter (GSU / ULM / LATECH, etc.)
    if org and school_col and school_col in df.columns:
        df = df[df[school_col].astype(str).str.contains(org, case=False, na=False)]

    # Keep rows where the window does NOT overlap any busy block
    ok = []
    for _, row in df.iterrows():
        try:
            if _row_is_free(row, day, s, e):
                ok.append({
                    "first": str(row.get(first_col, "")).strip(),
                    "last":  str(row.get(last_col, "")).strip(),
                    "ms":    _norm_ms(row.get(ms_col, "")),
                    # You can expose phone/email too if you wish:
                    # "email": row.get(col_like("School Email","Email"), ""),
                    # "phone": row.get(col_like("Phone Number","Phone"), ""),
                })
        except Exception:
            continue

    # Sort by MS level (numeric first)
    def _ms_key(x):
        m = re.search(r"\d+", x.get("ms",""))
        return int(m.group(0)) if m else 99
    ok.sort(key=_ms_key)
    return ok

def _fetch_csv_df():
    """Legacy alias — redirect to robust loader for compatibility."""
    url = os.getenv("GOOGLE_SHEET_URL")
    print("[availability] Using robust CSV fetcher…")
    return fetch_csv_df_robust(url)


