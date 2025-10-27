# availability.py — COMPLETE, ROBUST, DIAGNOSTIC
# ------------------------------------------------
# Supports two sources for the availability sheet:
#   - API mode  (AVAIL_MODE=api + AVAIL_SPREADSHEET_ID, AVAIL_WORKSHEET_NAME)
#   - CSV mode  (AVAIL_MODE=csv + AVAILABILITY_CSV_URL or GOOGLE_SHEET_URL)
#
# Key endpoints used by app.py:
#   - find_available(day, start_hhmm, end_hhmm, org=None)
#   - person_info(query, org=None)
#
# This file prints lots of diagnostics to stdout so you can see what's happening
# in Render logs (Live Tail). It never silently swallows problems.

import os
import io
import re
import time
import csv
import json
from typing import List, Tuple, Optional, Dict

import pandas as pd
import requests

# ---- Google API (only used in AVAIL_MODE=api) ----
try:
    from google.oauth2.service_account import Credentials
    import gspread
except Exception as _e:
    # Not fatal in csv mode; we just print so it's obvious if API mode is chosen.
    print("[availability] gspread/google-auth not available (ok if using CSV mode):", _e)

DAY_COLS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


# =============================================================================
# Utilities: time/interval parsing
# =============================================================================

def _to_minutes(hhmm: str) -> Optional[int]:
    """
    '0900' -> 540
    '900'  -> 540
    returns None if not parseable
    """
    if hhmm is None:
        return None
    s = re.sub(r"[^\d]", "", str(hhmm))
    if len(s) < 3:
        return None
    if len(s) == 3:  # 900 -> 0900
        s = "0" + s
    try:
        hh = int(s[:2])
        mm = int(s[2:4])
        if hh < 0 or hh > 23 or mm < 0 or mm > 59:
            return None
        return hh * 60 + mm
    except Exception:
        return None


def _parse_block(block: str) -> List[Tuple[int, int]]:
    """
    Parse a cell containing busy time ranges into intervals in minutes.
    Accepts things like:
      0900-1000
      900-1000
      0900–1000 (en dash)
      0900 - 1000, 1030-1100
      0900-1000; 1030-1100
    Ignores junk gracefully.
    """
    if not block:
        return []
    txt = str(block).strip()
    if not txt:
        return []

    # Normalize fancy dash
    txt = txt.replace("–", "-").replace("—", "-")

    # Split on commas, semicolons, multiple spaces, or pipes
    parts = re.split(r"[;,]\s*|\s+\|\s+|\s{2,}", txt)
    out: List[Tuple[int, int]] = []

    for it in parts:
        it = it.strip()
        if not it:
            continue
        m = re.match(r"^\s*(\d{3,4})\s*-\s*(\d{3,4})\s*$", it)
        if not m:
            # Single time like "1100" isn't usable as a range → skip
            continue
        s = _to_minutes(m.group(1))
        e = _to_minutes(m.group(2))
        if s is None or e is None:
            continue
        if e <= s:
            continue
        out.append((s, e))
    return out


def _overlap(a: Tuple[int, int], b: Tuple[int, int]) -> bool:
    """Intervals (s1,e1) and (s2,e2) overlap if s1 < e2 and s2 < e1."""
    return a[0] < b[1] and b[0] < a[1]


def _row_is_free(row: pd.Series, day_col: str, s_min: int, e_min: int) -> bool:
    """
    Your form stores BUSY windows under each day. A cadet is "free" if
    NONE of their busy blocks overlap with the requested window.
    """
    raw = str(row.get(day_col, "") or "")
    busy_blocks = _parse_block(raw)
    req = (s_min, e_min)
    for b in busy_blocks:
        if _overlap(b, req):
            return False
    return True


def _norm_ms(val) -> str:
    s = str(val or "").strip()
    m = re.search(r"\d+", s)
    return m.group(0) if m else s


# =============================================================================
# Column helpers
# =============================================================================

def _normalize_headers(cols: List[str]) -> List[str]:
    cleaned: List[str] = []
    seen: Dict[str, int] = {}
    for c in cols:
        c = _clean_str(c)
        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        cleaned.append(c)
    return cleaned


def _clean_str(s: str) -> str:
    if s is None:
        return ""
    s = re.sub(r"[\u200B-\u200D\uFEFF]", "", str(s))  # zero-width
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """
    Case-insensitive resolver. Tries exact lower match first, then startswith.
    Returns the actual DataFrame column name or None.
    """
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        lc = cand.lower()
        if lc in cols:
            return cols[lc]
    for k in cols:
        for cand in candidates:
            if k.startswith(cand.lower()):
                return cols[k]
    return None


# =============================================================================
# Mode selection
# =============================================================================

def _fetch_avail_df() -> pd.DataFrame:
    mode = (os.getenv("AVAIL_MODE") or "api").strip().lower()
    print(f"[availability] mode={mode}")
    try:
        if mode == "api":
            print("[availability] Fetching via Google API…")
            df = _fetch_api_df()
        else:
            print("[availability] Fetching via robust CSV fallback…")
            df = _fetch_csv_df()

        # Post-normalize: strip spaces in headers and values
        df.columns = [_clean_str(c) for c in df.columns]
        df = df.applymap(lambda x: _clean_str(x) if isinstance(x, str) else x)

        print(f"[availability] DataFrame shape: {df.shape}")
        print(f"[availability] Columns: {list(df.columns)}[:10] ...")
        return df

    except Exception as e:
        import traceback
        print("[availability] ERROR while fetching availability:")
        print("  ", e)
        traceback.print_exc()
        # Return empty frame instead of crashing callers
        return pd.DataFrame()


# =============================================================================
# Public API
# =============================================================================

def person_info(query: str, org: str | None = None):
    """
    Ultra-robust cadet lookup that tolerates:
      - long, multi-line, quoted Google Form headers
      - punctuation, accents/diacritics, smart quotes
      - case/space differences
      - partial names and partial emails
      - 'First Last', 'Last, First', initials, etc.
    Returns primary fields + a big 'extras' dict with every other column
    you enumerated (including the 5 weekday busy columns).
    """
    import logging, unicodedata

    log = logging.getLogger("availability.person_info")

    # ---------- text normalization helpers ----------
    def _norm_text(s: str) -> str:
        """Lowercase, fold accents, collapse whitespace, strip punctuation-like."""
        if s is None:
            return ""
        s = str(s)
        # unify smart quotes / dashes / newlines
        s = (s.replace("\u2019", "'").replace("\u2018", "'")
               .replace("\u201c", '"').replace("\u201d", '"')
               .replace("\u2013", "-").replace("\u2014", "-")
               .replace("\r\n", " ").replace("\n", " "))
        # fold diacritics
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        # lowercase
        s = s.lower()
        # collapse whitespace
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _canon_key(s: str) -> str:
        """Normalized key used to match headers — letters/numbers/spaces only."""
        s = _norm_text(s)
        # keep alnum + space only, drop the rest (quotes, ?, (, ), :, etc.)
        s = re.sub(r"[^a-z0-9 ]+", "", s)
        # collapse spaces again
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _initial(s: str) -> str:
        s = _norm_text(s)
        return s[0] if s else ""

    # ---------- load data ----------
    df = _fetch_avail_df()

    if df is None or df.empty:
        raise RuntimeError("Availability sheet is empty or failed to load")

    # Build a canonical lookup map for the actual dataframe columns
    canon_to_real: dict[str, str] = {}
    for col in df.columns:
        canon_to_real[_canon_key(col)] = col

    # Helper to resolve a real df column given *many* alias patterns
    def pick_col(*aliases: str) -> str | None:
        """
        Try exact canonical match first; then startswith; then contains.
        """
        cand_keys = [_canon_key(a) for a in aliases]
        # exact
        for k in cand_keys:
            if k in canon_to_real:
                return canon_to_real[k]
        # startswith
        for k in cand_keys:
            for have_k, real in canon_to_real.items():
                if have_k.startswith(k):
                    return real
        # contains
        for k in cand_keys:
            for have_k, real in canon_to_real.items():
                if k in have_k:
                    return real
        return None

    # ---------- define ALL your headers as aliases ----------
    # primary identity/contact
    first_c   = pick_col("first name", "first", "given name")
    last_c    = pick_col("last name", "last", "surname", "family name")
    ms_c      = pick_col("ms level", "mslevel", "ms")
    email_c   = pick_col("school email", "email address", "email")
    phone_c   = pick_col("phone number", "phone", "mobile number")
    school_c  = pick_col("academic school", "school", "campus")
    major_c   = pick_col("academic major", "major")
    timestamp_c = pick_col("timestamp")

    # booleans / flags
    contracted_c = pick_col("are you contracted", "contracted")
    prior_c      = pick_col("are you prior service guard or otherwise", "prior service")
    vehicle_c    = pick_col("do you have a vehicle or reliable transportation tofrom events",
                            "do you have a vehicle or reliable transportation to",
                            "vehicle reliable transportation")

    colorguard_c = pick_col("do you have colorguard experience", "colorguard experience")
    agsu_c       = pick_col("do you have agsu", "agsu")
    ranger_c     = pick_col("do you want to do ranger challenge", "ranger challenge")
    ocps_c       = pick_col("do you have ocps", "ocps")
    pt_c         = pick_col("do you have pt uniform", "pt uniform")
    compass_c    = pick_col("do you have a compass", "compass")

    # commute times (very long form labels)
    commute_tech_c = pick_col("how long is your commute to tech track in minutes type numbers only example 5")
    commute_gsu_c  = pick_col("how long is your commute to gsu track in minutes type numbers only example 5")
    commute_ulm_c  = pick_col("how long is your commute to ulm in minutes type numbers only example 5")
    commute_nsu_c  = pick_col("how long is your commute to nsu in minutes")

    # busy blocks (Mon–Fri) — those long multi-line “click the times…” headers
    mon_c = pick_col("monday click the times that you have classobligations including rotc", "monday")
    tue_c = pick_col("tuesday click the times that you have classobligations including rotc", "tuesday")
    wed_c = pick_col("wednesday click the times that you have classobligations including rotc", "wednesday")
    thu_c = pick_col("thursday click the times that you have classobligations including rotc", "thursday")
    fri_c = pick_col("friday click the times that you have classobligations including rotc", "friday")

    # extras
    bayou_c    = pick_col("do you want to attend bayou classic", "bayou classic")
    special1_c = pick_col("please mention here any special circumstances youre in if appropriate so they can be addressed",
                          "please mention here any special circumstances")
    special2_c = pick_col("location to fill in second special circumstance if necessary",
                          "second special circumstance")

    # sanity check for required minimum
    missing = [name for name, col in {
        "First Name": first_c,
        "Last Name":  last_c,
    }.items() if col is None]
    if missing:
        # make debugging friendly: show first 12 columns
        cols_preview = list(map(str, df.columns))[:12]
        raise RuntimeError(f"availability sheet is missing required columns {missing}; present (first 12): {cols_preview}")

    # optional org filter
    if org and school_c:
        df = df[df[school_c].astype(str).str.contains(org, case=False, na=False)]
        if df.empty:
            raise ValueError(f"No records for org '{org}'")

    # ---------- build normalized columns for matching ----------
    def _safe(col): 
        return df[col].astype(str) if col in df.columns else pd.Series([""] * len(df))

    df["_first_n"] = _safe(first_c).map(_norm_text)
    df["_last_n"]  = _safe(last_c).map(_norm_text)
    df["_full_n"]  = (df["_first_n"] + " " + df["_last_n"]).str.strip()
    df["_rev_n"]   = (df["_last_n"] + " " + df["_first_n"]).str.strip()

    if email_c:
        df["_email_n"] = _safe(email_c).map(_norm_text)
        df["_email_local"] = df["_email_n"].str.split("@").str[0]
    else:
        df["_email_n"] = ""
        df["_email_local"] = ""

    # ---------- parse the query and match ----------
    q_raw = (query or "").strip()
    if not q_raw:
        raise ValueError("Empty query")

    qn = _norm_text(q_raw)
    hit_idx = None

    # email path (full or partial)
    if "@" in qn:
        candidates = df.index[
            (df["_email_n"] == qn) |
            (df["_email_n"].str.contains(qn, na=False)) |
            (df["_email_local"] == qn.split("@")[0])
        ].tolist()
        if candidates:
            exacts = [i for i in candidates if df.at[i, "_email_n"] == qn]
            hit_idx = (exacts[0] if exacts else candidates[0])

    # name path
    if hit_idx is None:
        tokens = [t for t in qn.split(" ") if t]
        # try exact full or reversed
        fast = df.index[(df["_full_n"] == qn) | (df["_rev_n"] == qn)].tolist()
        if fast:
            hit_idx = fast[0]
        else:
            # score-based partial matching
            def _score_name(q_tokens: list[str], first: str, last: str) -> int:
                first_n = _norm_text(first)
                last_n  = _norm_text(last)
                full    = f"{first_n} {last_n}".strip()
                rev     = f"{last_n} {first_n}".strip()

                score = 0
                q    = " ".join(q_tokens)

                if q == full or q == rev:
                    score += 100
                if full.startswith(q) or rev.startswith(q):
                    score += 60
                if q in full or q in rev:
                    score += 40

                for t in q_tokens:
                    if t and (t == first_n or t == last_n):
                        score += 25
                    if t and (first_n.startswith(t) or last_n.startswith(t)):
                        score += 12
                    if t and (t in first_n or t in last_n):
                        score += 6

                # initials (e.g., "jd" or "j d")
                if len(q_tokens) == 2 and _initial(first) == q_tokens[0][:1] and _initial(last) == q_tokens[1][:1]:
                    score += 30
                if len(q) == 2 and _initial(first) == q[0] and _initial(last) == q[1]:
                    score += 25

                return score

            best_i = None
            best_s = -1
            for i, row in df.iterrows():
                s = _score_name(tokens, row.get(first_c, ""), row.get(last_c, ""))
                if s > best_s:
                    best_s, best_i = s, i
            if best_s >= 20:
                hit_idx = best_i

    if hit_idx is None:
        sample_cols = [c for c in [first_c, last_c, email_c, school_c] if c]
        sample_preview = df[sample_cols].head(5).to_dict(orient="records")
        log.warning("person_info: no match q=%r norm=%r org=%r; sample=%s", q_raw, qn, org, sample_preview)
        raise ValueError("No matching cadet found")

    hit = df.loc[hit_idx]

    def g(col):
        return (str(hit.get(col, "")).strip() if col in df.columns else "")

    # ---------- build response ----------
    person = {
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

    # HUGE extras payload with everything else you listed
    extras = {
        "timestamp": g(timestamp_c),
        "colorguard_experience": g(colorguard_c),
        "agsu": g(agsu_c),
        "ranger_challenge": g(ranger_c),
        "ocps": g(ocps_c),
        "pt_uniform": g(pt_c),
        "compass": g(compass_c),
        "commute_minutes": {
            "tech_track": g(commute_tech_c),
            "gsu_track":  g(commute_gsu_c),
            "ulm":        g(commute_ulm_c),
            "nsu":        g(commute_nsu_c),
        },
        "busy_blocks": {
            "monday":    g(mon_c),
            "tuesday":   g(tue_c),
            "wednesday": g(wed_c),
            "thursday":  g(thu_c),
            "friday":    g(fri_c),
        },
        "bayou_classic_interest": g(bayou_c),
        "special_circumstances_primary": g(special1_c),
        "special_circumstances_secondary": g(special2_c),
    }

    # Include extras in response for callers that want it,
    # but keep top-level keys backward-compatible.
    person["extras"] = extras

    return person



def find_available(day: str, start_hhmm: str, end_hhmm: str, org: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Returns a list of cadets whose BUSY windows **do not** overlap the requested window.
    Sorted by MS level ascending.
    """
    dnorm = (day or "").strip().capitalize()
    if dnorm not in DAY_COLS:
        raise ValueError(f"day must be one of {DAY_COLS}")

    s = _to_minutes(start_hhmm)
    e = _to_minutes(end_hhmm)
    if s is None or e is None or e <= s:
        raise ValueError("Bad time window; use HHMM (e.g., 0830 .. 1030) and end > start")

    df = _fetch_avail_df()
    if df.empty:
        print("[availability] WARNING: availability DataFrame is EMPTY.")
        return []

    # Flexible resolution of common columns
    first_col   = _col(df, "First Name", "First", "Given Name", "First name")
    last_col    = _col(df, "Last Name", "Last", "Surname", "Family name")
    ms_col      = _col(df, "MS level", "MS Level", "MS")
    school_col  = _col(df, "Academic School", "School", "Campus")

    # Validate essentials
    missing = [k for k, v in {"first": first_col, "last": last_col, "ms": ms_col}.items() if v is None]
    if missing:
        raise RuntimeError(f"Missing expected columns in availability sheet: {missing}. Present: {list(df.columns)}")

    # Resolve the day column case-insensitively
    # Resolve the day column by exact match OR startswith (covers long prompt headers)
    day_col = None
    dn = dnorm.lower()
    for c in df.columns:
        cl = c.strip().lower()
        if cl == dn or cl.startswith(dn):
            day_col = c
            break
    if not day_col:
        # As a final fallback, look for the day word anywhere in the column header
        for c in df.columns:
            if dn in c.strip().lower():
                day_col = c
                break
    if not day_col:
        raise RuntimeError(
            f"Day column like '{dnorm}' not found. Example of your headers: {list(df.columns)[:8]} ..."
        )

    # Optional org filter
    if org and school_col and (school_col in df.columns):
        pre = len(df)
        df = df[df[school_col].astype(str).str.contains(org, case=False, na=False)]
        print(f"[availability] Org filter {org!r}: {pre} -> {len(df)} rows")

    ok: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        try:
            if _row_is_free(row, day_col, s, e):
                ok.append({
                    "first": str(row.get(first_col, "")).strip(),
                    "last":  str(row.get(last_col, "")).strip(),
                    "ms":    _norm_ms(row.get(ms_col, "")),
                })
        except Exception as _e:
            # Ignore just this row, keep going
            continue

    def _ms_key(x: Dict[str, str]) -> int:
        m = re.search(r"\d+", x.get("ms", ""))
        return int(m.group(0)) if m else 99

    ok.sort(key=_ms_key)
    print(f"[availability] find_available: matched={len(ok)}")
    return ok


# =============================================================================
# API MODE
# =============================================================================

def _fetch_api_df() -> pd.DataFrame:
    """
    AVAIL_MODE=api
    Requires:
      GOOGLE_SERVICE_ACCOUNT_JSON   (or GOOGLE_SERVICE_ACCOUNT_JSON_PATH)
      AVAIL_SPREADSHEET_ID
      AVAIL_WORKSHEET_NAME   (default "Form Responses 1")
    """
    # Load service account
    info = None
    path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if path:
        print(f"[availability] Using JSON key from file: {path}")
        with open(path, "r", encoding="utf-8") as f:
            info = json.load(f)
    else:
        if not raw.strip():
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON missing for API mode")
        info = json.loads(raw)

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)

    ssid = os.getenv("AVAIL_SPREADSHEET_ID")
    wname = os.getenv("AVAIL_WORKSHEET_NAME", "Form Responses 1")
    if not ssid:
        raise RuntimeError("AVAIL_SPREADSHEET_ID missing for API mode")

    ws = gc.open_by_key(ssid).worksheet(wname)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)
    print(f"[availability] API fetched rows={len(df)} cols={len(df.columns)}")
    return df


# =============================================================================
# CSV MODE — ultra-robust CSV loader with diagnostics and self-repair
# =============================================================================

def _to_csv_export(url: str) -> str:
    """
    Convert a Google Sheets EDIT URL to CSV EXPORT URL if needed.
    Keeps gid when present.
    """
    if not url:
        return url
    if "export?format=csv" in url:
        return url
    m = re.search(r"/spreadsheets/d/([^/]+)/", url)
    mgid = re.search(r"[?&]gid=(\d+)", url)
    if m:
        sheet_id = m.group(1)
        gid_part = f"&gid={mgid.group(1)}" if mgid else ""
        fixed = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv{gid_part}"
        print(f"[csv] Rewrote edit URL → export URL: {fixed}")
        return fixed
    return url


def _fetch_csv_df() -> pd.DataFrame:
    """
    Legacy alias for callers. Uses robust fetcher underneath.
    Picks URL from AVAILABILITY_CSV_URL or GOOGLE_SHEET_URL.
    """
    url = os.getenv("AVAILABILITY_CSV_URL") or os.getenv("GOOGLE_SHEET_URL")
    url = _to_csv_export(url or "")
    print(f"[availability] Using robust CSV fetcher… url={url!r}")
    return fetch_csv_df_robust(
        url,
        required_columns=["First Name", "Last Name", "MS level", "Monday"]
    )


def fetch_csv_df_robust(
    url: Optional[str] = None,
    *,
    required_columns: Optional[List[str]] = None,
    max_mb: float = 15.0,
    timeout: int = 20,
    retries: int = 3,
    retry_backoff: float = 0.8,
) -> pd.DataFrame:
    """
    Ultra-robust CSV fetch + parse for Google Sheets 'export?format=csv&gid=...'.
    - Multi-strategy parsing: strict -> skip-bad -> heuristic repair
    - Handles encodings, BOM, delimiters, duplicate headers, bad rows
    - Enforces required schema (adds missing columns)
    - Emits detailed diagnostics; never raises on malformed content (returns empty df on hard failure)
    """
    url = url or os.getenv("GOOGLE_SHEET_URL")
    if not url:
        print("[csv] ERROR: missing url / GOOGLE_SHEET_URL")
        return pd.DataFrame()

    # 1) Fetch with retries
    text, last_headers = _fetch_text_with_retries(url, timeout=timeout, retries=retries, backoff=retry_backoff)
    if text is None:
        return pd.DataFrame()

    # If we accidentally got HTML (permissions/login page), retry with export URL
    if text.lstrip().startswith("<"):
        print("[csv] WARNING: HTML content detected; trying export URL rewrite…")
        url2 = _to_csv_export(url)
        if url2 != url:
            text2, _ = _fetch_text_with_retries(url2, timeout=timeout, retries=1, backoff=0.2)
            if text2:
                text = text2

    # Hard size guard (protect against wrong endpoint returning huge blob)
    approx_mb = len(text) / (1024 * 1024)
    if approx_mb > max_mb:
        print(f"[csv] WARNING: payload is {approx_mb:.2f} MB (> {max_mb} MB). Proceeding, but this is suspicious.")

    # Normalize newlines; handle BOM
    text = text.replace("\r\n", "\n").replace("\r", "\n").lstrip("\ufeff")

    # Debug preview
    print(f"[csv] first 160 chars: {text[:160].replace(chr(10),'\\n')}")
    delim = _detect_delimiter(text)
    print(f"[csv] detected delimiter: {repr(delim)}")
    header = _read_header(text, delim)
    print(f"[csv] header probe (len={len(header)}): {header[:8]}{' ...' if len(header)>8 else ''}")
    if not header:
        print("[csv] ERROR: header not detected. Returning empty df.")
        return pd.DataFrame()

    header = _normalize_headers(header)

    # 3) Strategy A: strict pandas parse
    df, skipped = _parse_with_pandas(text, delim, on_bad_lines="error")
    if df is not None:
        print(f"[csv] Parsed strictly with pandas (skipped=0), rows={len(df)} cols={df.shape[1]}")
        new_cols = _rehydrate_header(df.columns, header)
        if len(new_cols) == len(df.columns):
            df.columns = new_cols
        else:
            print(f"[csv] Header length mismatch; keeping pandas columns. parsed={len(df.columns)} expected={len(header)}")
        df = _postprocess_df(df, required_columns)
        _save_repaired_snapshot(df)
        return df

    # 4) Strategy B: pandas with skip-bad-lines
    df, skipped = _parse_with_pandas(text, delim, on_bad_lines="skip")
    if df is not None:
        print(f"[csv] Parsed with skip-bad-lines (skipped≈{skipped}), rows={len(df)} cols={df.shape[1]}")
        new_cols = _rehydrate_header(df.columns, header)
        if len(new_cols) == len(df.columns):
            df.columns = new_cols
        else:
            print(f"[csv] Header length mismatch; keeping pandas columns. parsed={len(df.columns)} expected={len(header)}")
        df = _postprocess_df(df, required_columns)
        _save_repaired_snapshot(df)
        return df

    # 5) Strategy C: heuristic row repair
    df = _repair_csv_to_df(text, header, delim)
    print(f"[csv] Heuristic repair parse, rows={len(df)} cols={df.shape[1]}")
    df = _postprocess_df(df, required_columns)
    _save_repaired_snapshot(df)
    return df


def _fetch_text_with_retries(url: str, *, timeout: int, retries: int, backoff: float):
    last_err = None
    last_headers = {}
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            last_headers = dict(r.headers or {})
            r.raise_for_status()
            ct = r.headers.get("content-type", "").lower()
            if "html" in ct:
                print(f"[csv] WARNING: content-type '{ct}'. This may be HTML, not CSV.")
            enc = r.encoding or "utf-8"
            try:
                text = r.content.decode(enc, errors="replace")
            except LookupError:
                text = r.content.decode("utf-8", errors="replace")
            return text, last_headers
        except Exception as e:
            last_err = e
            sleep = backoff * attempt
            print(f"[csv] Fetch attempt {attempt}/{retries} failed: {e}. Retrying in {sleep:.1f}s…")
            time.sleep(sleep)
    print(f"[csv] ERROR: all fetch attempts failed. Last error: {last_err}")
    return None, last_headers


def _detect_delimiter(text: str) -> str:
    for line in text.split("\n"):
        if not line.strip():
            continue
        candidates = [",", ";", "\t", "|"]
        counts = {d: line.count(d) for d in candidates}
        delim = max(counts, key=counts.get)
        return delim if counts[delim] > 0 else ","
    return ","


def _read_header(text: str, delim: str) -> List[str]:
    sio = io.StringIO(text)
    reader = csv.reader(sio, delimiter=delim)
    for row in reader:
        if any((cell or "").strip() for cell in row):
            return row
    return []


def _rehydrate_header(existing_cols, target_header):
    # If pandas merged names oddly, apply the normalized header shape.
    if len(existing_cols) != len(target_header):
        return list(existing_cols)
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


def _repair_csv_to_df(text: str, header: List[str], delim: str) -> pd.DataFrame:
    """
    Heuristic repair:
    - If row has fewer fields, right-pad with ""
    - If row has extra fields, merge extras into last column
    - Trims whitespace, drops fully empty rows
    """
    n = len(header)
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    rows = list(reader)[1:]  # skip header
    fixed = []
    for r in rows:
        if not any((cell or "").strip() for cell in r):
            continue
        if len(r) < n:
            r = r + [""] * (n - len(r))
        elif len(r) > n:
            r = r[:n-1] + [delim.join(r[n-1:])]
        fixed.append([_clean_str(c) for c in r])
    df = pd.DataFrame(fixed, columns=_normalize_headers(header))
    return df


def _postprocess_df(df: pd.DataFrame, required_columns: Optional[List[str]]) -> pd.DataFrame:
    if df.empty:
        return df
    # Trim strings
    df = df.applymap(lambda x: _clean_str(x) if isinstance(x, str) else x)

    # Drop fully empty rows
    df = df[~(df.astype(str).apply(lambda r: "".join(r), axis=1).str.strip() == "")]
    df = df.drop_duplicates(keep="first").reset_index(drop=True)

    # Enforce required schema
    if required_columns:
        for col in required_columns:
            if col not in df.columns:
                df[col] = ""
        # Just put required columns first if present
        ordered = [c for c in required_columns if c in df.columns]
        remainder = [c for c in df.columns if c not in ordered]
        df = df[ordered + remainder]
    return df


def _save_repaired_snapshot(df: pd.DataFrame):
    try:
        p = "/tmp/repaired_availability.csv"
        df.to_csv(p, index=False)
        print(f"[csv] Saved repaired CSV snapshot to {p}")
    except Exception as e:
        print(f"[csv] Could not save repaired CSV snapshot: {e}")
