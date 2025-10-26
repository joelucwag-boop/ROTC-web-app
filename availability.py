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
def _fetch_csv_df():
    url = os.getenv("GOOGLE_SHEET_URL")
    if not url:
        raise RuntimeError("GOOGLE_SHEET_URL missing")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

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

