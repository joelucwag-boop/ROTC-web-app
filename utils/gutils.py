import os, json, re, logging
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

log = logging.getLogger(__name__)

# ---- Config ----
SHEET_ID  = os.environ.get("SPREADSHEET_ID", "")
TAB_NAME  = os.environ.get("WORKSHEET_NAME", "Attendance Roster")
GSU_HEADER_ROW = int(os.environ.get("GSU_HEADER_ROW", "6"))
ULM_HEADER_ROW = int(os.environ.get("ULM_HEADER_ROW", "78"))
AVAIL_CSV_URL  = os.environ.get("AVAILABILITY_CSV_URL", "")

ATT_COLOR_PRESENT = os.environ.get("ATT_COLOR_PRESENT","#00FF00")
ATT_COLOR_FTR     = os.environ.get("ATT_COLOR_FTR","#FF0000")
ATT_COLOR_OTHER   = os.environ.get("ATT_COLOR_OTHER","#FFFF00")

def load_service_account_from_env():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is empty.")
    try:
        data = json.loads(raw)
    except Exception:
        # maybe it's a filepath
        with open(raw,"r",encoding="utf-8") as f:
            data = json.load(f)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(data, scopes=scopes)
    return gspread.authorize(creds)

def _open_ws():
    gc = load_service_account_from_env()
    sh = gc.open_by_key(SHEET_ID)
    return sh.worksheet(TAB_NAME)

DATE_HEADER_RE = re.compile(r'(\d{1,2})/(\d{1,2})/(\d{2,4})')

def _extract_date_str(header_cell: str):
    if not header_cell: return None
    m = DATE_HEADER_RE.search(str(header_cell))
    if not m: return None
    m_, d_, y_ = m.groups()
    y = int(y_)
    if y < 100: y += 2000
    return f"{int(m_):02d}/{int(d_):02d}/{y:04d}"

def detect_header_row(rows):
    # Heuristic: first row that has First/Last/MS
    for i, row in enumerate(rows[:200]):
        low = [str(c).strip().lower() for c in row]
        has_first = any(("name" in c and "first" in c) or c == "first" for c in low)
        has_last  = any(("name" in c and "last"  in c) or c == "last"  for c in low)
        has_ms    = any(("ms" in c and "level" in c) or c == "ms" for c in low)
        if has_first and has_last and has_ms:
            return i, rows[i]
    # Fallback to env-provided
    return GSU_HEADER_ROW, rows[GSU_HEADER_ROW]

def get_attendance_dataframe():
    ws = _open_ws()
    rows = ws.get_all_values()
    if not rows or len(rows)<3:
        raise RuntimeError("Attendance sheet looks empty.")
    hdr_i, header = detect_header_row(rows)
    data = rows[hdr_i:]
    df = pd.DataFrame(data[1:], columns=header)
    return df

def list_attendance_dates():
    ws = _open_ws()
    rows = ws.get_all_values()
    hdr_i, header = detect_header_row(rows)
    header = rows[hdr_i]  # use the detected header row explicitly

    items = []
    for h in header:
        md = _extract_date_str(h)   # returns 'MM/DD/YYYY' or None
        if not md:
            continue
        m, d, y = md.split("/")
        try:
            y = int(y)
            iso = f"{y:04d}-{int(m):02d}-{int(d):02d}"
        except Exception:
            continue
        evt = _event_from_header(h)
        items.append({"iso": iso, "header": h, "event": evt})

    # de-dup + sort
    seen = set()
    out = []
    for it in sorted(items, key=lambda x: x["iso"]):
        if it["iso"] in seen:
            continue
        seen.add(it["iso"])
        out.append(it)
    return out


def _event_from_header(header_cell: str) -> str:
    if not header_cell:
        return ""
    s = str(header_cell).strip()
    m = re.search(r'\d{1,2}/\d{1,2}/\d{2,4}\s*([+\-–—:]\s*(.+))?$', s)
    if m and m.group(2):
        return m.group(2).strip()
    return ""

def _classify_status(raw: str):
    v = (raw or "").strip().lower()
    if not v: return None
    if v == "present": return "Present"
    if v == "ftr": return "FTR"
    if v.startswith("excused"): return "Excused"
    if v == "nfr" or v.startswith("nfr:"): return "NFR"
    return None

def build_present_rates_by_ms(df: pd.DataFrame):
    # Build per-date present rate for MS1..MS4
    # Columns detection
    cols = [c for c in df.columns if _extract_date_str(c)]
    ms_col = None
    for c in df.columns:
        s = str(c).strip().lower()
        if s in ("ms level","ms","mslevel"): ms_col = c
    if not ms_col:
        raise RuntimeError("Can't find 'MS Level' column.")
    ms_levels = ["1","2","3","4"]
    out = []
    # build ordered date info
    dlist = []
    for c in cols:
        md = _extract_date_str(c)
        m,d,y = md.split("/")
        iso = f"{y}-{int(m):02d}-{int(d):02d}"
        dlist.append((iso, c, _event_from_header(c)))
    dlist.sort(key=lambda t: t[0])
    for iso, col, evt in dlist:
        row = {"iso": iso, "event": evt}
        for ms in ms_levels:
            subset = df[(df[ms_col].astype(str).str.strip()==ms)]
            p=f=e=0
            for v in subset[col]:
                status = _classify_status(v)
                if status=="Present": p+=1
                elif status=="FTR": f+=1
                elif status=="Excused": e+=1
            denom = p+f+e
            row[f"MS{ms}"] = (p/denom*100.0) if denom>0 else None
        vals = [row[f"MS{m}"] for m in ["1","2","3","4"] if row[f"MS{m}"] is not None]
        row["AVG"] = sum(vals)/len(vals) if vals else None
        out.append(row)
    return out

def get_cadet_directory_rows():
    df = get_attendance_dataframe()
    # Build First Last and MS
    # Try to find name columns
    def _find(header_opts):
        for h in df.columns:
            s = str(h).strip().lower()
            for opt in header_opts:
                if opt == s: return h
        return None
    c_first = _find(["name first","first name","firstname","first","namefirst"])
    c_last  = _find(["name last","last name","lastname","last","namelast"])
    c_ms    = _find(["ms level","ms","mslevel"])
    if not all([c_first,c_last,c_ms]):
        raise RuntimeError("Could not find First/Last/MS columns.")
    first = df[c_first].astype(str).str.strip()
    last  = df[c_last].astype(str).str.strip()
    ms    = df[c_ms].astype(str).str.strip()
    names = (first + " " + last).str.strip()
    rows = []
    for n, m in zip(names, ms):
        if not n: continue
        rows.append({"Name": n, "MS": m})
    # Dedup by name keep first
    seen=set(); out=[]
    for r in rows:
        if r["Name"].lower() in seen: continue
        seen.add(r["Name"].lower()); out.append(r)
    out.sort(key=lambda r: (r["Name"].split(" ")[-1].lower(), r["Name"].split(" ")[0].lower()))
    return out

def get_availability_df():
    if not AVAIL_CSV_URL:
        raise RuntimeError("AVAILABILITY_CSV_URL not set.")
    df = pd.read_csv(AVAIL_CSV_URL)
    # Normalize a FullName column
    cols = {c.lower(): c for c in df.columns}
    first = df[cols.get("first name")].astype(str).str.strip() if "first name" in cols else ""
    last  = df[cols.get("last name")].astype(str).str.strip() if "last name" in cols else ""
    df["FullName"] = (first + " " + last).str.strip()
    return df

def _parse_time_window(s):
    # Accepts '09:00-11:30', '0900-1130', '09 00 - 11 30'
    s = re.sub(r"[^\d\-]", "", s or "")
    a, b = s.split("-")
    def minutes(hhmm):
        hh = int(hhmm[0:2]); mm = int(hhmm[2:4])
        return hh*60 + mm
    return minutes(a.zfill(4)), minutes(b.zfill(4))

def find_cadet_availability(day: str, window: str):
    day_norm = (day or "").strip().lower()
    # map a bunch of common variants to canonical column headers
    day_alias = {
        "m": "Monday", "mon": "Monday", "monday": "Monday",
        "t": "Tuesday", "tu": "Tuesday", "tue": "Tuesday", "tues": "Tuesday", "tuesday": "Tuesday",
        "w": "Wednesday", "wed": "Wednesday", "weds": "Wednesday", "wednesday": "Wednesday",
        "th": "Thursday", "thu": "Thursday", "thur": "Thursday", "thurs": "Thursday", "thursday": "Thursday",
        "f": "Friday", "fri": "Friday", "friday": "Friday",
    }
    col = day_alias.get(day_norm)
    if not col:
        # also allow prefix match like 'th' from 'Thursday'
        for k, v in day_alias.items():
            if day_norm.startswith(k):
                col = v; break
    if not col:
        raise RuntimeError("Day must be one of Monday..Friday")

    start, end = _parse_time_window(window)
    df = get_availability_df()
    if col not in df.columns:
        raise RuntimeError(f"Availability sheet has no '{col}' column")

    hits = []
    for _, row in df.iterrows():
        slots = str(row.get(col, "") or "")
        busy = False
        for token in slots.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                bstart, bend = _parse_time_window(token)
                if not (bend <= start or bstart >= end):
                    busy = True; break
            except Exception:
                continue
        if not busy:
            hits.append({"name": row.get("FullName", ""), "row": row.to_dict()})
    hits.sort(key=lambda r: r["name"].split(" ")[-1].lower())
    return hits


def _iso_to_mdyyyy(iso):
    # accepts 'YYYY-MM-DD' or already 'MM/DD/YYYY'
    if "/" in iso:
        return iso
    y, m, d = (int(x) for x in iso.strip().split("-"))
    return f"{int(m)}/{int(d)}/{y}"

def get_status_by_date_and_ms(df: pd.DataFrame, iso: str):
    md = _iso_to_mdyyyy(iso)  # 'M/D/YYYY'
    # find the date column by extracting MDY from each header cell
    date_col = None
    for c in df.columns:
        if _extract_date_str(c) == md:
            date_col = c
            break
    if not date_col:
        raise RuntimeError(f"Date {iso} not found in header.")
    ...


def build_leaderboards_like_ui95(df: pd.DataFrame, cadets: list):
    # cadets = [{"Name":..., "MS":...}]
    # Count over all date columns
    date_cols = [c for c in df.columns if _extract_date_str(c)]
    boards = {}
    for ms in ["1","2","3","4","5"]:
        rows=[]
        for c in cadets:
            if str(c["MS"]).strip()!=ms: continue
            name=c["Name"]
            present=ftr=0
            series = df.loc[df.apply(lambda r: (f"{r.get('Name First','').strip()} {r.get('Name Last','').strip()}").strip()==name, axis=1)]
            # If name matching fails, fallback on first/last columns via search
            if series.empty:
                # try looser: any row with same last and first
                parts=name.split(" ",1)
                fn=parts[0]; ln=parts[1] if len(parts)>1 else ""
                series = df[(df.filter(regex="(?i)^name.*first|^first").astype(str).apply(lambda s:s.str.strip()).eq(fn, axis=0)).any(axis=1) &
                            (df.filter(regex="(?i)^name.*last|^last").astype(str).apply(lambda s:s.str.strip()).eq(ln, axis=0)).any(axis=1)]
            if series.empty:
                continue
            s = series.iloc[0]
            for col in date_cols:
                val = str(s.get(col,"") or "")
                st = _classify_status(val)
                if st=="Present": present+=1
                elif st=="FTR": ftr+=1
            rows.append({"Name":name, "Present":present, "FTR":ftr})
        # Ranking
        if ms in ("1","2"):
            rows.sort(key=lambda r: (-r["Present"], r["Name"].split(" ")[-1].lower()))
        else:
            rows.sort(key=lambda r: (r["FTR"], -r["Present"], r["Name"].split(" ")[-1].lower()))
        boards[f"MS{ms}"] = rows[:20]
    return boards

def _today_label(event_suffix="PT"):
    from datetime import datetime
    now = datetime.now()
    return f"{now.month}/{now.day}/{now.year} + {event_suffix}"

def add_date_column_for_sections(event_suffix="PT"):
    """Append today's 'M/D/YYYY + suffix' to BOTH header rows (GSU & ULM)."""
    ws = _open_ws()
    rows = ws.get_all_values()
    gsu_i = GSU_HEADER_ROW
    ulm_i = ULM_HEADER_ROW
    header_gsu = rows[gsu_i]
    header_ulm = rows[ulm_i]

    label = _today_label(event_suffix)

    # GSU
    ws.update_cell(gsu_i+1, len(header_gsu)+1, label)
    # ULM
    ws.update_cell(ulm_i+1, len(header_ulm)+1, label)

    # return the ISO we will use in forms
    m, d, y = label.split("+")[0].strip().split("/")
    iso = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    return {"label": label, "iso": iso}


# ---- Update cell (writer) ----
def update_attendance_cell(cadet_name: str, iso_date: str, status: str, section="ANY"):
    ws = _open_ws()
    rows = ws.get_all_values()
    hdr_i, header = detect_header_row(rows)

    # find first/last indices
    first_i = last_i = None
    for j, h in enumerate(header):
        s = str(h).strip().lower()
        if s in ("name first","first name","firstname","first","namefirst"): first_i = j
        if s in ("name last","last name","lastname","last","namelast"):   last_i  = j

    # resolve date column
    date_col = None
    if iso_date == "__TODAY__":
        # use the last cell in the detected header
        date_col = len(header) - 1
    else:
        md = _iso_to_mdyyyy(iso_date)  # 'M/D/YYYY'
        for j, h in enumerate(header):
            if _extract_date_str(h) == md:
                date_col = j; break
    if date_col is None:
        raise RuntimeError("Date column not found.")

    # locate cadet row (respect section split)
    fn, ln = cadet_name.split(" ", 1) if " " in cadet_name else (cadet_name, "")
    target_row = None
    for i, r in enumerate(rows[hdr_i+1:], start=hdr_i+2):
        # section gate
        if i <= ULM_HEADER_ROW and section.upper() == "ULM":   continue
        if i >  ULM_HEADER_ROW and section.upper() == "GSU":   continue
        f = (r[first_i] if first_i is not None and first_i < len(r) else "").strip()
        l = (r[last_i]  if last_i  is not None and last_i  < len(r) else "").strip()
        if f.lower() == fn.lower() and l.lower() == ln.lower():
            target_row = i; break

    if not target_row:
        return False

    ws.update_cell(target_row, date_col+1, status)  # 1-indexed
    return True

