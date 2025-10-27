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
    header = ws.row_values(1+detect_header_row(ws.get_all_values())[0])
    items = []
    for h in header:
        md = _extract_date_str(h)
        if md:
            m, d, y = md.split("/")
            iso = f"{y}-{int(m):02d}-{int(d):02d}"
            evt = _event_from_header(h)
            items.append({"iso": iso, "header": h, "event": evt})
    items.sort(key=lambda x: x["iso"])
    return items

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
    # "0900-1130" -> (540, 690) minutes
    s = s.replace(":","")
    a,b = s.split("-")
    def minutes(hhmm):
        hh = int(hhmm[0:2]); mm=int(hhmm[2:4])
        return hh*60+mm
    return minutes(a), minutes(b)

def find_cadet_availability(day: str, window: str):
    df = get_availability_df()
    day_col_map = {
        "monday": "Monday",
        "tuesday": "Tuesday",
        "wednesday": "Wednesday",
        "thursday": "Thursday",
        "friday": "Friday"
    }
    col = None
    for k,v in day_col_map.items():
        if day.lower().startswith(k[:3]): col=v
    if not col or col not in df.columns:
        raise RuntimeError("Day must be one of Monday..Friday")
    start,end = _parse_time_window(window)
    hits = []
    for _, row in df.iterrows():
        slots = str(row[col] or "")
        # free if NOT within any busy segments the cadet listed
        busy = False
        for token in slots.split(","):
            token = token.strip()
            if not token: continue
            # tokens like 0900-0930
            try:
                bstart, bend = _parse_time_window(token.replace(" ", ""))
                # overlap?
                if not (bend <= start or bstart >= end):
                    busy = True; break
            except Exception:
                continue
        if not busy:
            hits.append({"name": row.get("FullName",""), "row": row.to_dict()})
    hits.sort(key=lambda r: r["name"].split(" ")[-1].lower())
    return hits

def _iso_to_mdyyyy(iso):
    y,m,d = (int(x) for x in iso.split("-"))
    return f"{m}/{d}/{y}"

def get_status_by_date_and_ms(df: pd.DataFrame, iso: str):
    md = _iso_to_mdyyyy(iso)
    date_col = None
    for c in df.columns:
        if _extract_date_str(c)==md:
            date_col = c; break
    if not date_col:
        raise RuntimeError(f"Date {iso} not found in header.")
    # find MS column
    ms_col = None
    for c in df.columns:
        if str(c).strip().lower() in ("ms level","ms","mslevel"):
            ms_col = c; break
    first_col = None; last_col=None
    for c in df.columns:
        s = str(c).strip().lower()
        if s in ("name first","first name","firstname","first","namefirst"): first_col=c
        if s in ("name last","last name","lastname","last","namelast"): last_col=c
    out = {"Present":[], "FTR":[], "Excused":[]}
    for _, row in df.iterrows():
        status = _classify_status(row.get(date_col,""))
        if status in out:
            name = f"{row.get(first_col,'').strip()} {row.get(last_col,'').strip()}".strip()
            ms = str(row.get(ms_col,"")).strip()
            out[status].append(f"{name} (MS{ms})")
    for k in out: out[k].sort(key=lambda s: s.split(" ")[-1].lower())
    return out

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

# ---- Update cell (writer) ----
def update_attendance_cell(cadet_name: str, iso_date: str, status: str, section="ANY"):
    ws = _open_ws()
    # Find the row for cadet
    rows = ws.get_all_values()
    hdr_i, header = detect_header_row(rows)
    first_i = None; last_i=None
    for j,h in enumerate(header):
        s=str(h).strip().lower()
        if s in ("name first","first name","firstname","first","namefirst"): first_i=j
        if s in ("name last","last name","lastname","last","namelast"): last_i=j
    date_col=None
    md = _extract_date_str(iso_date) if "/" in iso_date else f"{int(iso_date.split('-')[1]):02d}/{int(iso_date.split('-')[2]):02d}/{iso_date.split('-')[0]}"
    for j,h in enumerate(header):
        if _extract_date_str(h)==md:
            date_col=j; break
    if date_col is None: raise RuntimeError("Date column not found.")
    target_row=None
    fn, ln = cadet_name.split(" ",1) if " " in cadet_name else (cadet_name, "")
    for i,r in enumerate(rows[hdr_i+1:], start=hdr_i+2):
        if i<=ULM_HEADER_ROW and section.upper()=="ULM": continue
        if i>ULM_HEADER_ROW and section.upper()=="GSU": continue
        f = (r[first_i] if first_i is not None and first_i < len(r) else "").strip()
        l = (r[last_i] if last_i is not None and last_i < len(r) else "").strip()
        if f.lower()==fn.lower() and l.lower()==ln.lower():
            target_row=i; break
    if not target_row:
        return False
    ws.update_cell(target_row, date_col+1, status)  # gspread is 1-indexed
    return True
