import os, json, time, logging, datetime as dt
from collections import defaultdict

import gspread
from google.oauth2.service_account import Credentials
import gspread.utils as U

log = logging.getLogger(__name__)

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Attendance Roster")
GSU_HEADER_ROW = int(os.getenv("GSU_HEADER_ROW", "7"))
ULM_HEADER_ROW = int(os.getenv("ULM_HEADER_ROW", "7"))

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "900"))  # 15 min

# --------------------------- Simple cache -----------------------------
_cache: dict[str, tuple[float, object]] = {}

def _cache_get(key: str):
    ts_val = _cache.get(key)
    if not ts_val:
        return None
    ts, val = ts_val
    if time.time() - ts > CACHE_TTL_SECONDS:
        _cache.pop(key, None)
        return None
    return val

def _cache_set(key: str, val):
    _cache[key] = (time.time(), val)
    return val

# ----------------------- Google Sheets Client -------------------------
def _client():
    info = None
    if os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"):
        with open(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"),"r",encoding="utf-8") as f:
            info = json.load(f)
    else:
        info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","{}"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def _ws():
    key = "ws_obj"
    ws = _cache_get(key)
    if ws: return ws
    gc = _client()
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
    return _cache_set(key, ws)

# ---------------------------- Roster ----------------------------------
def list_roster(label: str):
    ws = _ws()
    cache_key = f"roster:{label}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    start_row = GSU_HEADER_ROW if label == "gsu" else ULM_HEADER_ROW
    rng = f"A{start_row}:C1007"
    vals = ws.get(rng)
    out = []
    for row in vals:
        name = (row[0] if len(row) > 0 else "").strip()
        ms   = (row[2] if len(row) > 2 else "").strip().replace("MS", "").replace("ms","").strip()
        if name:
            parts = name.split()
            first = " ".join(parts[:-1]) if len(parts) > 1 else parts[0]
            last  = parts[-1] if len(parts) > 1 else ""
            out.append({"first": first, "last": last, "ms": ms or ""})
    return _cache_set(cache_key, out)

# ------------------------------ Events --------------------------------
def list_events(label: str):
    ws = _ws()
    cache_key = f"events:{label}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    header_row = GSU_HEADER_ROW if label == "gsu" else ULM_HEADER_ROW
    rng = f"Q{header_row}:AS{header_row}"
    cols = ws.get(rng)
    headers = cols[0] if cols else []
    events = []
    col_index = 17  # Q
    for idx, h in enumerate(headers, start=col_index):
        h = (h or "").strip()
        if not h:
            continue
        if "+" in h:
            date_part, kind_part = h.split("+", 1)
            events.append({"header": h, "date": date_part.strip(), "kind": kind_part.strip(), "col": idx})
    return _cache_set(cache_key, events)

# ---------------------------- Day Records -----------------------------
def _row_bounds(label: str):
    start = GSU_HEADER_ROW+1 if label=="gsu" else ULM_HEADER_ROW+1
    end   = start + 1000
    return start, end

def read_event_records(label: str, event_col: int):
    ws = _ws()
    cache_key = f"day:{label}:{event_col}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    start, end = _row_bounds(label)
    rng = f"{U.rowcol_to_a1(start,1)}:{U.rowcol_to_a1(end,event_col)}"
    vals = ws.get(rng)

    rows = []
    for row in vals:
        name = (row[0] if len(row)>0 else "").strip()
        if not name:
            continue
        ms   = (row[2] if len(row)>2 else "").strip().replace("MS","").replace("ms","").strip()
        mark = (row[event_col-1] if len(row)>=event_col else "").strip().lower()
        present = 1 if mark in ("p","present","✔","✓","x","1") else 0
        ftr     = 1 if mark in ("ftr","unexcused","u","no","-","0") else 0
        excused = 1 if mark in ("e","excused") else 0
        rows.append({"name": name, "ms": ms or "", "present": present, "ftr": ftr, "excused": excused})
    return _cache_set(cache_key, rows)

# ------------------------------ Writes --------------------------------
def add_event_and_mark(label: str, header: str, marks: list[dict]):
    raise NotImplementedError("Writes are disabled in this build.")

# ------------------------------ Helpers -------------------------------
def _date_in_window(date_str: str, d1: dt.date|None, d2: dt.date|None) -> bool:
    try:
        d = dt.datetime.strptime(date_str, "%m/%d/%Y").date()
    except Exception:
        return False
    if d1 and d < d1: return False
    if d2 and d > d2: return False
    return True

def _week_start(d: dt.date) -> dt.date:
    return d - dt.timedelta(days=d.weekday())  # Monday

# ------------------------------ Leaderboard ---------------------------
def leaderboard(label: str, dfrom: dt.date|None, dto: dt.date|None, top: int = 10):
    cache_key = f"lb:{label}:{dfrom}:{dto}:{top}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    evs = list_events(label)
    by_cadet = {}
    for e in evs:
        if not _date_in_window(e["date"], dfrom, dto):
            continue
        rows = read_event_records(label, e["col"])
        for r in rows:
            name = r["name"]
            ms   = (r["ms"] or "").upper().replace("MS","").strip()
            st = by_cadet.setdefault(name, {"ms": ms or "", "present":0, "ftr":0, "excused":0, "sessions":0})
            st["present"] += int(r["present"])
            st["ftr"]     += int(r["ftr"])
            st["excused"] += int(r["excused"])
            st["sessions"]+= 1

    groups = {"ms1":[], "ms2":[], "ms3":[], "ms4":[], "ms5":[]}
    for name, st in by_cadet.items():
        msn = f"ms{st['ms']}" if st["ms"] in ("1","2","3","4","5") else "ms1"
        groups.setdefault(msn, []).append({"name": name, **st})

    def top_by_present(rows): return sorted(rows, key=lambda r: (-r["present"], -r["sessions"], r["name"]))[:top]
    def top_by_ftr(rows):     return sorted(rows, key=lambda r: (r["ftr"], -r["present"], r["name"]))[:top]

    out = {
        "ms1": top_by_present(groups.get("ms1", [])),
        "ms2": top_by_present(groups.get("ms2", [])),
        "ms3": top_by_ftr(groups.get("ms3", [])),
        "ms4": top_by_ftr(groups.get("ms4", [])),
        "ms5": top_by_ftr(groups.get("ms5", [])),
        "window": {
            "from": dfrom.strftime("%m/%d/%Y") if dfrom else None,
            "to":   dto.strftime("%m/%d/%Y")   if dto else None,
            "label": label,
            "top": top
        }
    }
    return _cache_set(cache_key, out)

def render_leaderboard_text(data: dict) -> str:
    def fmt_group(title, rows, mode):
        lines = [f"{title} — Top {len(rows)} ({'highest Present' if mode=='present' else 'fewest FTR'})"]
        for i, r in enumerate(rows, 1):
            nm = r["name"]; p=r["present"]; f=r["ftr"]; s=r["sessions"]
            if mode == "present":
                lines.append(f"   {i}. {nm}  ({p} Present; Sessions: {s})")
            else:
                lines.append(f"   {i}. {nm}  ({f} FTR; {p} Present; Sessions: {s})")
        return "\n".join(lines)
    ms1 = fmt_group("MS1", data.get("ms1",[]), "present")
    ms2 = fmt_group("MS2", data.get("ms2",[]), "present")
    ms3 = fmt_group("MS3", data.get("ms3",[]), "ftr")
    ms4 = fmt_group("MS4", data.get("ms4",[]), "ftr")
    ms5 = fmt_group("MS5", data.get("ms5",[]), "ftr")
    return "\n\n".join([ms1, "", ms2, "", ms3, "", ms4, "", ms5]).strip()

# ------------------------------ Day Text ------------------------------
def day_text_report(label: str, date_str: str) -> str:
    ev = None
    for e in list_events(label):
        if e["date"] == date_str:
            ev = e; break
    if not ev:
        return f"No event for {date_str}."

    rows = read_event_records(label, ev["col"])
    tP = sum(r["present"] for r in rows)
    tF = sum(r["ftr"]     for r in rows)
    tE = sum(r["excused"] for r in rows)

    lines = [f"{ev['date']}+{ev['kind']}", f"Present={tP}  FTR={tF}  Excused={tE}", ""]
    for r in rows:
        mark = "P" if r["present"] else ("E" if r["excused"] else ("FTR" if r["ftr"] else ""))
        lines.append(f"{r['name']} — {mark}")
    return "\n".join(lines)

# ------------------------------ Charts --------------------------------
def charts_weekly(label: str, dfrom: dt.date|None, dto: dt.date|None):
    """
    Aggregate totals by week (Monday anchor).
    Returns:
      {
        "labels": ["YYYY-MM-DD", ...],  # week start
        "totals": {"present":[...], "ftr":[...], "excused":[...]},
        "by_ms": {"MS1":[...], "MS2":[...], "MS3":[...], "MS4":[...], "MS5":[...]},
      }
    """
    cache_key = f"charts:{label}:{dfrom}:{dto}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    evs = list_events(label)
    buckets: dict[dt.date, dict] = {}

    for e in evs:
        if not _date_in_window(e["date"], dfrom, dto):
            continue
        try:
            d = dt.datetime.strptime(e["date"], "%m/%d/%Y").date()
        except Exception:
            continue
        wk = _week_start(d)
        b = buckets.setdefault(wk, {
            "present":0, "ftr":0, "excused":0,
            "ms": {"MS1":0,"MS2":0,"MS3":0,"MS4":0,"MS5":0}
        })
        rows = read_event_records(label, e["col"])
        for r in rows:
            b["present"] += int(r["present"])
            b["ftr"]     += int(r["ftr"])
            b["excused"] += int(r["excused"])
            mskey = f"MS{(r['ms'] or '1')}"
            if mskey not in b["ms"]:
                mskey = "MS1"
            if r["present"]:
                b["ms"][mskey] += 1

    if not buckets:
        return {"labels": [], "totals": {"present":[],"ftr":[],"excused":[]}, "by_ms":{"MS1":[],"MS2":[],"MS3":[],"MS4":[],"MS5":[]} }

    # sort by week
    weeks = sorted(buckets.keys())
    labels = [w.isoformat() for w in weeks]
    totals = {
        "present": [buckets[w]["present"] for w in weeks],
        "ftr":     [buckets[w]["ftr"]     for w in weeks],
        "excused": [buckets[w]["excused"] for w in weeks],
    }
    by_ms = {k:[buckets[w]["ms"][k] for w in weeks] for k in ["MS1","MS2","MS3","MS4","MS5"]}

    out = {"labels": labels, "totals": totals, "by_ms": by_ms}
    return _cache_set(cache_key, out)
