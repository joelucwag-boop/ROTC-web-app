import os, io, csv, re, time, json, logging, unicodedata, datetime as dt
from typing import Dict, List, Any, Optional
import requests

log = logging.getLogger(__name__)

GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL", "").strip()
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "900"))  # 15 min

# ------------------------------- cache -------------------------------
_cache: dict[str, tuple[float, Any]] = {}

def _cache_get(key: str):
    item = _cache.get(key)
    if not item:
        return None
    ts, val = item
    if time.time() - ts > CACHE_TTL_SECONDS:
        _cache.pop(key, None)
        return None
    return val

def _cache_set(key: str, val: Any):
    _cache[key] = (time.time(), val)
    return val

# ---------------------------- normalization --------------------------
def _ascii_fold(s: str) -> str:
    s = s or ""
    # normalize quotes and whitespace
    s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"\s+", " ", s).strip()
    # remove accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s

def _norm_name(s: str) -> str:
    return _ascii_fold(s).lower()

def _split_name(full: str) -> tuple[str, str]:
    parts = _ascii_fold(full).split()
    if not parts:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (" ".join(parts[:-1]), parts[-1])

# ------------------------------- CSV ---------------------------------
def _fetch_csv_text() -> str:
    if not GOOGLE_SHEET_URL:
        raise RuntimeError("GOOGLE_SHEET_URL not set")
    ck = "csv_text"
    cached = _cache_get(ck)
    if cached is not None:
        return cached
    r = requests.get(GOOGLE_SHEET_URL, timeout=20)
    r.raise_for_status()
    text = r.text
    return _cache_set(ck, text)

def _read_rows() -> list[dict]:
    ck = "rows_parsed"
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    text = _fetch_csv_text()
    buf = io.StringIO(text)
    rdr = csv.DictReader(buf)
    rows = []
    for raw in rdr:
        # fold keys to a canonical short key set
        rows.append(_canon_row(raw))
    return _cache_set(ck, rows)

# ------------------------- column canonicalization --------------------
# map many possible header variants to canonical keys
COLMAP = {
    "timestamp": ["timestamp"],
    "email": ["email address","school email","email"],
    "last": ["last name","surname"],
    "first": ["first name","given name"],
    "ms": ["ms level","ms", "ms class"],
    "contracted": ["are you contracted ?","contracted"],
    "prior_service": ["are you prior service? (guard or otherwise)","prior service"],
    "major": ["academic major"],
    "phone": ["phone number","phone"],
    "vehicle": ["do you have a vehicle or reliable transportation to/from events?","vehicle"],
    "school": ["academic school","school"],
    "commute_tech": ["how long is your commute to tech track? ( in minutes ) type numbers only example '5'"],
    "commute_gsu":  ["how long is your commute to gsu track? (in minutes) type numbers only example '5'"],
    "commute_ulm":  ["how long is your commute to ulm? ( in minutes) type numbers only example '5'"],
    "commute_nsu":  ["how long is your commute to nsu? ( in minutes )"],
    "colorguard": ["do you have colorguard experience?","colorguard experience"],
    "agsu": ["do you have agsu? (this is the last time we'll ask this, this semester )","agsu"],
    "ranger": ["do you want to do ranger challenge?","ranger challenge"],
    "ocps": ["do you have ocps?","ocp","ocps"],
    "pt_uniform": ["do you have pt uniform?","pt uniform"],
    "compass": ["do you have a compass?","compass"],
    "monday": ["monday\nclick the times that you have class/obligations  \n ( including rotc ) ","monday"],
    "tuesday": ["tuesday\nclick the times that you have class/obligations  \n ( including rotc ) ","tuesday"],
    "wednesday": ["wednesday\nclick the times that you have class/obligations  \n ( including rotc ) ","wednesday"],
    "thursday": ["thursday \nclick the times that you have class/obligations  \n ( including rotc ) ","thursday"],
    "friday": ["friday\nclick the times that you have class/obligations  \n ( including rotc ) ","friday"],
    "bayou": ["do you want to attend bayou classic?","bayou classic"],
    "special1": ["please mention here any special circumstances you're in (if appropriate) so they can be addressed if possible and considered when it comes to your current attendance record and your availability/ consideration for future privileges or taskings.\n(type n/a if none)"],
    "special2": ["[location to fill in second special circumstance if necessary]\n \nplease mention here any special circumstances you're in (if appropriate) so they can be addressed if possible and considered when it comes to your current attendance record and your availability/ consideration for future privileges or taskings.\n(type n/a if none)"],
}

def _canon_key(h: str) -> Optional[str]:
    h0 = _norm_name(h)
    for canon, variants in COLMAP.items():
        for v in variants:
            if _norm_name(v) == h0:
                return canon
    # attempt loose contains match for day columns etc.
    if "monday" in h0: return "monday"
    if "tuesday" in h0: return "tuesday"
    if "wednesday" in h0: return "wednesday"
    if "thursday" in h0: return "thursday"
    if "friday" in h0: return "friday"
    return None

def _canon_row(raw: dict) -> dict:
    out = {k: "" for k in COLMAP.keys()}
    # keep unknowns too
    for k, v in raw.items():
        ck = _canon_key(k) or k
        out[ck] = (v or "").strip()
    # derived names
    first = out.get("first","").strip()
    last  = out.get("last","").strip()
    if not first or not last:
        # try to split if a combined name present in either column
        nm = (raw.get("Name") or raw.get("name") or "").strip()
        if nm and not (first or last):
            f,l = _split_name(nm)
            if f: out["first"] = f
            if l: out["last"]  = l
    return out

# -------------------------- busy block parsing ------------------------
TIME_RE = re.compile(r"^\s*(\d{3,4})\s*-\s*(\d{3,4})\s*$")

def _to_min(hhmm: str) -> Optional[int]:
    hhmm = hhmm.strip()
    if len(hhmm) in (3,4):
        if len(hhmm) == 3: hhmm = "0"+hhmm
        h = int(hhmm[:2]); m = int(hhmm[2:])
        if 0 <= h < 24 and 0 <= m < 60:
            return h*60+m
    return None

def _parse_ranges(s: str) -> list[tuple[int,int]]:
    """Return list of [start_min,end_min] for '0900-0930, 0930-1000' etc."""
    out = []
    s = (s or "").replace("–","-").replace("—","-")
    for piece in re.split(r"[,\s]+", s):
        if not piece or "-" not in piece: 
            continue
        m = TIME_RE.match(piece)
        if not m:
            # tolerate '0900-0930,' with comma stuck
            piece = piece.strip(",")
            m = TIME_RE.match(piece)
        if not m: 
            continue
        a, b = m.group(1), m.group(2)
        ta, tb = _to_min(a), _to_min(b)
        if ta is not None and tb is not None and tb > ta:
            out.append((ta, tb))
    return out

def _day_busy_map(row: dict) -> dict:
    return {
        "monday":    _parse_ranges(row.get("monday","")),
        "tuesday":   _parse_ranges(row.get("tuesday","")),
        "wednesday": _parse_ranges(row.get("wednesday","")),
        "thursday":  _parse_ranges(row.get("thursday","")),
        "friday":    _parse_ranges(row.get("friday","")),
    }

def _overlaps(blocks: list[tuple[int,int]], a: int, b: int) -> bool:
    for s,e in blocks:
        if not (e <= a or s >= b):  # overlap
            return True
    return False

# ------------------------------ public API ----------------------------
def list_all_cadets() -> list[dict]:
    rows = _read_rows()
    out = []
    for r in rows:
        first = (r.get("first") or "").strip()
        last  = (r.get("last") or "").strip()
        ms    = (r.get("ms") or "").strip().replace("MS","").replace("ms","").strip()
        if first or last:
            out.append({"first": first, "last": last, "ms": ms})
    return out

def _match_row(rows: list[dict], name_or_email: str) -> Optional[dict]:
    q = _norm_name(name_or_email)
    if not q:
        return None
    # 1) exact email
    for r in rows:
        if _norm_name(r.get("email","")) == q:
            return r
    # 2) full name exact
    for r in rows:
        full = f"{r.get('first','')} {r.get('last','')}".strip()
        if _norm_name(full) == q:
            return r
    # 3) loose contains (either side)
    for r in rows:
        full = f"{r.get('first','')} {r.get('last','')}".strip()
        if q in _norm_name(full):
            return r
    # 4) last-name only match (if query is one word)
    if " " not in q:
        for r in rows:
            if q == _norm_name(r.get("last","")):
                return r
    return None

def cadet_details(name_or_email: str) -> dict:
    rows = _read_rows()
    r = _match_row(rows, name_or_email)
    if not r:
        raise ValueError("No matching cadet found")

    first, last = (r.get("first","").strip(), r.get("last","").strip())
    ms_raw = (r.get("ms") or "").strip()
    ms = ("MS"+ms_raw.replace("MS","").replace("ms","").strip()) if ms_raw else ""

    # Busy blocks
    busy = _day_busy_map(r)

    # Commutes (numbers as strings, keep raw if blank)
    commutes = {
        "tech_track": (r.get("commute_tech") or "").strip(),
        "gsu_track":  (r.get("commute_gsu")  or "").strip(),
        "ulm":        (r.get("commute_ulm")  or "").strip(),
        "nsu":        (r.get("commute_nsu")  or "").strip(),
    }

    # Bool-ish flags (leave original strings; caller can interpret)
    def _clean_bool(x: str) -> str:
        return (x or "").strip().title()

    extras = {
        "timestamp": r.get("timestamp",""),
        "busy_blocks": {
            "monday":    ", ".join([f"{_min_to_hhmm(s)}-{_min_to_hhmm(e)}" for s,e in busy["monday"]]),
            "tuesday":   ", ".join([f"{_min_to_hhmm(s)}-{_min_to_hhmm(e)}" for s,e in busy["tuesday"]]),
            "wednesday": ", ".join([f"{_min_to_hhmm(s)}-{_min_to_hhmm(e)}" for s,e in busy["wednesday"]]),
            "thursday":  ", ".join([f"{_min_to_hhmm(s)}-{_min_to_hhmm(e)}" for s,e in busy["thursday"]]),
            "friday":    ", ".join([f"{_min_to_hhmm(s)}-{_min_to_hhmm(e)}" for s,e in busy["friday"]]),
        },
        "commute_minutes": {
            "tech_track": commutes["tech_track"],
            "gsu_track":  commutes["gsu_track"],
            "ulm":        commutes["ulm"],
            "nsu":        commutes["nsu"],
        },
        "colorguard_experience": _clean_bool(r.get("colorguard","")),
        "agsu": _clean_bool(r.get("agsu","")),
        "ranger_challenge": _clean_bool(r.get("ranger","")),
        "ocps": (r.get("ocps","") or r.get("ocp","")).strip().lower() or "",
        "pt_uniform": (r.get("pt_uniform","") or "").strip().lower() or "",
        "compass": (r.get("compass","") or "").strip().lower() or "",
        "bayou_classic_interest": r.get("bayou",""),
        "special_circumstances_primary": (r.get("special1","") or "").strip(),
        "special_circumstances_secondary": (r.get("special2","") or "").strip(),
    }

    return {
        "first": first,
        "last": last,
        "ms": ms,
        "email": r.get("email",""),
        "school": r.get("school",""),
        "major": r.get("major",""),
        "phone": r.get("phone",""),
        "contracted": (r.get("contracted","") or "").strip().title(),
        "prior_service": (r.get("prior_service","") or "").strip().title(),
        "vehicle": (r.get("vehicle","") or "").strip().title(),
        "extras": extras,
    }

def person_info(query: str, org: Optional[str] = None) -> dict:
    # org (GSU/ULM) not enforced here; left for future if a column exists
    return cadet_details(query)

# -------------------------- availability logic ------------------------
DAY_ALIASES = {
    "mon":"monday","monday":"monday",
    "tue":"tuesday","tues":"tuesday","tuesday":"tuesday",
    "wed":"wednesday","wednesday":"wednesday",
    "thu":"thursday","thur":"thursday","thurs":"thursday","thursday":"thursday",
    "fri":"friday","friday":"friday",
}

def _min_to_hhmm(m: int) -> str:
    h = m // 60
    mm = m % 60
    return f"{h:02d}{mm:02d}"

def search_availability(day: str, start: str, end: str) -> list[dict]:
    """
    Return cadets who are **free** for the entire [start,end) window on a given weekday.
    Input times are strings like '0900', '1030'.
    """
    dkey = DAY_ALIASES.get(_norm_name(day), None)
    if not dkey:
        raise ValueError("Bad day")
    a = _to_min(start); b = _to_min(end)
    if a is None or b is None or b <= a:
        raise ValueError("Bad time window")

    rows = _read_rows()
    out = []
    for r in rows:
        first, last = (r.get("first","").strip(), r.get("last","").strip())
        if not (first or last):
            continue
        ms = (r.get("ms") or "").strip().replace("MS","").replace("ms","").strip()
        blocks = _day_busy_map(r).get(dkey, [])
        if not _overlaps(blocks, a, b):
            out.append({"first": first, "last": last, "ms": ms})
    # sort name asc
    out.sort(key=lambda x: (_norm_name(x["last"]), _norm_name(x["first"])))
    return out
