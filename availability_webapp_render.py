#!/usr/bin/env python3
import os, re
import pandas as pd
from pathlib import Path
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

DF = None
DAY_COLS = {}
COLMAP = {}
INITIALIZED = False

# ---------- Helpers ----------
_TIME_TOKEN = re.compile(r"""
    ^\s*
    (?P<h1>\d{1,2})
    :?
    (?P<m1>\d{2})?
    \s*-\s*
    (?P<h2>\d{1,2})
    :?
    (?P<m2>\d{2})?
    \s*$
""", re.VERBOSE)

def to_minutes(h, m): return int(h)*60 + int(m)

def parse_time_token(tok: str):
    tok = tok.strip()
    m = _TIME_TOKEN.match(tok)
    if not m: return None
    h1 = int(m.group('h1')); m1 = int(m.group('m1') or 0)
    h2 = int(m.group('h2')); m2 = int(m.group('m2') or 0)
    if m.group('m1') is None: m1 = 0
    if m.group('m2') is None: m2 = 0
    start = to_minutes(h1, m1); end = to_minutes(h2, m2)
    if end <= start: return None
    return start, end

def parse_window_hhmm(start_hhmm: str, end_hhmm: str):
    s = start_hhmm.strip()
    e = end_hhmm.strip()
    if len(s) == 4 and s.isdigit(): s = f"{s[:2]}:{s[2:]}"
    if len(e) == 4 and e.isdigit(): e = f"{e[:2]}:{e[2:]}"
    w = parse_time_token(f"{s}-{e}")
    if not w: raise ValueError("Use HHMM like 0930 and 1100, with start < end.")
    return w

def guess_day_column(df: pd.DataFrame, day: str):
    d = day.strip().lower()
    for col in df.columns:
        c = str(col).strip().lower()
        if c.startswith(d): return col
    return None

def parse_busy_field(s: str):
    if s is None: return []
    text = str(s).strip()
    if text.lower() in {"", "n/a", "na", "none"}: return []
    parts = [p for p in text.split(",") if p.strip()]
    out = []
    for p in parts:
        cand = parse_time_token(p)
        if cand: out.append(cand); continue
        m = re.search(r'(\d{1,2}:?\d{2})\s*-\s*(\d{1,2}:?\d{2})', p)
        if m:
            cand = parse_time_token(m.group(0))
            if cand: out.append(cand)
    return out

def overlaps(a, b): return a[0] < b[1] and b[0] < a[1]
def is_available_for_window(busy_blocks, window):
    for b in busy_blocks:
        if overlaps(b, window): return False
    return True

def find_cols(df: pd.DataFrame):
    def find(cands):
        lc = {str(c).strip().lower(): c for c in df.columns}
        for k in cands:
            kl = k.lower()
            if kl in lc: return lc[kl]
        for c in df.columns:
            cl = str(c).strip().lower()
            for k in cands:
                if k.lower() in cl: return c
        return None
    colmap = {}
    colmap["email"] = find(["Email Address","Email"])
    colmap["school_email"] = find(["School Email","Student Email"])
    colmap["phone"] = find(["Phone Number","Phone"])
    colmap["ms_level"] = find(["MS level","MS Level","MS"])
    colmap["first_name"] = find(["First Name","First"])
    colmap["last_name"] = find(["Last Name","Last"])
    return colmap

def base_profile(row: pd.Series, colmap: dict):
    first = row.get(colmap.get("first_name"))
    last = row.get(colmap.get("last_name"))
    ms = row.get(colmap.get("ms_level"))
    phone = row.get(colmap.get("phone"))
    email = row.get(colmap.get("school_email")) or row.get(colmap.get("email"))
    return {"first": first, "last": last, "ms": ms, "phone": phone, "email": email}

# ---------- Data loading ----------
def load_dataframe_from_source(source: str):
    if source and source.startswith("http"):
        return pd.read_csv(source)
    p = Path(source)
    if p.suffix.lower() in {".xlsx",".xls"}:
        return pd.read_excel(p)
    return pd.read_csv(p)

def init_data_if_needed():
    global INITIALIZED, DF, DAY_COLS, COLMAP
    if INITIALIZED:
        return
    csv_url = os.getenv("GOOGLE_SHEET_URL", "").strip()
    if not csv_url:
        raise RuntimeError("GOOGLE_SHEET_URL env var is not set. Set it in Render → Environment.")
    DF = load_dataframe_from_source(csv_url)
    COLMAP = find_cols(DF)
    DAY_COLS = {
        "Mon": guess_day_column(DF, "monday"),
        "Tue": guess_day_column(DF, "tuesday"),
        "Wed": guess_day_column(DF, "wednesday"),
        "Thu": guess_day_column(DF, "thursday"),
        "Fri": guess_day_column(DF, "friday"),
    }
    if not any(DAY_COLS.values()):
        raise RuntimeError("Could not find weekday columns starting with Monday..Friday in the sheet.")
    INITIALIZED = True

# ---------- API ----------
@app.before_request
def ensure_initialized():
    if not INITIALIZED:
        init_data_if_needed()

@app.get("/api/available")
def api_available():
    day = request.args.get("day", "Mon")[:3]
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    if day not in DAY_COLS or not DAY_COLS[day]:
        return jsonify({"ok": False, "error": f"No column found for {day}"}), 400
    try:
        window = parse_window_hhmm(start, end)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    col = DAY_COLS[day]
    rows = []
    for idx, r in DF.iterrows():
        busy = parse_busy_field(r.get(col, ""))
        if is_available_for_window(busy, window):
            prof = base_profile(r, COLMAP)
            prof["row"] = int(idx)
            rows.append(prof)
    return jsonify({"ok": True, "count": len(rows), "people": rows})

@app.get("/api/person")
def api_person():
    try:
        idx = int(request.args.get("row", "-1"))
    except:
        return jsonify({"ok": False, "error": "row must be an integer"}), 400
    if idx < 0 or idx >= len(DF):
        return jsonify({"ok": False, "error": "row out of range"}), 400
    drop_days = request.args.get("drop_days", "0") == "1"
    data = {}
    for k in DF.columns:
        if drop_days and k in DAY_COLS.values():
            continue
        data[str(k)] = DF.iloc[idx][k]
    return jsonify({"ok": True, "fields": data})

# ---------- UI ----------
INDEX_HTML = """<!doctype html>
<html lang='en'>
<head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Availability Lookup</title>
<style>
:root{--bg:#0f172a;--panel:#111827;--fg:#f8fafc;--muted:#a1a1aa;--accent:#22c55e}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.4 system-ui,-apple-system,Segoe UI,Roboto,Arial}
header{padding:16px 20px;background:#0b1022;border-bottom:1px solid #1f2937}
h1{margin:0;font-size:20px}.wrap{max-width:900px;margin:0 auto;padding:16px}
.panel{background:var(--panel);border:1px solid #1f2937;border-radius:12px;padding:14px;margin-bottom:14px}
label{display:block;margin:10px 0 6px;color:var(--muted);font-size:14px}
select,input,button{width:100%;padding:12px 14px;border-radius:10px;border:1px solid #334155;background:#0b1222;color:var(--fg)}
button{cursor:pointer;border:1px solid #1f2937;background:#0b1a2e}
.row{display:grid;grid-template-columns:1fr;gap:12px}@media(min-width:640px){.row{grid-template-columns:1fr 1fr 1fr}}
.cards{display:grid;grid-template-columns:1fr;gap:12px;margin-top:10px}@media(min-width:720px){.cards{grid-template-columns:1fr 1fr}}
.card{border:1px solid #1f2937;background:#0b1222;border-radius:12px;padding:14px}
.card h3{margin:0 0 6px;font-size:18px}.pill{display:inline-block;font-size:12px;padding:3px 8px;border-radius:999px;background:#0f2135;border:1px solid #1f3552;color:#cce9ff;margin-left:6px}
.kv{margin:6px 0;font-size:14px}.muted{color:var(--muted)}.center{display:flex;justify-content:center;align-items:center;padding:10px}
.btn{padding:10px 12px;border-radius:10px;border:1px solid #1f2937;background:#0b1a2e;color:var(--fg);margin-right:8px}
.ok{color:#a7f3d0}.error{color:#fecaca}.hidden{display:none}
.modal{position:fixed;inset:0;background:rgba(0,0,0,.4);display:none;align-items:center;justify-content:center}
.modal .box{background:#0b1222;border:1px solid #1f2937;border-radius:12px;padding:16px;max-width:640px;width:90%}
.box h2{margin:0 0 10px}.close{float:right}
.kvgrid{display:grid;grid-template-columns:1fr 2fr;gap:8px}
</style>
</head>
<body>
<header><div class="wrap"><h1>Availability Lookup</h1></div></header>
<div class="wrap">
  <div class="panel">
    <div class="row">
      <div><label for="day">Day</label><select id="day"><option>Monday</option><option>Tuesday</option><option>Wednesday</option><option>Thursday</option><option>Friday</option></select></div>
      <div><label for="start">Start (HHMM)</label><input id="start" maxlength="4" inputmode="numeric" placeholder="0900"></div>
      <div><label for="end">End (HHMM)</label><input id="end" maxlength="4" inputmode="numeric" placeholder="1030"></div>
    </div>
    <div style="margin-top:10px">
      <button class="btn" data-preset="0700-0830">0700–0830</button>
      <button class="btn" data-preset="0930-1100">0930–1100</button>
      <button class="btn" data-preset="1300-1430">1300–1430</button>
      <button class="btn" data-preset="1500-1700">1500–1700</button>
    </div>
    <div style="margin-top:10px"><button id="search" class="btn">Search</button></div>
    <div id="status" class="center muted"></div>
  </div>
  <div id="results" class="cards"></div>
</div>
<div id="modal" class="modal"><div class="box">
  <button id="close" class="btn close">Close</button>
  <h2 id="m_title">Cadet</h2>
  <div id="kvgrid" class="kvgrid"></div>
</div></div>
<script>
const statusEl=document.getElementById('status');const resEl=document.getElementById('results');
const dayEl=document.getElementById('day');const sEl=document.getElementById('start');const eEl=document.getElementById('end');
const modal=document.getElementById('modal');const closeBtn=document.getElementById('close');
const kvgrid=document.getElementById('kvgrid');const mtitle=document.getElementById('m_title');
document.querySelectorAll('[data-preset]').forEach(btn=>{btn.addEventListener('click',()=>{const[a,b]=btn.dataset.preset.split('-');sEl.value=a;eEl.value=b;});});
function showStatus(msg,cls='muted'){statusEl.className='center '+cls;statusEl.textContent=msg;}
function cardHTML(p){const name=[p.first||'',p.last||''].join(' ').trim();return`<div class="card"><h3>${escape(name)} <span class="pill">${escape(p.ms||'')}</span></h3><div class="kv"><span class="muted">Phone:</span> ${escape(p.phone||'')}</div><div class="kv"><span class="muted">Email:</span> ${escape(p.email||'')}</div><div><button class="btn" data-row="${p.row}">View all info</button></div></div>`;}
function escape(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[c]))}
async function fetchPerson(row){const r=await fetch('/api/person?row='+row+'&drop_days=0');const j=await r.json();if(!j.ok)throw new Error(j.error||'Error');return j.fields;}
function openModal(title,fields){mtitle.textContent=title;kvgrid.innerHTML='';for(const[k,v]of Object.entries(fields)){const kdiv=document.createElement('div');kdiv.className='muted';kdiv.textContent=k;const vdiv=document.createElement('div');vdiv.textContent=String(v||'');kvgrid.appendChild(kdiv);kvgrid.appendChild(vdiv);}modal.style.display='flex';}
closeBtn.addEventListener('click',()=>modal.style.display='none');modal.addEventListener('click',e=>{if(e.target===modal)modal.style.display='none';});
document.getElementById('search').addEventListener('click',async()=>{const d=dayEl.value.slice(0,3);const s=sEl.value.trim(),e=eEl.value.trim();if(!/^\d{4}$/.test(s)||!/^\d{4}$/.test(e)){showStatus('Use 4-digit times like 0900 and 1030','error');return;}showStatus('Searching…');resEl.innerHTML='';const r=await fetch(`/api/available?day=${encodeURIComponent(d)}&start=${s}&end=${e}`);const j=await r.json();if(!j.ok){showStatus(j.error||'Error','error');return;}if(j.count===0){showStatus('No one is available for that window.','error');return;}showStatus(`${j.count} available`,'ok');resEl.innerHTML=j.people.map(cardHTML).join('');resEl.querySelectorAll('button[data-row]').forEach(btn=>{btn.addEventListener('click',async()=>{const row=btn.dataset.row;try{const fields=await fetchPerson(row);const name=btn.closest('.card').querySelector('h3').childNodes[0].textContent.trim();openModal(name,fields);}catch(e){showStatus(e.message,'error');}});});});
</script></body></html>
"""

@app.get("/")
def index():
    app_pw = os.getenv("APP_PASSWORD", "").strip()
    if app_pw:
        supplied = request.args.get("pw", "")
        if supplied != app_pw:
            return Response("<h3 style='font-family:system-ui'>Locked</h3><p>Append ?pw=YOURPASSWORD to the URL.</p>", mimetype="text/html")
    return Response(INDEX_HTML, mimetype="text/html")
