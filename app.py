#!/usr/bin/env python3
import os, json, re, datetime as dt
import pandas as pd
from flask import Flask, request, jsonify, Response
from attendance_matrix_writer import add_event_and_mark

# ----------------- Config -----------------
APP_PASSWORD   = os.getenv("APP_PASSWORD","").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD","").strip()
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL","").strip()  # availability CSV
# For roster extraction:
PRESENT_POINTS = float(os.getenv("POINTS_PRESENT","1"))
LATE_POINTS    = float(os.getenv("POINTS_LATE","0.5"))
EXC_POINTS     = float(os.getenv("POINTS_EXCUSED","0.25"))
ABS_POINTS     = float(os.getenv("POINTS_ABSENT","0"))

app = Flask(__name__)

# -------- Availability DF + helpers --------
DF=None; COLS={}; DAYS={}; INIT=False
def _guess(df, names):
    # try exact match by lower-case name, else contains
    lower = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower: return lower[n.lower()]
    for c in df.columns:
        cl = str(c).strip().lower()
        for n in names:
            if n.lower() in cl: return c
    return None

def _guess_day(df, day):
    d = day.lower()
    for c in df.columns:
        if str(c).strip().lower().startswith(d): return c
    return None

def _parse_block(s):
    if s is None: return []
    t = str(s).strip()
    if t.lower() in {"","n/a","na","none"}: return []
    parts = [p for p in t.split(",") if p.strip()]
    out=[]
    rx = re.compile(r'^\s*(\d{1,2}):?(\d{2})\s*-\s*(\d{1,2}):?(\d{2})\s*$')
    for p in parts:
        m = rx.match(p)
        if m:
            h1,m1,h2,m2 = m.groups()
            a = int(h1)*60+int(m1); b = int(h2)*60+int(m2)
            if b>a: out.append((a,b))
    return out

def _overlap(a,b): return a[0]<b[1] and b[0]<a[1]
def _free(blocks, window):
    for b in blocks:
        if _overlap(b,window): return False
    return True

def _init():
    global DF,COLS,DAYS,INIT
    if INIT: return
    if not GOOGLE_SHEET_URL: raise RuntimeError("GOOGLE_SHEET_URL not set")
    DF = pd.read_csv(GOOGLE_SHEET_URL)
    COLS = {
      "first": _guess(DF,["First Name","First"]),
      "last":  _guess(DF,["Last Name","Last"]),
      "ms":    _guess(DF,["MS Level","MS"]),
      "phone": _guess(DF,["Phone Number","Phone"]),
      "email": _guess(DF,["School Email","Email Address","Email"]),
    }
    DAYS = { k: _guess_day(DF,k) for k in ["monday","tuesday","wednesday","thursday","friday"] }
    INIT=True

@app.before_request
def _ensure(): 
    if not INIT: _init()

def _gate(html):
    if APP_PASSWORD and request.args.get("pw","")!=APP_PASSWORD:
        return Response("<h2>Locked</h2><p>Append ?pw=YOURPASSWORD</p>", mimetype="text/html")
    return Response(html, mimetype="text/html")

# ---------------- API: Availability ----------------
@app.get("/api/available")
def api_available():
    d = request.args.get("day","Mon")[:3].lower()
    s = request.args.get("start",""); e=request.args.get("end","")
    if not re.fullmatch(r"\d{4}",s) or not re.fullmatch(r"\d{4}",e):
        return jsonify(ok=False, error="Use HHMM like 0900 and 1030"), 400
    a=(int(s[:2])*60+int(s[2:]), int(e[:2])*60+int(e[2:]))
    col = {"mon":DAYS["monday"],"tue":DAYS["tuesday"],"wed":DAYS["wednesday"],"thu":DAYS["thursday"],"fri":DAYS["friday"]}.get(d)
    if not col: return jsonify(ok=False,error="Day column not found"),400
    people=[]
    for i,r in DF.iterrows():
        busy = _parse_block(r.get(col,""))
        if _free(busy,a):
            people.append({
                "row": int(i),
                "first": r.get(COLS["first"]),
                "last":  r.get(COLS["last"]),
                "ms":    r.get(COLS["ms"]),
                "phone": r.get(COLS["phone"]),
                "email": r.get(COLS["email"]),
            })
    return jsonify(ok=True,count=len(people),people=people)

@app.get("/api/person")
def api_person():
    try: idx=int(request.args.get("row","-1"))
    except: return jsonify(ok=False,error="row must be int"),400
    if idx<0 or idx>=len(DF): return jsonify(ok=False,error="row out of range"),400
    drop_days = request.args.get("drop_days","1")=="1"
    data={}
    for k in DF.columns:
        if drop_days and str(k).strip().lower() in DAYS.values(): continue
        data[str(k)]=DF.iloc[idx][k]
    return jsonify(ok=True, fields=data)

# ---------------- API: Attendance (matrix) ----------------
def _require_admin():
    if ADMIN_PASSWORD and request.args.get("admin_pw","")!=ADMIN_PASSWORD:
        return ("Admin only", 403)

@app.post("/api/att/add_event_and_mark")
def api_add_event_and_mark():
    if (r:=_require_admin()): return r
    body = request.get_json(force=True,silent=True) or {}
    date = (body.get("date") or "").strip()           # "M/D/YYYY"
    etype= (body.get("event_type") or "").upper()     # PT | LAB | OTHER
    other= (body.get("event_other") or "").strip()
    label= (body.get("label") or "gsu").lower()       # gsu | ulm
    marks= body.get("marks", [])

    if not re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", date):
        return jsonify(ok=False,error="date must be M/D/YYYY"),400
    if etype not in {"PT","LAB","OTHER"}:
        return jsonify(ok=False,error="event_type must be PT, LAB or OTHER"),400
    event_text = {"PT":"PT","LAB":"LAB","OTHER": (other or "Other")}[etype]

    try:
        res = add_event_and_mark(label, date, event_text, marks)
        return jsonify(ok=True, **res)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400

# ---------------- UI ----------------
INDEX = """<!doctype html><meta charset=utf-8>
<title>ROTC Tools</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<style>
:root{--bg:#0f172a;--panel:#111827;--fg:#f8fafc;--muted:#a1a1aa}
*{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--fg);font:16px system-ui}
.wrap{max-width:1000px;margin:0 auto;padding:14px}
.tabs{display:flex;gap:8px;margin-bottom:8px}
.tab{padding:8px 10px;border:1px solid #1f2937;background:#0b1a2e;border-radius:10px;cursor:pointer}
.tab.active{background:#0c2440}
.panel{background:#0b1222;border:1px solid #1f2937;border-radius:12px;padding:12px}
.row{display:grid;grid-template-columns:1fr;gap:10px} @media(min-width:720px){.row{grid-template-columns:repeat(3,1fr)}}
.btn{padding:9px 10px;border:1px solid #1f2937;background:#0b1a2e;color:var(--fg);border-radius:10px;cursor:pointer}
input,select{width:100%;padding:10px;border-radius:10px;border:1px solid #334155;background:#0b1222;color:var(--fg)}
.cards{display:grid;grid-template-columns:1fr;gap:10px} @media(min-width:860px){.cards{grid-template-columns:1fr 1fr}}
.card{border:1px solid #1f2937;background:#0b1222;border-radius:12px;padding:12px}
.kv{font-size:14px;color:var(--muted)} .hidden{display:none}
.modal{position:fixed;inset:0;background:rgba(0,0,0,.55);display:none;align-items:center;justify-content:center;padding:16px}
.box{background:#0b1222;border:1px solid #1f2937;border-radius:12px;width:min(900px,94vw);max-height:90vh;display:flex;flex-direction:column}
.boxhdr{display:flex;gap:8px;align-items:center;padding:8px 12px;border-bottom:1px solid #1f2937}
#kvwrap{flex:1;min-height:0;overflow:auto;padding:12px} .kvgrid{display:grid;grid-template-columns:1fr 2fr;gap:8px}
.table{width:100%;border-collapse:collapse} .table td,.table th{border-bottom:1px solid #223;padding:8px;text-align:left;font-size:14px}
</style>
<div class=wrap>
  <div class=tabs>
    <div class="tab active" data-tab=avail>Availability</div>
    <div class="tab" data-tab=att>Attendance</div>
  </div>

  <div id=tab_avail class=panel>
    <div class=row>
      <div><label>Day</label><select id=day><option>Monday</option><option>Tuesday</option><option>Wednesday</option><option>Thursday</option><option>Friday</option></select></div>
      <div><label>Start (HHMM)</label><input id=start placeholder=0900 maxlength=4 inputmode=numeric></div>
      <div><label>End (HHMM)</label><input id=end placeholder=1030 maxlength=4 inputmode=numeric></div>
    </div>
    <div style=margin:8px 0><button class=btn id=search>Search</button></div>
    <div id=results class=cards></div>
  </div>

  <div id=tab_att class="panel hidden">
    <div class=row>
      <div><label>Date (M/D/YYYY)</label><input id=evt_date placeholder="8/27/2025"></div>
      <div><label>Event type</label><select id=evt_type><option value=PT>PT</option><option value=LAB>Lab</option><option value=OTHER>Other…</option></select></div>
      <div id=evt_other_wrap class=hidden><label>Other event name</label><input id=evt_other placeholder="APFT, Bayou Classic, FTX…"></div>
    </div>
    <div class=row style=margin-top:8px>
      <div><label>Block</label><select id=evt_block><option value=gsu>GSU</option><option value=ulm>ULM</option></select></div>
      <div><label>&nbsp;</label><button class=btn id=save_att>Save attendance</button></div>
    </div>
    <h3>Roster</h3>
    <table class=table id=rost><thead><tr><th>Name</th><th>MS</th><th>Present</th><th>FTR</th><th>Excused</th></tr></thead><tbody></tbody></table>
  </div>
</div>

<div id=modal class=modal><div class=box>
  <div class=boxhdr><h3 id=mtitle style=margin:0;flex:1></h3><button class=btn id=mbasic>Basic</button><button class=btn id=mall>All</button><button class=btn id=mclose>Close</button></div>
  <div id=kvwrap><div id=kvgrid class=kvgrid></div></div>
</div></div>

<script>
const $=s=>document.querySelector(s);
const $$=s=>document.querySelectorAll(s);
$$('.tab').forEach(t=>t.onclick=()=>{ $$('#tab_avail,#tab_att').forEach(x=>x.classList.add('hidden')); $('#tab_'+t.dataset.tab).classList.remove('hidden'); $$('.tab').forEach(x=>x.classList.remove('active')); t.classList.add('active'); });
const escapeHtml=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[c]));

let currentMarks=[];

function renderRoster(rows){
  const tb = $('#rost tbody'); tb.innerHTML='';
  rows.forEach(r=>{
    const name = `${r.first||''} ${r.last||''}`.trim();
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${escapeHtml(name)}</td><td>${escapeHtml(r.ms||'')}</td>
    <td><button class=btn data-status=Present>Present</button></td>
    <td><button class=btn data-status=FTR>FTR</button></td>
    <td><button class=btn data-status="Excused: NFR">Excused</button></td>`;
    tb.appendChild(tr);
    tr.querySelectorAll('button').forEach(b=>{
      b.onclick=()=>{
        // store/replace mark for this person
        const key = `${r.first}|${r.last}`.toLowerCase();
        const idx = currentMarks.findIndex(m=>(m.first+'|'+m.last).toLowerCase()===key);
        const mark = {first:r.first, last:r.last, status:b.dataset.status};
        if(idx>=0) currentMarks[idx]=mark; else currentMarks.push(mark);
        b.textContent='✓ '+b.dataset.status;
      };
    });
  });
}

async function loadRoster(){
  const r = await fetch('/api/available?day=mon&start=0000&end=0001'); // hack just to get base fields
  const j = await r.json();
  // Deduplicate and show everyone (the endpoint returns all who are “free” at 00:00 which is everyone)
  const rows = j.ok ? j.people.map(p=>({first:p.first,last:p.last,ms:p.ms})) : [];
  renderRoster(rows);
}
loadRoster();

$('#evt_type').onchange=()=>$('#evt_other_wrap').classList.toggle('hidden', $('#evt_type').value!=='OTHER');

$('#search').onclick=async ()=>{
  const day=$('#day').value.slice(0,3), s=$('#start').value.trim(), e=$('#end').value.trim();
  const r = await fetch(`/api/available?day=${encodeURIComponent(day)}&start=${s}&end=${e}`);
  const j = await r.json();
  const grid = $('#results'); grid.innerHTML='';
  if(!j.ok||j.count===0){ grid.textContent='No matches'; return; }
  j.people.forEach(p=>{
    const card=document.createElement('div'); card.className='card';
    card.innerHTML = `<h3>${escapeHtml((p.first||'')+' '+(p.last||''))}</h3>
    <div class=kv>MS ${escapeHtml(p.ms||'')}</div>
    <div class=kv>${escapeHtml(p.phone||'')} · ${escapeHtml(p.email||'')}</div>
    <button class=btn data-row="${p.row}">View info</button>`;
    card.querySelector('button').onclick=()=>openPerson(p.row, (p.first||'')+' '+(p.last||''));
    grid.appendChild(card);
  });
};

async function openPerson(row,name){
  $('#mtitle').textContent=name;
  await loadPerson(row,true);
  $('#modal').style.display='flex';
}
$('#mclose').onclick=()=>$('#modal').style.display='none';
$('#modal').onclick=e=>{ if(e.target.id==='modal') $('#modal').style.display='none'; }
$('#mbasic').onclick=()=>{ if(window._row!=null) loadPerson(window._row,true) }
$('#mall').onclick=()=>{ if(window._row!=null) loadPerson(window._row,false) }

async function loadPerson(row,drop){
  window._row=row;
  const r = await fetch('/api/person?row='+row+'&drop_days='+(drop?1:0));
  const j = await r.json(); const kv=$('#kvgrid'); kv.innerHTML='';
  for(const [k,v] of Object.entries(j.fields)){ const a=document.createElement('div'); a.className='kv'; a.textContent=k; const b=document.createElement('div'); b.textContent=String(v??''); kv.append(a,b); }
}

$('#save_att').onclick=async ()=>{
  const date = $('#evt_date').value.trim();
  const event_type = $('#evt_type').value;
  const event_other = $('#evt_other').value.trim();
  const label = $('#evt_block').value;
  if(event_type==='OTHER' && !event_other){ alert('Enter event name'); return; }
  if(currentMarks.length===0){ alert('Mark at least one person'); return; }
  const admin_pw = localStorage.getItem('admin_pw') || prompt('Admin password?') || '';
  localStorage.setItem('admin_pw', admin_pw);
  const r = await fetch('/api/att/add_event_and_mark?admin_pw='+encodeURIComponent(admin_pw), {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({date, event_type, event_other, label, marks: currentMarks})
  });
  const j = await r.json();
  if(!j.ok){ alert(j.error||'Save failed'); return; }
  alert('✓ Wrote '+j.updated_cells.length+' cells to '+j.header);
  currentMarks=[]; loadRoster();
};
</script>
"""
@app.get("/")
def index(): return _gate(INDEX)

# Gunicorn entrypoint:
#   gunicorn app:app
