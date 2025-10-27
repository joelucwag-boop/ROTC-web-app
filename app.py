import os, json, io, re, datetime as dt, logging, traceback
from functools import wraps
from flask import Flask, request, jsonify, render_template_string

# --- Logging -----------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("app")

# --- Secrets -----------------------------------------------------------
APP_PASSWORD   = (os.getenv("APP_PASSWORD") or "").strip()
ADMIN_PASSWORD = (os.getenv("ADMIN_PASSWORD") or "").strip()

# --- Flask -------------------------------------------------------------
app = Flask(__name__, static_folder="static", static_url_path="/static")

# ------------------------------ Auth ----------------------------------
def require_pw(view):
    @wraps(view)
    def _w(*a, **k):
        pw = request.args.get("pw") or request.headers.get("X-APP-PW") or ""
        if APP_PASSWORD and pw != APP_PASSWORD:
            log.warning("Unauthorized access attempt from %s", request.remote_addr)
            return "<h1>Locked</h1><p>Append ?pw=YOURPASSWORD</p>", 401
        return view(*a, **k)
    return _w

def require_admin(view):
    @wraps(view)
    def _w(*a, **k):
        body = request.get_json(silent=True) or {}
        admin_pw = request.args.get("admin_pw") or body.get("admin_pw") or ""
        if ADMIN_PASSWORD and admin_pw != ADMIN_PASSWORD:
            log.warning("Admin auth failed for %s", request.remote_addr)
            return jsonify(ok=False, error="admin auth failed"), 401
        return view(*a, **k)
    return _w

# ---------------------------- Index/Static -----------------------------
@app.get("/")
@require_pw
def index():
    try:
        p = os.path.join(os.path.dirname(__file__), "templates", "index.html")
        with open(p, "r", encoding="utf-8") as f:
            html = f.read()
    except Exception:
        html = """<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>ROTC Tools</title>
<link rel="stylesheet" href="/static/styles.css">
</head>
<body>
<div class="wrap">
<h1>ROTC Tools</h1>
<div class="tabs">
  <button class="tab on" data-for="availability">Availability</button>
  <button class="tab" data-for="attendance">Attendance</button>
  <button class="tab" data-for="reports">Reports</button>
  <button class="tab" data-for="directory">Cadet Directory</button>
</div>

<section id="availability" class="panel on">
  <div class="row">
    <label>Day</label>
    <select id="day"><option>Monday</option><option>Tuesday</option><option>Wednesday</option><option>Thursday</option><option>Friday</option></select>
    <label>Start</label><input id="a_start" value="0900">
    <label>End</label><input id="a_end" value="1030">
    <button id="a_go" class="btn">Search</button>
  </div>
  <div id="a_out"></div>
  <div id="modal" class="modal"><div class="card">
    <div class="row"><h3 id="mtitle" style="flex:1">Details</h3><button id="mclose" class="btn">Close</button></div>
    <div id="kvgrid" class="kvgrid"></div>
  </div></div>
</section>

<section id="attendance" class="panel">
  <div class="row">
    <label>Block</label><select id="evt_block"><option value="gsu">GSU</option><option value="ulm">ULM</option></select>
    <label>Date</label><input id="evt_date" placeholder="8/27/2025">
    <label>Type</label><select id="evt_type"><option>PT</option><option>LAB</option><option value="OTHER">OTHER</option></select>
    <input id="evt_other" placeholder="Other name">
    <button id="save_att" class="btn primary">Save attendance</button>
  </div>
  <table id="rost" class="table"><thead><tr><th>Name</th><th>MS</th><th>Present</th><th>FTR</th><th>Excused</th></tr></thead><tbody></tbody></table>
</section>

<section id="reports" class="panel">
  <h3>Day view</h3>
  <div class="row">
    <label>Block</label><select id="rep_block"><option value="gsu">GSU</option><option value="ulm">ULM</option></select>
    <label>Date</label><input id="rep_date" placeholder="8/27/2025">
    <button id="rep_day_btn" class="btn">Load day</button>
  </div>
  <pre id="rep_day_out"></pre>

  <h3 style="margin-top:18px">Leaderboard</h3>
  <div class="row">
    <label>From</label><input id="lb_from" placeholder="8/1/2025">
    <label>To</label><input id="lb_to" placeholder="10/31/2025">
    <button id="lb_btn" class="btn">Build</button>
  </div>
  <pre id="lb_out"></pre>

  <h3 style="margin-top:18px">Charts (weekly)</h3>
  <div class="row">
    <label>Range</label>
    <input id="ch_from" placeholder="8/1/2025">
    <input id="ch_to" placeholder="10/31/2025">
    <button id="ch_refresh" class="btn">Refresh charts</button>
  </div>
  <div class="chartwrap"><canvas id="chart_totals" height="140"></canvas></div>
  <div class="chartwrap"><canvas id="chart_ms" height="140"></canvas></div>
</section>

<section id="directory" class="panel">
  <div class="row"><button id="load_dir" class="btn">Load Cadet Directory</button></div>
  <div id="dir_out"></div>
</section>
</div>
<script>window.APP_PW=new URLSearchParams(location.search).get("pw")||"";</script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="/static/script.js"></script>
</body></html>"""
    return render_template_string(html)

# -------------------------- Availability API --------------------------
from availability import (
    search_availability, person_info, list_all_cadets, cadet_details
)

@app.get("/api/available")
@require_pw
def api_available():
    try:
        day   = request.args.get("day", "Monday")
        start = request.args.get("start", "0900")
        end   = request.args.get("end", "1030")
        rows  = search_availability(day, start, end)
        return jsonify(ok=True, rows=rows)
    except Exception as e:
        log.exception("availability failed")
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400

@app.get("/api/person")
@require_pw
def api_person():
    try:
        query = request.args.get("q", "")
        org   = request.args.get("org")
        data  = person_info(query, org=org)
        return jsonify(ok=True, person=data)
    except Exception as e:
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400

@app.get("/api/cadet/list")
@require_pw
def api_cadet_list():
    try:
        return jsonify(ok=True, rows=list_all_cadets())
    except Exception as e:
        log.exception("cadet list failed")
        return jsonify(ok=False, error=str(e)), 400

@app.get("/api/cadet/details")
@require_pw
def api_cadet_details():
    try:
        name = request.args.get("name","")
        return jsonify(ok=True, person=cadet_details(name))
    except Exception as e:
        log.exception("cadet details failed")
        return jsonify(ok=False, error=str(e)), 400

# --------------------------- Attendance API ---------------------------
from attendance import (
    list_roster, list_events, read_event_records,
    add_event_and_mark, leaderboard, render_leaderboard_text,
    day_text_report, charts_weekly
)

@app.get("/api/att/roster")
@require_pw
def api_att_roster():
    try:
        label = (request.args.get("label") or "gsu").lower()
        return jsonify(ok=True, rows=list_roster(label))
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400

@app.post("/api/att/add_event_and_mark")
@require_admin
def api_att_add_event_and_mark():
    body = request.get_json(force=True)
    date = (body.get("date","") or "").trim()
    event_type  = (body.get("event_type","PT") or "PT").strip().upper()
    event_other = (body.get("event_other","") or "").strip()
    label = (body.get("label") or "gsu").lower()
    marks = body.get("marks") or []
    if not date:
        return jsonify(ok=False, error="date required"), 400
    if event_type not in ("PT","LAB","OTHER"):
        return jsonify(ok=False, error="bad event_type"), 400
    header = f"{date} + {(event_other if event_type=='OTHER' else event_type)}"
    try:
        updated = add_event_and_mark(label, header, marks)
        return jsonify(ok=True, header=header, updated_cells=updated)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400

@app.get("/api/att/events")
@require_pw
def api_att_events():
    label = (request.args.get("label") or "gsu").lower()
    return jsonify(ok=True, events=list_events(label))

@app.get("/api/att/day")
@require_pw
def api_att_day():
    try:
        label = (request.args.get("label") or "gsu").lower()
        date  = (request.args.get("date") or "").strip()
        txt   = day_text_report(label, date)
        return jsonify(ok=True, text=txt)
    except Exception as e:
        log.exception("day report failed")
        return jsonify(ok=False, error=str(e)), 400

@app.get("/api/att/leaderboard")
@require_pw
def api_att_leaderboard():
    try:
        label = (request.args.get("label") or "gsu").lower()
        dfrom = request.args.get("from","")
        dto   = request.args.get("to","")
        mode  = (request.args.get("mode") or "json").lower()
        top   = int(request.args.get("top","10"))
        d1 = dt.datetime.strptime(dfrom,"%m/%d/%Y").date() if dfrom else None
        d2 = dt.datetime.strptime(dto,"%m/%d/%Y").date() if dto else None
        data = leaderboard(label, d1, d2, top=top)
        if mode == "text":
            return jsonify(ok=True, text=render_leaderboard_text(data))
        return jsonify(ok=True, **data)
    except Exception as e:
        log.exception("leaderboard failed")
        return jsonify(ok=False, error=str(e)), 400

@app.get("/api/att/charts")
@require_pw
def api_att_charts():
    try:
        label = (request.args.get("label") or "gsu").lower()
        dfrom = request.args.get("from","")
        dto   = request.args.get("to","")
        d1 = dt.datetime.strptime(dfrom,"%m/%d/%Y").date() if dfrom else None
        d2 = dt.datetime.strptime(dto,"%m/%d/%Y").date() if dto else None
        data = charts_weekly(label, d1, d2)  # cached
        return jsonify(ok=True, **data)
    except Exception as e:
        log.exception("charts failed")
        return jsonify(ok=False, error=str(e)), 400

# ----------------------------- Health/Diag -----------------------------
@app.get("/api/envcheck")
def envcheck():
    out = {
        "have": {
            "APP_PASSWORD": bool(os.getenv("APP_PASSWORD")),
            "ADMIN_PASSWORD": bool(os.getenv("ADMIN_PASSWORD")),
            "GOOGLE_SHEET_URL": bool(os.getenv("GOOGLE_SHEET_URL")),
            "SPREADSHEET_ID": bool(os.getenv("SPREADSHEET_ID")),
            "WORKSHEET_NAME": bool(os.getenv("WORKSHEET_NAME")),
            "GSU_HEADER_ROW": bool(os.getenv("GSU_HEADER_ROW")),
            "ULM_HEADER_ROW": bool(os.getenv("ULM_HEADER_ROW")),
            "GOOGLE_SERVICE_ACCOUNT_JSON": bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")),
            "GOOGLE_SERVICE_ACCOUNT_JSON_PATH": bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")),
        },
        "service_account_email": None,
        "errors": {}
    }
    try:
        if os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"):
            p = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")
            with open(p, "r", encoding="utf-8") as f:
                out["service_account_email"] = json.load(f)["client_email"]
        elif os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"):
            out["service_account_email"] = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))["client_email"]
    except Exception as e:
        out["errors"]["creds_parse"] = f"{type(e).__name__}: {e}"

    try:
        import requests
        url = os.getenv("GOOGLE_SHEET_URL")
        if url:
            r = requests.get(url, timeout=15)
            out["availability_http"] = {"status": r.status_code, "ok": r.ok, "len": len(r.text)}
        else:
            out["availability_http"] = {"status": None, "ok": False, "len": 0}
    except Exception as e:
        out["errors"]["availability_http"] = f"{type(e).__name__}: {e}"

    return out

@app.get("/healthz")
def healthz():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")), debug=os.getenv("FLASK_ENV")=="development")

