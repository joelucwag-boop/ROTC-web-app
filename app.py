import os, json, logging, datetime as dt
from functools import wraps
from flask import Flask, request, jsonify, render_template_string, send_from_directory

# ---- your modules (unchanged) ----
from attendance import list_events, read_event_records, leaderboard, list_roster, add_event_and_mark
from availability import find_available, person_info, list_all_cadets, cadet_details

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("app")

APP_PASSWORD  = (os.getenv("APP_PASSWORD")  or "").strip()
ADMIN_PASSWORD = (os.getenv("ADMIN_PASSWORD") or "").strip()

app = Flask(__name__, static_folder="static", static_url_path="/static")


# ---------- helpers ----------
def _coerce_one(v, default=""):
    """Return a single string even if v is a list/tuple/None."""
    if v is None: return default
    if isinstance(v, (list, tuple)): 
        return "" if not v else str(v[0])
    return str(v)

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


# ---------- routes ----------
@app.get("/")
@require_pw
def index():
    # Serve inline template so you don't depend on /templates in Render
    html = open(os.path.join(os.path.dirname(__file__), "templates", "index.html"), "r", encoding="utf-8").read() \
           if os.path.exists(os.path.join(os.path.dirname(__file__), "templates", "index.html")) else DEFAULT_INDEX
    return render_template_string(html)

@app.get("/api/available")
@require_pw
def api_available():
    # Harden param coercion
    day    = _coerce_one(request.args.getlist("day")    or request.args.get("day")    or "Monday")
    start  = _coerce_one(request.args.getlist("start")  or request.args.get("start")  or "0900")
    end    = _coerce_one(request.args.getlist("end")    or request.args.get("end")    or "1000")
    org    = _coerce_one(request.args.getlist("org")    or request.args.get("org")    or None) or None
    try:
        rows = find_available(day, start, end, org=org)
        return jsonify(ok=True, rows=rows)
    except Exception as e:
        log.exception("available failed")
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400

@app.get("/api/person")
@require_pw
def api_person():
    q   = _coerce_one(request.args.getlist("q") or request.args.get("q") or "")
    org = _coerce_one(request.args.getlist("org") or request.args.get("org") or None) or None
    try:
        data = person_info(q, org=org)
        return jsonify(ok=True, person=data)
    except Exception as e:
        log.exception("person failed")
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400

@app.get("/api/directory")
@require_pw
def api_directory():
    # lightweight directory for the Availability “All Cadets” panel
    org = _coerce_one(request.args.get("org") or None) or None
    try:
        rows = list_all_cadets(org=org)
        return jsonify(ok=True, rows=rows)
    except Exception as e:
        log.exception("directory failed")
        return jsonify(ok=False, error=str(e)), 400

@app.get("/api/cadet")
@require_pw
def api_cadet():
    # exact, by email/slug, or by full name
    key = _coerce_one(request.args.get("key") or "")
    org = _coerce_one(request.args.get("org") or None) or None
    try:
        return jsonify(ok=True, person=cadet_details(key, org=org))
    except Exception as e:
        log.exception("cadet details failed")
        return jsonify(ok=False, error=str(e)), 400

@app.get("/api/roster")
@require_pw
def api_roster():
    label = (_coerce_one(request.args.get("label")) or "gsu").lower()
    try:
        return jsonify(ok=True, rows=list_roster(label))
    except Exception as e:
        log.exception("roster failed")
        return jsonify(ok=False, error=str(e)), 400

@app.post("/api/att/add_event_and_mark")
@require_admin
def api_att_add_event_and_mark():
    body = request.get_json(force=True)
    date = (body.get("date","") or "").strip()
    event_type  = (body.get("event_type","PT") or "PT").strip().upper()
    event_other = (body.get("event_other","") or "").strip()
    label = (body.get("label") or "gsu").lower()
    marks = body.get("marks") or []
    if not date: return jsonify(ok=False, error="date required"), 400
    if event_type not in ("PT","LAB","OTHER"): return jsonify(ok=False, error="bad event_type"), 400
    header = f"{date} + {(event_other if event_type=='OTHER' else event_type)}"
    try:
        updated = add_event_and_mark(label, header, marks)
        return jsonify(ok=True, header=header, updated_cells=updated)
    except Exception as e:
        log.exception("add_event_and_mark failed")
        return jsonify(ok=False, error=str(e)), 400

@app.get("/api/att/events")
@require_pw
def api_att_events():
    label = (_coerce_one(request.args.get("label")) or "gsu").lower()
    return jsonify(ok=True, events=list_events(label))

@app.get("/api/att/day")
@require_pw
def api_att_day():
    label = (_coerce_one(request.args.get("label")) or "gsu").lower()
    date  = (_coerce_one(request.args.get("date")) or "").strip()
    target = None
    for e in list_events(label):
        if e["date"] == date: target = e; break
    if not target: return jsonify(ok=False, error="no event for that date"), 404
    try:
        return jsonify(ok=True, header=target["header"], records=read_event_records(label, target["col"]))
    except Exception as e:
        log.exception("day view failed")
        return jsonify(ok=False, error=str(e)), 400

@app.get("/api/att/leaderboard")
@require_pw
def api_att_leaderboard():
    # returns raw counts; client formats pretty text
    label = (_coerce_one(request.args.get("label")) or "gsu").lower()
    dfrom = _coerce_one(request.args.get("from") or "")
    dto   = _coerce_one(request.args.get("to")   or "")
    d1 = dt.datetime.strptime(dfrom,"%m/%d/%Y").date() if dfrom else None
    d2 = dt.datetime.strptime(dto,"%m/%d/%Y").date() if dto else None
    top = int(_coerce_one(request.args.get("top") or "10") or "10")
    try:
        data = leaderboard(label, d1, d2, top=top)
        return jsonify(ok=True, **data)
    except Exception as e:
        log.exception("leaderboard failed")
        return jsonify(ok=False, error=str(e)), 400

@app.get("/healthz")
def healthz():
    return "ok", 200

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
            with open(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"), "r", encoding="utf-8") as f:
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

# default inline template if /templates/index.html is absent
DEFAULT_INDEX = """<!doctype html><html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>ROTC Tools</title>
<link rel="stylesheet" href="/static/styles.css">
</head><body>
<div class="wrap">
  <h1>ROTC Tools</h1>
  <div class="tabs">
    <button class="tab on" data-for="availability">Availability</button>
    <button class="tab" data-for="attendance">Attendance</button>
    <button class="tab" data-for="reports">Reports</button>
  </div>
  <section id="availability" class="panel on">
    <div class="row">
      <label>Day</label>
      <select id="day"><option>Monday</option><option>Tuesday</option><option>Wednesday</option><option>Thursday</option><option>Friday</option></select>
      <label>Start</label><input id="a_start" value="0830">
      <label>End</label><input id="a_end" value="1030">
      <button id="a_go" class="btn">Search</button>
    </div>
    <div class="row">
      <button id="open_dir" class="btn">Open Cadet Directory</button>
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
      <input id="admin_pw" placeholder="admin password">
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
    <div id="rep_day_out"></div>
    <h3 style="margin-top:18px">Leaderboard</h3>
    <div class="row">
      <label>From</label><input id="lb_from" placeholder="8/1/2025">
      <label>To</label><input id="lb_to" placeholder="10/27/2025">
      <button id="lb_btn" class="btn">Build</button>
    </div>
    <div id="lb_out"></div>
    <h3 style="margin-top:18px">Charts (auto-load PNGs)</h3>
    <div id="charts" class="charts"></div>
  </section>
</div>
<script src="/static/script.js"></script>
</body></html>"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")), debug=os.getenv("FLASK_ENV")=="development")
