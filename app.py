"""
app.py — ROTC Attendance + Availability Web Service (hardened)
Joseph Waguespack / KiwiAutoTech 2025 Edition
"""

import os, io, json, re, datetime as dt, traceback, logging
from functools import wraps
from flask import Flask, request, jsonify, render_template_string, send_from_directory

# --- imports from modules ---
from attendance import (
    list_events, read_event_records, leaderboard,
    list_roster, add_event_and_mark
)
from availability import find_available, person_info

# --- LOGGING CONFIG ---
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
log = logging.getLogger("app")

# --- FLASK INIT ---
app = Flask(__name__, static_folder="static", static_url_path="/static")

# --- ENV VARS ---
APP_PASSWORD = os.getenv("APP_PASSWORD", "").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "").strip()

# --- AUTH DECORATORS ---
def require_pw(view):
    @wraps(view)
    def _wrap(*a, **k):
        pw = (
            request.args.get("pw")
            or request.headers.get("X-APP-PW")
            or (request.get_json(silent=True) or {}).get("pw", "")
        )
        if APP_PASSWORD and pw != APP_PASSWORD:
            log.warning(f"Unauthorized access attempt from {request.remote_addr}")
            return "<h1>Locked</h1><p>Append ?pw=YOURPASSWORD</p>", 401
        return view(*a, **k)
    return _wrap

def require_admin(view):
    @wraps(view)
    def _wrap(*a, **k):
        pw = (
            request.args.get("admin_pw")
            or (request.get_json(silent=True) or {}).get("admin_pw", "")
        )
        if ADMIN_PASSWORD and pw != ADMIN_PASSWORD:
            log.warning(f"Admin auth failed for {request.remote_addr}")
            return jsonify(ok=False, error="admin auth failed"), 401
        return view(*a, **k)
    return _wrap

# --- INDEX ROUTE ---
@app.route("/", methods=["GET", "HEAD"])
@require_pw
def index():
    if request.method == "HEAD":
        return ("", 200)
    html_path = os.path.join(app.root_path, "templates", "index.html")
    try:
        if os.path.exists(html_path):
            with open(html_path, "r", encoding="utf-8") as f:
                return render_template_string(f.read())
        else:
            log.warning("Missing templates/index.html — serving fallback UI.")
            return """<!doctype html><html><head>
<meta charset="utf-8"><title>ROTC Tools</title>
<link rel="stylesheet" href="/static/styles.css">
</head><body><div class="wrap">
<h1>ROTC Tools</h1>
<p>Template missing. Static and APIs should still function.</p>
<script src="/static/script.js"></script></div></body></html>"""
    except Exception as e:
        log.exception("Error rendering index:")
        return f"<pre>Fatal error: {e}</pre>", 500

# --- API: Availability ---
@app.route("/api/available", methods=["GET", "HEAD"])
@require_pw
def api_available():
    if request.method == "HEAD":
        return ("", 200)
    day_map = {"Mon":"Monday","Tue":"Tuesday","Wed":"Wednesday","Thu":"Thursday","Fri":"Friday"}
    day = day_map.get(request.args.get("day","Mon"), request.args.get("day","Mon"))
    start = request.args.get("start","0900")
    end = request.args.get("end","1000")
    org = request.args.get("org")
    try:
        rows = find_available(day, start, end, org=org)
        log.debug(f"/api/available -> {len(rows)} results for {day} {start}-{end}")
        return jsonify(ok=True, rows=rows)
    except Exception as e:
        log.exception("api_available failed:")
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400

# --- API: Person ---
@app.route("/api/person", methods=["GET", "HEAD"])
@require_pw
def api_person():
    if request.method == "HEAD":
        return ("", 200)
    q = request.args.get("q", "")
    org = request.args.get("org")
    try:
        person = person_info(q, org=org)
        return jsonify(ok=True, person=person)
    except Exception as e:
        log.exception("api_person failed:")
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400

# --- API: Roster ---
@app.route("/api/roster", methods=["GET", "HEAD"])
@require_pw
def api_roster():
    if request.method == "HEAD":
        return ("", 200)
    label = (request.args.get("label") or "gsu").lower()
    try:
        rows = list_roster(label)
        return jsonify(ok=True, rows=rows)
    except Exception as e:
        log.exception("api_roster failed:")
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400

# --- API: Attendance Add + Mark ---
@app.route("/api/att/add_event_and_mark", methods=["POST", "HEAD"])
@require_admin
def api_att_add_event_and_mark():
    if request.method == "HEAD":
        return ("", 200)
    try:
        body = request.get_json(force=True)
        date = body.get("date","").strip()
        etype = (body.get("event_type","PT") or "PT").strip().upper()
        other = (body.get("event_other","") or "").strip()
        label = (body.get("label") or "gsu").lower()
        marks = body.get("marks") or []
        if not date: return jsonify(ok=False, error="date required"), 400
        if etype not in ("PT","LAB","OTHER"): return jsonify(ok=False, error="bad event_type"), 400
        header = f"{date} + {(other if etype=='OTHER' else etype)}"
        updated = add_event_and_mark(label, header, marks)
        return jsonify(ok=True, header=header, updated_cells=updated)
    except Exception as e:
        log.exception("add_event_and_mark failed:")
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400

# --- API: Events list ---
@app.route("/api/att/events", methods=["GET", "HEAD"])
@require_pw
def api_att_events():
    if request.method == "HEAD": return ("", 200)
    label = (request.args.get("label") or "gsu").lower()
    return jsonify(ok=True, events=list_events(label))

# --- API: Attendance Day ---
@app.route("/api/att/day", methods=["GET", "HEAD"])
@require_pw
def api_att_day():
    if request.method == "HEAD": return ("", 200)
    label = (request.args.get("label") or "gsu").lower()
    date = request.args.get("date","").strip()
    for e in list_events(label):
        if e["date"] == date:
            return jsonify(ok=True, header=e["header"], records=read_event_records(label, e["col"]))
    return jsonify(ok=False, error="no event for that date"), 404

# --- API: Leaderboard ---
@app.route("/api/att/leaderboard", methods=["GET", "HEAD"])
@require_pw
def api_att_leaderboard():
    if request.method == "HEAD": return ("", 200)
    label = (request.args.get("label") or "gsu").lower()
    dfrom = request.args.get("from","")
    dto = request.args.get("to","")
    try:
        d1 = dt.datetime.strptime(dfrom,"%m/%d/%Y").date() if dfrom else None
        d2 = dt.datetime.strptime(dto,"%m/%d/%Y").date() if dto else None
        data = leaderboard(label, d1, d2, top=int(request.args.get("top","50")))
        return jsonify(ok=True, **data)
    except Exception as e:
        log.exception("leaderboard failed:")
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400

# --- COMPATIBILITY ROUTES ---
@app.route("/api/attendance/roster", methods=["GET", "HEAD"])
@require_pw
def compat_roster(): return api_roster()

@app.route("/api/attendance/save", methods=["POST", "HEAD"])
@require_admin
def compat_save(): return api_att_add_event_and_mark()

@app.route("/api/reports/leaderboard", methods=["GET", "HEAD"])
@require_pw
def compat_lb(): return api_att_leaderboard()

# --- DEBUG STATIC CHECK ---
@app.route("/api/debug/static")
@require_pw
def debug_static():
    out = {}
    for name in ["script.js","styles.css"]:
        p = os.path.join(app.static_folder, name)
        out[name] = {"exists": os.path.exists(p), "size": os.path.getsize(p) if os.path.exists(p) else 0}
    return jsonify(ok=True, static_folder=app.static_folder, files=out)

# --- HEALTH + ENV CHECK ---
@app.route("/api/envcheck")
def envcheck():
    out = {"have":{}, "errors":{}, "service_account_email": None}
    keys = ["APP_PASSWORD","ADMIN_PASSWORD","GOOGLE_SHEET_URL","SPREADSHEET_ID","WORKSHEET_NAME",
            "GSU_HEADER_ROW","ULM_HEADER_ROW","GOOGLE_SERVICE_ACCOUNT_JSON","GOOGLE_SERVICE_ACCOUNT_JSON_PATH"]
    for k in keys: out["have"][k] = bool(os.getenv(k))
    # Try parse creds
    try:
        info = None
        if os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"):
            with open(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"),"r",encoding="utf-8") as f: info=json.load(f)
        else:
            info=json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","{}"))
        out["service_account_email"]=info.get("client_email")
    except Exception as e:
        out["errors"]["creds_parse"]=f"{type(e).__name__}: {e}"
    # Try sheet fetch
    try:
        import requests
        url=os.getenv("GOOGLE_SHEET_URL")
        if url:
            r=requests.get(url,timeout=15)
            out["availability_http"]={"status":r.status_code,"ok":r.ok,"len":len(r.text)}
        else:
            out["availability_http"]={"status":None,"ok":False,"len":0}
    except Exception as e:
        out["errors"]["availability_http"]=f"{type(e).__name__}: {e}"
    return jsonify(out)

@app.get("/healthz")
def healthz(): return "ok",200

# --- MAIN ---
if __name__ == "__main__":
    port = int(os.getenv("PORT","5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

