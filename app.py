# app.py — ROTC web app (clean rewrite)

import os
import json
import datetime as dt
import logging
from functools import wraps

from flask import (
    Flask, request, jsonify, render_template_string, send_from_directory
)

# ---- Backends you already have ----
# (these modules must exist in your repo)
from attendance import (
    list_events, read_event_records, leaderboard,
    list_roster, add_event_and_mark
)
from availability import find_available, person_info


# =========================
# Logging / App Init
# =========================
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s: %(message)s"
)
log = logging.getLogger("rotc")

# Default passwords (you can override via Render Env Vars)
APP_PASSWORD   = (os.getenv("APP_PASSWORD") or "letmein123").strip()
ADMIN_PASSWORD = (os.getenv("ADMIN_PASSWORD") or "letmein123").strip()

app = Flask(
    __name__,
    static_folder="static",
    static_url_path="/static",
)

# Helpful to avoid HEAD mapping surprises
app.url_map.strict_slashes = False


# =========================
# Auth helpers (query/header based)
# =========================
def _get_pw_from_request() -> str:
    # Accept via ?pw=, header X-APP-PW, or JSON body { pw: "" } for POSTs
    if request.method in ("POST", "PUT", "PATCH"):
        body = request.get_json(silent=True) or {}
        if "pw" in body and isinstance(body["pw"], str):
            return body["pw"].strip()
    return (request.args.get("pw")
            or request.headers.get("X-APP-PW")
            or "").strip()

def _get_admin_pw_from_request() -> str:
    if request.method in ("POST", "PUT", "PATCH"):
        body = request.get_json(silent=True) or {}
        if "admin_pw" in body and isinstance(body["admin_pw"], str):
            return body["admin_pw"].strip()
    return (request.args.get("admin_pw")
            or request.headers.get("X-ADMIN-PW")
            or "").strip()

def require_pw(view):
    @wraps(view)
    def _wrapped(*args, **kwargs):
        pw = _get_pw_from_request()
        if APP_PASSWORD and pw != APP_PASSWORD:
            return jsonify(ok=False, error="locked: add ?pw=..."), 401
        return view(*args, **kwargs)
    return _wrapped

def require_admin(view):
    @wraps(view)
    def _wrapped(*args, **kwargs):
        apw = _get_admin_pw_from_request()
        if ADMIN_PASSWORD and apw != ADMIN_PASSWORD:
            return jsonify(ok=False, error="admin auth failed"), 401
        return view(*args, **kwargs)
    return _wrapped


# =========================
# Error handlers (uniform JSON)
# =========================
@app.errorhandler(400)
def _bad_request(e):
    return jsonify(ok=False, error="bad request"), 400

@app.errorhandler(404)
def _not_found(e):
    return jsonify(ok=False, error="not found"), 404

@app.errorhandler(Exception)
def _unhandled(e):
    log.exception("Unhandled error")
    # Never leak internals to the browser unless you want to.
    return jsonify(ok=False, error=f"{type(e).__name__}"), 500


# =========================
# Root: serves the UI
# =========================
@app.get("/")
@require_pw
def index():
    """
    Try to load templates/index.html.
    If missing, serve a minimal fallback that still loads /static/*.
    """
    tpl_path = os.path.join(os.path.dirname(__file__), "templates", "index.html")
    try:
        with open(tpl_path, "r", encoding="utf-8") as f:
            html = f.read()
    except Exception:
        html = """<!doctype html>
<html><head><meta charset="utf-8">
<title>ROTC Tools</title>
<link rel="stylesheet" href="/static/styles.css">
</head><body>
  <div class="wrap">
    <h1>ROTC Tools</h1>
    <p>Template missing. Static + APIs should still work.</p>
  </div>
  <script src="/static/script.js"></script>
</body></html>"""
    return render_template_string(html)


# =========================
# Availability APIs
# =========================
@app.get("/api/available")
@require_pw
def api_available():
    """
    Query: ?day=Mon|Tue|... or full 'Monday', etc.
           ?start=HHMM  ?end=HHMM
           ?org=GSU|ULM|...
    """
    # normalize Mon/Tue → Monday/Tuesday
    day_map = dict(Mon="Monday", Tue="Tuesday", Wed="Wednesday",
                   Thu="Thursday", Fri="Friday")
    day_arg = (request.args.get("day") or "").strip()
    day = day_map.get(day_arg, day_arg or "Mon")

    start = (request.args.get("start") or "0900").strip()
    end   = (request.args.get("end") or "1000").strip()
    org   = (request.args.get("org") or "").strip() or None

    log.debug(f"/api/available day={day} start={start} end={end} org={org}")
    try:
        rows = find_available(day, start, end, org=org)
        return jsonify(ok=True, rows=rows)
    except Exception as e:
        log.exception("available failed")
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400


@app.get("/api/person")
@require_pw
def api_person():
    """
    Query: ?q=First Last | email
           ?org=GSU|ULM|... (optional filter)
    """
    q   = (request.args.get("q") or "").strip()
    org = (request.args.get("org") or "").strip() or None
    if not q:
        return jsonify(ok=False, error="q required"), 400
    log.debug(f"/api/person q={q} org={org}")
    try:
        data = person_info(q, org=org)
        return jsonify(ok=True, person=data)
    except Exception as e:
        log.exception("person lookup failed")
        return jsonify(ok=False, error=f"{type(e).__name__}: {e}"), 400


# =========================
# Attendance APIs
# =========================
@app.get("/api/roster")
@require_pw
def api_roster():
    """
    Query: ?label=gsu|ulm|latech   (defaults to gsu)
    """
    label = (request.args.get("label") or "gsu").lower()
    log.debug(f"/api/roster label={label}")
    try:
        rows = list_roster(label)
        return jsonify(ok=True, rows=rows)
    except Exception as e:
        log.exception("roster failed")
        return jsonify(ok=False, error=str(e)), 400


@app.post("/api/att/add_event_and_mark")
@require_admin
def api_att_add_event_and_mark():
    """
    Body (JSON):
      {
        "date": "MM/DD/YYYY",
        "event_type": "PT"|"LAB"|"OTHER",
        "event_other": "string if OTHER",
        "label": "gsu|ulm|...",
        "marks": [ {"first": "...", "last": "...", "status": "P|FTR|NFR|Excused"} ]
      }
    """
    body = request.get_json(force=True) or {}
    date = (body.get("date") or "").strip()
    event_type  = (body.get("event_type") or "PT").strip().upper()
    event_other = (body.get("event_other") or "").strip()
    label = (body.get("label") or "gsu").lower()
    marks = body.get("marks") or []

    if not date:
        return jsonify(ok=False, error="date required"), 400
    if event_type not in ("PT", "LAB", "OTHER"):
        return jsonify(ok=False, error="bad event_type"), 400

    header = f"{date} + {event_other if event_type == 'OTHER' else event_type}"
    log.debug(f"/api/att/add_event_and_mark label={label} header={header} marks={len(marks)}")
    try:
        updated = add_event_and_mark(label, header, marks)
        return jsonify(ok=True, header=header, updated_cells=updated)
    except Exception as e:
        log.exception("add_event_and_mark failed")
        return jsonify(ok=False, error=str(e)), 400


@app.get("/api/att/events")
@require_pw
def api_att_events():
    label = (request.args.get("label") or "gsu").lower()
    log.debug(f"/api/att/events label={label}")
    try:
        return jsonify(ok=True, events=list_events(label))
    except Exception as e:
        log.exception("list_events failed")
        return jsonify(ok=False, error=str(e)), 400


@app.get("/api/att/day")
@require_pw
def api_att_day():
    label = (request.args.get("label") or "gsu").lower()
    date  = (request.args.get("date") or "").strip()
    log.debug(f"/api/att/day label={label} date={date}")

    if not date:
        return jsonify(ok=False, error="date required"), 400

    try:
        target = None
        for e in list_events(label):
            if e.get("date") == date:
                target = e
                break
        if not target:
            return jsonify(ok=False, error="no event for that date"), 404

        records = read_event_records(label, target["col"])
        return jsonify(ok=True, header=target["header"], records=records)
    except Exception as e:
        log.exception("att day failed")
        return jsonify(ok=False, error=str(e)), 400


@app.get("/api/att/leaderboard")
@require_pw
def api_att_leaderboard():
    """
    Query: ?label=gsu|ulm|...  ?from=MM/DD/YYYY  ?to=MM/DD/YYYY  ?top=50
    """
    label = (request.args.get("label") or "gsu").lower()
    dfrom = (request.args.get("from") or "").strip()
    dto   = (request.args.get("to") or "").strip()
    top_n = int(request.args.get("top") or "50")

    d1 = dt.datetime.strptime(dfrom, "%m/%d/%Y").date() if dfrom else None
    d2 = dt.datetime.strptime(dto,   "%m/%d/%Y").date() if dto   else None

    log.debug(f"/api/att/leaderboard label={label} from={d1} to={d2} top={top_n}")
    try:
        data = leaderboard(label, d1, d2, top=top_n)
        return jsonify(ok=True, **data)
    except Exception as e:
        log.exception("leaderboard failed")
        return jsonify(ok=False, error=str(e)), 400


# =========================
# Diagnostics / Health
# =========================
@app.get("/api/debug/ping")
def ping():
    return jsonify(ok=True, pong=True, time=dt.datetime.utcnow().isoformat()+"Z")

@app.get("/healthz")
def healthz():
    return "ok", 200

@app.get("/api/envcheck")
def envcheck():
    """
    Lightweight environment + connectivity probe.
    Does *read-only* checks to prevent write-side effects.
    """
    out = {
        "have": {
            "APP_PASSWORD": bool(APP_PASSWORD),
            "ADMIN_PASSWORD": bool(ADMIN_PASSWORD),
            "GOOGLE_SHEET_URL": bool(os.getenv("GOOGLE_SHEET_URL")),
            "SPREADSHEET_ID": bool(os.getenv("SPREADSHEET_ID")),
            "WORKSHEET_NAME": bool(os.getenv("WORKSHEET_NAME")),
            "GSU_HEADER_ROW": bool(os.getenv("GSU_HEADER_ROW")),
            "ULM_HEADER_ROW": bool(os.getenv("ULM_HEADER_ROW")),
            "GOOGLE_SERVICE_ACCOUNT_JSON": bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")),
            "GOOGLE_SERVICE_ACCOUNT_JSON_PATH": bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")),
        },
        "service_account_email": None,
        "errors": {},
        "attendance_open_ok": None,
        "attendance_sample_rows": 0,
        "availability_http": {},
    }

    # Parse service account quickly (if present)
    try:
        if os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"):
            with open(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"), "r", encoding="utf-8") as f:
                out["service_account_email"] = json.load(f).get("client_email")
        elif os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"):
            out["service_account_email"] = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")).get("client_email")
    except Exception as e:
        out["errors"]["creds_parse"] = f"{type(e).__name__}: {e}"

    # Try availability CSV fetch (no parse, just reachability)
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

    # Try Sheets auth + open attendance (read-only)
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        info = None
        if os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"):
            with open(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"), "r", encoding="utf-8") as f:
                info = json.load(f)
        else:
            info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "{}"))
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(os.getenv("SPREADSHEET_ID")).worksheet(os.getenv("WORKSHEET_NAME"))
        vals = ws.get('A1:C10')
        out["attendance_open_ok"] = True
        out["attendance_sample_rows"] = len(vals)
    except Exception as e:
        out["attendance_open_ok"] = False
        out["errors"]["attendance_open"] = f"{type(e).__name__}: {e}"

    return jsonify(out)


# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    # Local dev default port; Render sets PORT env
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_ENV") == "development"
    app.run(host="0.0.0.0", port=port, debug=debug)

