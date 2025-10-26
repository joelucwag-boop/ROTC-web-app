import os, json, re, io, datetime as dt
from functools import wraps
from flask import Flask, request, jsonify, render_template_string
from attendance import list_events, read_event_records, leaderboard, list_roster, add_event_and_mark
from availability import find_available, person_info

APP_PASSWORD = os.getenv("APP_PASSWORD","").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD","").strip()

app = Flask(__name__, static_folder="static", static_url_path="/static")

def require_pw(view):
    from functools import wraps
    @wraps(view)
    def _w(*a, **k):
        pw = request.args.get("pw") or request.headers.get("X-APP-PW") or ""
        if APP_PASSWORD and pw != APP_PASSWORD:
            return "<h1>Locked</h1><p>Append ?pw=YOURPASSWORD</p>", 401
        return view(*a, **k)
    return _w

def require_admin(view):
    from functools import wraps
    @wraps(view)
    def _w(*a, **k):
        admin_pw = request.args.get("admin_pw") or (request.get_json(silent=True) or {}).get("admin_pw") or ""
        if ADMIN_PASSWORD and admin_pw != ADMIN_PASSWORD:
            return jsonify(ok=False, error="admin auth failed"), 401
        return view(*a, **k)
    return _w

@app.get("/")
@require_pw
def index():
    try:
        # Try the real template file first
        p = os.path.join(os.path.dirname(__file__), "templates", "index.html")
        with open(p, "r", encoding="utf-8") as f:
            html = f.read()
    except Exception as e:
        # Fallback: inline minimal HTML so the page always loads
        html = """<!doctype html>
<html><head><meta charset="utf-8">
<title>ROTC Tools</title>
<link rel="stylesheet" href="/static/styles.css">
</head><body>
<div class="wrap">
  <h1>ROTC Tools</h1>
  <p>Template file missing. Static and APIs should still work.</p>
  <script src="/static/script.js"></script>
</div>
</body></html>"""
    return render_template_string(html)


@app.get("/api/available")
@require_pw
def api_available():
    day = request.args.get("day","Mon")
    start = request.args.get("start","0900")
    end = request.args.get("end","1030")
    return jsonify(ok=True, people=find_available(day,start,end))

@app.get("/api/person")
@require_pw
def api_person():
    row = request.args.get("row","")
    drop_days = request.args.get("drop_days","0")=="1"
    info = person_info(row, drop_days=drop_days)
    if info is None:
        return jsonify(ok=False, error="not found"), 404
    return jsonify(ok=True, fields=info)

@app.get("/api/roster")
@require_pw
def api_roster():
    label = (request.args.get("label") or "gsu").lower()
    try:
        return jsonify(ok=True, rows=list_roster(label))
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 400

@app.post("/api/att/add_event_and_mark")
@require_admin
def api_att_add_event_and_mark():
    body = request.get_json(force=True)
    date = body.get("date","").strip()
    event_type = (body.get("event_type","PT") or "PT").strip().upper()
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
        return jsonify(ok=False, error=str(e)), 400

@app.get("/api/att/events")
@require_pw
def api_att_events():
    label = (request.args.get("label") or "gsu").lower()
    return jsonify(ok=True, events=list_events(label))

@app.get("/api/att/day")
@require_pw
def api_att_day():
    label = (request.args.get("label") or "gsu").lower()
    date  = request.args.get("date","").strip()
    target = None
    for e in list_events(label):
        if e["date"] == date: target = e; break
    if not target: return jsonify(ok=False, error="no event for that date"), 404
    return jsonify(ok=True, header=target["header"], records=read_event_records(label, target["col"]))

@app.get("/api/att/leaderboard")
@require_pw
def api_att_leaderboard():
    label = (request.args.get("label") or "gsu").lower()
    dfrom = request.args.get("from","")
    dto   = request.args.get("to","")
    d1 = dt.datetime.strptime(dfrom,"%m/%d/%Y").date() if dfrom else None
    d2 = dt.datetime.strptime(dto,"%m/%d/%Y").date() if dto else None
    data = leaderboard(label, d1, d2, top=int(request.args.get("top","50")))
    return jsonify(ok=True, **data)

@app.get("/healthz")
def healthz():
    return "ok", 200
# --- TEMP: environment + connectivity check ---
@app.get("/api/envcheck")
def envcheck():
    import os, json, traceback
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
    # Try to parse service account (env or file)
    try:
        if os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"):
            p = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")
            with open(p, "r", encoding="utf-8") as f:
                out["service_account_email"] = json.load(f)["client_email"]
        elif os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"):
            out["service_account_email"] = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))["client_email"]
    except Exception as e:
        out["errors"]["creds_parse"] = f"{type(e).__name__}: {e}"

    # Try availability CSV fetch (HEAD request)
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

    # Try Sheets auth + open roster (no writes)
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        import json as _json, os as _os
        info = None
        if _os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"):
            with open(_os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"), "r", encoding="utf-8") as f:
                info = _json.load(f)
        else:
            info = _json.loads(_os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "{}"))
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(_os.getenv("SPREADSHEET_ID")).worksheet(_os.getenv("WORKSHEET_NAME"))
        # read a tiny range to confirm access
        vals = ws.get('A1:C10')
        out["attendance_open_ok"] = True
        out["attendance_sample_rows"] = len(vals)
    except Exception as e:
        out["attendance_open_ok"] = False
        out["errors"]["attendance_open"] = f"{type(e).__name__}: {e}"

    return out


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")), debug=os.getenv("FLASK_ENV")=="development")
