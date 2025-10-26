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
    return render_template_string(open(os.path.join(os.path.dirname(__file__),"templates","index.html"),"r",encoding="utf-8").read())

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")), debug=os.getenv("FLASK_ENV")=="development")
