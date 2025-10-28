import os, json, logging
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, abort, flash
from datetime import datetime, date
from utils.gutils import (
    get_attendance_dataframe, detect_header_row, list_attendance_dates,
    build_present_rates_by_ms, get_cadet_directory_rows, get_availability_df,
    find_cadet_availability, update_attendance_cell, get_status_by_date_and_ms,
    load_service_account_from_env
)

# ---- Flask setup ----
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))

LOG_LEVEL = os.environ.get("LOG_LEVEL","INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("rotc")

# ---- Passwords ----
APP_PASSWORD  = os.environ.get("APP_PASSWORD", "")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")

def require_user():
    if not session.get("user_ok"):
        return redirect(url_for("login", next=request.path))

def require_admin():
    if not session.get("admin_ok"):
        return redirect(url_for("admin_login", next=request.path))

@app.get("/login")
def login():
    return render_template("login.html")

@app.post("/login")
def login_post():
    pw = request.form.get("password","")
    if pw == APP_PASSWORD or pw == ADMIN_PASSWORD:
        session["user_ok"] = True
        if pw == ADMIN_PASSWORD:
            session["admin_ok"] = True
        return redirect(request.args.get("next") or url_for("home"))
    flash("Bad password")
    return redirect(url_for("login"))

@app.get("/admin-login")
def admin_login():
    return render_template("admin_login.html")

@app.post("/admin-login")
def admin_login_post():
    pw = request.form.get("password","")
    if pw == ADMIN_PASSWORD:
        session["user_ok"] = True
        session["admin_ok"] = True
        return redirect(request.args.get("next") or url_for("home"))
    flash("Bad admin password")
    return redirect(url_for("admin_login"))

@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("home"))

# ---- Home ----
@app.get("/")
def home():
    # minimal public home; charts require user login
    return render_template("home.html")

@app.get("/dashboard")
def dashboard():
    if not session.get("user_ok"): return require_user()
    try:
        df = get_attendance_dataframe()
        rates = build_present_rates_by_ms(df)
        # Return template which pulls JSON via ajax for Chart.js
        return render_template("dashboard.html", rates_json=json.dumps(rates))
    except Exception as e:
        log.exception("dashboard error")
        return render_template("error.html", msg=str(e)), 500

# JSON endpoint for charts
@app.get("/api/attendance/rates")
def api_rates():
    if not session.get("user_ok"): return abort(401)
    try:
        df = get_attendance_dataframe()
        rates = build_present_rates_by_ms(df)
        return jsonify(rates)
    except Exception as e:
        log.exception("rates error")
        return jsonify({"error": str(e)}), 500

# ---- Reports ----
@app.get("/reports")
def reports_menu():
    if not session.get("user_ok"): return require_user()
    dates = list_attendance_dates()
    return render_template("reports.html", dates=dates)

@app.get("/reports/daily")
def reports_daily():
    if not session.get("user_ok"): 
        return require_user()
    iso = request.args.get("date", "").strip()
    try:
        df = get_attendance_dataframe()
        rows = get_status_by_date_and_ms(df, iso) if iso else {}
        if rows is None:
            rows = {}
        return render_template("daily.html", rows=rows, iso=iso)
    except Exception as e:
        log.exception("daily report error")
        return render_template("error.html", msg=str(e)), 500

@app.get("/reports/weekly")
def weekly_report():
    if not session.get("user_ok"): 
        return require_user()
    try:
        df = get_attendance_dataframe()
        date_objs = list_attendance_dates() or []
        dates = [d["iso"] for d in date_objs][-7:]
        rows = {}
        for iso in dates:
            try:
                val = get_status_by_date_and_ms(df, iso) or {}
                rows[iso] = val
            except Exception as e:
                logging.getLogger("rotc").warning("weekly skip %s: %s", iso, e)
        return render_template("weekly.html", rows=rows, dates=list(rows.keys()))
    except Exception as e:
        log.exception("weekly report error")
        return render_template("error.html", msg=str(e)), 500


# ---- Directory (password protected) ----
@app.get("/directory")
def directory():
    if not session.get("user_ok"): return require_user()
    try:
        cadets = get_cadet_directory_rows()
        return render_template("directory.html", cadets=cadets)
    except Exception as e:
        log.exception("directory error")
        return render_template("error.html", msg=str(e)), 500

@app.get("/directory/<name>")
def directory_entry(name):
    if not session.get("user_ok"): return require_user()
    try:
        avail = get_availability_df()
        entry = avail[avail["FullName"].str.lower()==name.lower()]
        if entry.empty:
            return render_template("directory_entry.html", name=name, has=False, row=None)
        return render_template("directory_entry.html", name=name, has=True, row=entry.iloc[0].to_dict())
    except Exception as e:
        log.exception("directory entry error")
        return render_template("error.html", msg=str(e)), 500

# ---- Availability tracker (password protected) ----
@app.get("/availability")
def availability():
    if not session.get("user_ok"): return require_user()
    return render_template("availability.html")

@app.post("/availability/search")
def availability_search():
    if not session.get("user_ok"): return require_user()
    day = request.form.get("day","")
    window = request.form.get("window","").strip()  # e.g. "0900-1100"
    try:
        hits = find_cadet_availability(day, window)
        return render_template("availability_results.html", results=hits, day=day, window=window)
    except Exception as e:
        log.exception("availability error")
        return render_template("error.html", msg=str(e)), 500

# ---- OML ranking (based on UI95 logic) ----
@app.get("/oml")
def oml():
    if not session.get("user_ok"): return require_user()
    try:
        df = get_attendance_dataframe()
        # MS1–2: rank by Presents descending; MS3+ by FTR ascending
        import pandas as pd
        # Build counts per cadet
        name_col = [c for c in df.columns if str(c).strip().lower() in ("name first","first","firstname","namefirst")] or [None]
        # Use util to produce same result
        rows = []
        first = None; last=None; ms=None
        # We'll reuse directory rows for names + ms, then count in util
        cadets = get_cadet_directory_rows()
        for c in cadets:
            rows.append(c)  # contains Name and MS
        # util returns 'leaderboards' dict
        from utils.gutils import build_leaderboards_like_ui95
        boards = build_leaderboards_like_ui95(df, cadets)
        return render_template("oml.html", boards=boards)
    except Exception as e:
        log.exception("oml error")
        return render_template("error.html", msg=str(e)), 500

# ---- Attendance Writer (admin protected) ----
@app.get("/writer")
def writer_form():
    if not session.get("admin_ok"): return require_admin()
    dates = list_attendance_dates()
    return render_template("writer.html", dates=dates)

@app.post("/writer")
def writer_post():
    if not session.get("admin_ok"): return require_admin()
    cadet = request.form.get("cadet","").strip()
    iso = request.form.get("date","").strip()
    status = request.form.get("status","").strip()
    section = request.form.get("section","ANY")  # GSU/ULM/ANY
    try:
        ok = update_attendance_cell(cadet, iso, status, section)
        if ok:
            flash("Wrote attendance successfully.")
        else:
            flash("Could not write a cell (not found).")
        return redirect(url_for("writer_form"))
    except Exception as e:
        log.exception("writer post error")
        flash(f"Error: {e}")
        return redirect(url_for("writer_form"))
# --- Create today's date column (admin) ---
@app.post("/writer/create-today")
def writer_create_today():
    if not session.get("admin_ok"): return require_admin()
    suffix = request.form.get("suffix", "PT").strip() or "PT"
    try:
        info = add_date_column_for_sections(suffix)  # returns {'label','iso'}
        flash(f"Added header for today: {info['label']}")
        return redirect(url_for("writer_bulk", date=info["iso"]))
    except Exception as e:
        log.exception("create-today error")
        return render_template("error.html", msg=str(e)), 500

# --- Bulk writer form ---
@app.get("/writer/bulk")
def writer_bulk():
    if not session.get("admin_ok"): 
        return require_admin()
    from utils.gutils import get_cadet_directory_rows
    date_iso = request.args.get("date") or "__TODAY__"
    cadets = get_cadet_directory_rows() or []
    return render_template("writer_bulk.html", cadets=cadets, date_iso=date_iso)


# --- Bulk writer submit ---
@app.post("/writer/bulk")
def writer_bulk_post():
    if not session.get("admin_ok"): return require_admin()
    date_iso = request.form.get("date_iso") or "__TODAY__"
    section  = request.form.get("section", "ANY")
    total = ok = 0
    for key in request.form:
        if not key.startswith("status__"): 
            continue
        total += 1
        name = key.split("__",1)[1]
        val  = request.form.get(key)
        if val == "Excused":
            reason = (request.form.get(f"reason__{name}") or "").strip()
            if reason:
                val = f"Excused: {reason}"
        try:
            if update_attendance_cell(name, date_iso, val, section):
                ok += 1
        except Exception as e:
            log.warning("write fail %s: %s", name, e)
            continue
    flash(f"Wrote {ok}/{total} selections.")
    return redirect(url_for("writer_bulk", date=date_iso))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
