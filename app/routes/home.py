from flask import Blueprint, render_template, current_app
from ..utils.sheet_cache import get_cached_data
import datetime
# app/__init__.py
from .routes.admin import admin_bp          # example
from .routes.reports import reports_bp      # example
# ...
app.register_blueprint(admin_bp, url_prefix="/writer")
app.register_blueprint(reports_bp, url_prefix="/reports")

home_bp = Blueprint("home", __name__)

@bp.route("/")
def index():
    app = current_app
    today = datetime.date.today().strftime("%B %d, %Y")
    data = get_cached_data(app, "attendance") or {}

    rows = data.get("table", [])
    labels  = [r.get("MS Level", "") for r in rows]
    presents = [r.get("Present", 0) for r in rows]
    ftrs     = [r.get("FTR", 0) for r in rows]
    excused  = [r.get("Excused", 0) for r in rows]

    return render_template("home.html",
                           today=today,
                           labels=labels, presents=presents, ftrs=ftrs, excused=excused)
    @home_bp.get("/")
def root():
    return redirect("/writer", code=302)  # or "/dashboard"
