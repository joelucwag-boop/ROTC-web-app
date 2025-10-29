from flask import Blueprint, render_template, current_app
from ..utils.sheet_cache import get_cached_data

bp = Blueprint("reports", __name__, url_prefix="/reports")

@bp.route("/")
def reports():
    app = current_app
    data = get_cached_data(app, "attendance")
    table = data.get("table", [])
    overall = data.get("overall", {})
    return render_template("reports.html", table=table, overall=overall)
