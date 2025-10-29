from flask import Blueprint, render_template, current_app
from ..utils.sheet_cache import get_cached_data
import datetime

bp = Blueprint("home", __name__, url_prefix="/")

@bp.route("/")
def index():
    app = current_app
    today = datetime.date.today()
    data = get_cached_data(app, "attendance") or {}

    # Extract simple values for charting
    labels = [r.get("MS Level") for r in data.get("table", [])]
    presents = [r.get("Present") for r in data.get("table", [])]
    ftrs = [r.get("FTR") for r in data.get("table", [])]
    excused = [r.get("Excused") for r in data.get("table", [])]

    return render_template(
        "home.html",
        today=today.strftime("%B %d, %Y"),
        labels=labels,
        presents=presents,
        ftrs=ftrs,
        excused=excused,
    )
