from flask import Blueprint, render_template, current_app
from ..utils.sheet_cache import get_cached_data

bp = Blueprint("oml", __name__, url_prefix="/oml")

@bp.route("/")
def oml():
    app = current_app
    data = get_cached_data(app, "attendance")
    leaderboard = []
    if "names_by_ms" in data:
        for ms, namesets in data["names_by_ms"].items():
            count_present = len(namesets.get("Present", []))
            count_ftr = len(namesets.get("FTR", []))
            leaderboard.append({
                "MS": ms,
                "Score": count_present - count_ftr,
                "Present": count_present,
                "FTR": count_ftr,
            })
    leaderboard.sort(key=lambda x: x["Score"], reverse=True)
    return render_template("oml.html", leaderboard=leaderboard)
