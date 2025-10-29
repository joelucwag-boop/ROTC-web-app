from flask import Blueprint, render_template, current_app
from ..utils.sheet_cache import get_cached_data

bp = Blueprint("directory", __name__, url_prefix="/directory")

@bp.route("/")
def directory():
    app = current_app
    data = get_cached_data(app, "attendance")
    cadets = []
    if "names_by_ms" in data:
        for ms, namesets in data["names_by_ms"].items():
            for category, namelist in namesets.items():
                for n in namelist:
                    cadets.append((n, ms, category))
    return render_template("directory.html", cadets=cadets)
