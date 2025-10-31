from flask import Blueprint, current_app, render_template

from ..utils.sheet_cache import get_cached_data

bp = Blueprint("waterfall", __name__, url_prefix="/waterfall")


@bp.route("/")
def waterfall():
    app = current_app
    data = get_cached_data(app, "umr")
    matrix = data.get("entries", [])
    app.logger.debug("Waterfall matrix loaded with %d entries", len(matrix))
    return render_template("waterfall.html", matrix=matrix)
