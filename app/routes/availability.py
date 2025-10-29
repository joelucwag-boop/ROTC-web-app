from flask import Blueprint, render_template, request, current_app
from datetime import datetime

bp = Blueprint("availability", __name__, url_prefix="/availability")

@bp.route("/", methods=["GET", "POST"])
def availability():
    app = current_app
    results = []
    selected_day = ""
    if request.method == "POST":
        selected_day = request.form.get("day", "").lower().strip()
        # In production, you'd parse availability sheet here
        results = [
            {"name": "Cadet A", "available": True, "time": "0600-0700"},
            {"name": "Cadet B", "available": False, "time": "None"},
        ]
        app.logger.debug("Availability search for %s returned %d results", selected_day, len(results))
    return render_template("availability.html", results=results, selected_day=selected_day)
