from flask import Blueprint, render_template, request, current_app
from datetime import date

bp = Blueprint("writer", __name__, url_prefix="/writer")

@bp.route("/", methods=["GET", "POST"])
def writer():
    app = current_app
    message = ""
    date_default = date.today().isoformat()  # 'YYYY-MM-DD'

    if request.method == "POST":
        school = request.form.get("school")
        event = request.form.get("event")
        status = request.form.get("status")
        sel_date = request.form.get("date") or date_default

        app.logger.debug("Writer input: school=%s event=%s status=%s date=%s",
                         school, event, status, sel_date)

        if not app.config.get("ENABLE_WRITES", False):
            message = "⚠️ Write mode disabled. Set ENABLE_WRITES=True to enable."
        else:
            # TODO: call your safe write function here
            message = "✅ Would have written attendance (disabled)."

    return render_template("writer.html", message=message, date_default=date_default)

