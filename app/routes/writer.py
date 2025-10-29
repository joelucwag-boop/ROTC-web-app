from flask import Blueprint, render_template, request, current_app

bp = Blueprint("writer", __name__, url_prefix="/writer")

@bp.route("/", methods=["GET", "POST"])
def writer():
    app = current_app
    message = ""
    if request.method == "POST":
        school = request.form.get("school")
        event = request.form.get("event")
        status = request.form.get("status")
        date = request.form.get("date")
        app.logger.debug("Writer input: %s %s %s %s", school, event, status, date)

        if not app.config["ENABLE_WRITES"]:
            message = "⚠️ Write mode disabled. ENABLE_WRITES=False."
            app.logger.info("Write attempted but disabled.")
        else:
            # placeholder: write logic would go here
            message = "✅ Would have written attendance (disabled in current mode)."
    return render_template("writer.html", message=message)
