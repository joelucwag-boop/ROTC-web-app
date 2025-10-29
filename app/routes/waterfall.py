from flask import Blueprint, render_template, current_app

bp = Blueprint("waterfall", __name__, url_prefix="/waterfall")

@bp.route("/")
def waterfall():
    app = current_app
    app.logger.debug("Waterfall matrix requested.")
    # This is stubbed — would pull positions from the UMR tab.
    matrix = [
        {"position": "Battalion Commander", "name": "Cadet Smith"},
        {"position": "XO", "name": "Cadet Johnson"},
        {"position": "S3", "name": "Cadet Lee"},
    ]
    return render_template("waterfall.html", matrix=matrix)
