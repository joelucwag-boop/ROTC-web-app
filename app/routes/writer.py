# app/routes/writer.py
from flask import Blueprint, render_template

URL_PREFIX = "/writer"
bp = Blueprint("writer", __name__)   # <-- blueprint NAME = 'writer'

@bp.get("/")
def index():
    # keep it simple until templates are stable
    return "Writer is alive", 200
    # or your real page:
    # message = "..."
    # date_default = "..."
    # return render_template("writer.html", message=message, date_default=date_default)
