# app/routes/home.py
from flask import Blueprint, render_template

URL_PREFIX = "/home"                 # auto-registrar will use this
bp = Blueprint("home", __name__)     # <-- blueprint NAME = 'home'

@bp.get("/")
def index():
    # If you don’t have a template yet, return a string:
    return "Home is alive", 200
    # or: return render_template("home.html")
