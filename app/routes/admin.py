# app/routes/admin.py
from flask import Blueprint, jsonify, current_app
from app.utils.sheet_cache import refresh_cache

bp = Blueprint("admin", __name__, url_prefix="/admin")

@bp.get("/refresh-cache")
def refresh_cache_now():
    refresh_cache(current_app, kind="attendance")
    return jsonify({"ok": True})
