# app/routes/admin.py
from flask import Blueprint, jsonify, current_app, request

from ..utils.sheet_cache import refresh_cache

bp = Blueprint("admin", __name__, url_prefix="/admin")

@bp.get("/refresh-cache")
def refresh_cache_now():
    cache_name = request.args.get("cache", "attendance")
    ok = refresh_cache(current_app, cache_name)
    return jsonify({"ok": ok, "cache": cache_name})
