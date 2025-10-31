# app/__init__.py (paste this function into the file)
import os
import logging
from flask import Flask, redirect
from .config import Config
from .utils.logger import init_logging
from .utils.sheet_cache import init_cache_scheduler

def create_app():
    """
    Flask application factory.
    - config loaded from Config
    - logging initialized via init_logging(app)
    - root and /healthz routes added
    - blueprints registered if available (safe imports)
    - cache scheduler started once (guarded)
    """
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.from_object(Config)

    # ensure there's a cache dir value so sheet_cache helpers can use it
    app.config.setdefault("CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))
    app.config.setdefault("CACHE_REFRESH_HOUR", 5)
    app.config.setdefault("CACHE_REFRESH_MINUTE", 0)
    app.config.setdefault("TZ", "America/Chicago")
    app.config.setdefault("ATTENDANCE_TAB_NAME", "Attendance Roster")

    # --- Logging setup (this should configure app.logger) ---
    try:
        init_logging(app)
    except Exception:
        # Don't let logging setup crash the app; fallback to basic config
        logging.basicConfig(level=logging.INFO)
        app.logger.exception("init_logging failed; using basic logging fallback.")

    # --- small, safe root endpoints so Render health check is happy ---
    @app.get("/")
    def root():
        # change redirect to "/writer" or another URL if you'd prefer
        return redirect("/writer")

    @app.get("/healthz")
    def healthz():
        return "ok", 200

    # --- Register blueprints (safe imports so missing modules don't crash startup) ---
    def try_register(module_path, bp_name, url_prefix=None):
        """
        Safe blueprint importer: module_path is something like "app.routes.writer",
        bp_name is the variable name of the blueprint inside that module, e.g. "writer_bp".
        """
        try:
            mod = __import__(module_path, fromlist=[bp_name])
            bp = getattr(mod, bp_name)
            if url_prefix:
                app.register_blueprint(bp, url_prefix=url_prefix)
                app.logger.info("Registered blueprint %s at %s", bp_name, url_prefix)
            else:
                app.register_blueprint(bp)
                app.logger.info("Registered blueprint %s", bp_name)
        except Exception as e:
            app.logger.warning("Could not register %s from %s: %s", bp_name, module_path, e)

    # --- Example blueprint registrations (adjust to your filenames/blueprint names) ---
    # If your blueprint files live in app/routes/ use something like below.
    try_register("app.routes.writer", "writer_bp", url_prefix="/writer")
    try_register("app.routes.reports", "reports_bp", url_prefix="/reports")
    try_register("app.routes.admin", "admin_bp", url_prefix="/admin")
    # --- INSERT YOUR STUFF HERE ---
    # If you want to register additional blueprints or do extra initialization,
    # put it where this marker is. Use try/except around custom imports if you
    # want to avoid breaking the whole app on errors.

    # --- Start the cache scheduler exactly once per app instance ---
    # Guard against multiple starts (e.g., multiple imports / Gunicorn worker forks).
    if not app.config.get("SCHEDULER_STARTED", False):
        try:
            init_cache_scheduler(app)
            app.config["SCHEDULER_STARTED"] = True
        except Exception as e:
            app.logger.exception("Failed to start cache scheduler: %s", e)

    # --- Optional: global error handlers to avoid exposing tracebacks ---
    @app.errorhandler(404)
    def handle_404(e):
        return "Not Found", 404

    @app.errorhandler(500)
    def handle_500(e):
        app.logger.exception("Internal Server Error: %s", e)
        return "Internal Server Error", 500

    return app
