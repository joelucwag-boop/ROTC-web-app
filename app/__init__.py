# app/__init__.py
import os, logging, pkgutil, importlib, inspect
from flask import Flask, redirect, url_for
from .config import Config
from .utils.logger import init_logging
from .utils.sheet_cache import init_cache_scheduler
from flask import Blueprint  # type: ignore

def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.from_object(Config)

    # sane defaults
    app.config.setdefault("CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))
    app.config.setdefault("CACHE_REFRESH_HOUR", 5)
    app.config.setdefault("CACHE_REFRESH_MINUTE", 0)
    app.config.setdefault("TZ", "America/Chicago")
    app.config.setdefault("ATTENDANCE_TAB_NAME", "Attendance Roster")

    # logging
    try:
        init_logging(app)
    except Exception:
        logging.basicConfig(level=logging.INFO)
        app.logger.exception("init_logging failed; using basic logging fallback.")

    # ---- root + health ----
    @app.get("/")
    def root():
        # change to "/writer" if you want that page first
        return redirect("/home")

    @app.get("/healthz")
    def healthz():
        return "ok", 200

    # ---- auto-discover and register ALL blueprints in app/routes ----
    def register_all_blueprints():
        base_pkg = "app.routes"
        try:
            pkg = importlib.import_module(base_pkg)
        except Exception as e:
            app.logger.warning("Could not import %s: %s", base_pkg, e)
            return

        for modinfo in pkgutil.iter_modules(pkg.__path__):
            name = f"{base_pkg}.{modinfo.name}"
            try:
                m = importlib.import_module(name)
            except Exception as e:
                app.logger.warning("Skipping %s (import error): %s", name, e)
                continue

            # find any Blueprint objects exported by the module
            bps = [
                (attr_name, obj)
                for attr_name, obj in inspect.getmembers(m)
                if isinstance(obj, Blueprint)
            ]
            if not bps:
                continue

            # optional per-module URL prefix:
            #   set URL_PREFIX = "/writer" in writer.py to override.
            url_prefix = getattr(m, "URL_PREFIX", None)

            for attr_name, bp in bps:
                # default prefix = "/<module name>" if none specified
                prefix = url_prefix or f"/{modinfo.name}"
                try:
                    app.register_blueprint(bp, url_prefix=prefix)
                    app.logger.info("Registered %s.%s at %s", name, attr_name, prefix)
                except Exception as e:
                    app.logger.warning("Failed registering %s.%s: %s", name, attr_name, e)

    register_all_blueprints()

    @app.context_processor
    def _inject_nav_links():
        nav_items = [
            ("Home", "home.index"),
            ("Writer", "writer.index"),
            ("Reports", "reports.index"),
            ("Directory", "directory.index"),
            ("Availability", "availability.index"),
            ("OML", "oml.index"),
            ("Waterfall", "waterfall.index"),
            ("Admin", "admin.index"),
        ]

        available = []
        for label, endpoint in nav_items:
            if endpoint not in app.view_functions:
                continue

            try:
                href = url_for(endpoint)
            except Exception:
                app.logger.warning(
                    "Navigation link could not be generated",
                    extra={"endpoint": endpoint},
                )
                continue

            available.append(
                {
                    "label": label,
                    "endpoint": endpoint,
                    "href": href,
                }
            )

        return {"nav_links": available}

    # ---- start scheduler once ----
    if not app.config.get("SCHEDULER_STARTED", False):
        try:
            init_cache_scheduler(app)
            app.config["SCHEDULER_STARTED"] = True
        except Exception as e:
            app.logger.exception("Failed to start cache scheduler: %s", e)

    # minimal error handlers
    @app.errorhandler(404)
    def _404(e): return "Not Found", 404

    @app.errorhandler(500)
    def _500(e):
        app.logger.exception("500: %s", e)
        return "Internal Server Error", 500

    return app
