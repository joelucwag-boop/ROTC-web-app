"""Application factory and blueprint registration."""

import importlib
import inspect
import logging
import os
import pkgutil

from flask import Blueprint, Flask, redirect, url_for

from .config import Config
from .utils.logger import init_logging
from .utils.sheet_cache import init_cache_scheduler


def create_app() -> Flask:
    """Create and configure the Flask application."""

    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.from_object(Config)

    # sane defaults for deployment environments that omit config
    app.config.setdefault("CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))
    app.config.setdefault("CACHE_REFRESH_HOUR", 5)
    app.config.setdefault("CACHE_REFRESH_MINUTE", 0)
    app.config.setdefault("TZ", "America/Chicago")
    app.config.setdefault("ATTENDANCE_TAB_NAME", "Attendance Roster")

    # Initialise logging with a defensive fallback so deploys never fail on logging
    try:
        init_logging(app)
    except Exception:  # pragma: no cover - only hit during catastrophic logging failure
        logging.basicConfig(level=logging.INFO)
        app.logger.exception("init_logging failed; using basic logging fallback")

    # Basic routes ---------------------------------------------------------
    @app.get("/")
    def root():
        """Redirect the bare domain to the dashboard."""

        return redirect("/home")

    @app.get("/healthz")
    def healthz():
        """Lightweight liveness probe for Render."""

        return "ok", 200

    # Blueprint auto-discovery ---------------------------------------------
    def register_all_blueprints() -> None:
        base_pkg = "app.routes"
        try:
            pkg = importlib.import_module(base_pkg)
        except Exception as exc:
            app.logger.warning("Could not import %s: %s", base_pkg, exc)
            return

        for modinfo in pkgutil.iter_modules(pkg.__path__):
            name = f"{base_pkg}.{modinfo.name}"
            try:
                module = importlib.import_module(name)
            except Exception as exc:
                app.logger.warning("Skipping %s (import error): %s", name, exc)
                continue

            blueprints = [
                obj
                for _, obj in inspect.getmembers(module)
                if isinstance(obj, Blueprint)
            ]
            if not blueprints:
                continue

            url_prefix = getattr(module, "URL_PREFIX", None)
            for bp in blueprints:
                prefix = url_prefix or f"/{modinfo.name}"
                try:
                    app.register_blueprint(bp, url_prefix=prefix)
                    app.logger.info("Registered %s at %s", bp.name, prefix)
                except Exception as exc:
                    app.logger.warning("Failed registering %s at %s: %s", bp.name, prefix, exc)

    register_all_blueprints()

    # Navigation context ---------------------------------------------------
    @app.context_processor
    def inject_nav_links():
        """Expose navigation links while tolerating missing endpoints."""

        try:
            configured = app.config.get("NAVIGATION_ITEMS")
            if configured:
                raw_items = list(configured)
            else:
                raw_items = [
                    ("Home", "home.index"),
                    ("Writer", "writer.index"),
                    ("Reports", "reports.index"),
                    ("Directory", "directory.index"),
                    ("Availability", "availability.index"),
                    ("OML", "oml.index"),
                    ("Waterfall", "waterfall.index"),
                    ("Admin", "admin.index"),
                ]

            prepared = []
            for item in raw_items:
                if isinstance(item, dict):
                    label = item.get("label")
                    endpoint = item.get("endpoint")
                else:
                    try:
                        label, endpoint = item
                    except Exception:
                        app.logger.debug("Skipping malformed navigation item", extra={"item": item})
                        continue

                if not label or not endpoint:
                    app.logger.debug(
                        "Skipping navigation item with missing data", extra={"item": item}
                    )
                    continue

                if endpoint not in app.view_functions:
                    app.logger.debug(
                        "Navigation endpoint unavailable; skipping", extra={"endpoint": endpoint}
                    )
                    continue

                try:
                    href = url_for(endpoint)
                except Exception as exc:
                    app.logger.warning(
                        "Navigation link could not be generated",
                        extra={"endpoint": endpoint, "error": str(exc)},
                    )
                    continue

                prepared.append({"label": label, "endpoint": endpoint, "href": href})

            return {"nav_links": prepared}
        except Exception:  # pragma: no cover - defensive guard for templates
            app.logger.exception("Failed to prepare navigation links")
            return {"nav_links": []}

    # Cache scheduler ------------------------------------------------------
    if not app.config.get("SCHEDULER_STARTED", False):
        try:
            init_cache_scheduler(app)
            app.config["SCHEDULER_STARTED"] = True
        except Exception as exc:
            app.logger.exception("Failed to start cache scheduler: %s", exc)

    # Minimal error handlers -----------------------------------------------
    @app.errorhandler(404)
    def _handle_404(error):
        return "Not Found", 404

    @app.errorhandler(500)
    def _handle_500(error):
        app.logger.exception("500: %s", error)
        return "Internal Server Error", 500

    return app

