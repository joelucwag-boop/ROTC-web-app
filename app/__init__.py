import os
from flask import Flask
from .config import Config
from .utils.logger import init_logging
from .utils.sheet_cache import init_cache_scheduler


def create_app():
    """Main Flask factory — creates the app and registers blueprints."""
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.from_object(Config)

    # --- Logging setup ---
    init_logging(app)

    # --- Register Blueprints ---
    from .routes import home, directory, reports, availability, oml, writer, waterfall

    app.register_blueprint(home.bp)
    app.register_blueprint(directory.bp)
    app.register_blueprint(reports.bp)
    app.register_blueprint(availability.bp)
    app.register_blueprint(oml.bp)
    app.register_blueprint(writer.bp)
    app.register_blueprint(waterfall.bp)

    # --- Initialize cache scheduler ---
    init_cache_scheduler(app)

    @app.route("/health")
    def health():
        return {"status": "ok", "cached": True}

    return app
