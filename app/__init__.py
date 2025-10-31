import os
os.makedirs(os.path.join(os.path.dirname(__file__), "cache"), exist_ok=True)
from flask import Flask
from .config import Config
from .utils.logger import init_logging
from .utils.sheet_cache import init_cache_scheduler
import sys, os
print("PYTHONPATH:", sys.path)
print("APP PACKAGE FILE:", __file__)
print("Dir(app/integrations):", os.listdir(os.path.join(os.path.dirname(__file__), "integrations")))


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

# ---- optional fallback so 'gunicorn app:app' also works ----
try:
    app  # if already defined elsewhere, don't redefine
except NameError:
    app = create_app()

