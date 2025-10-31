# app/__init__.py
from flask import Flask
from .config import Config
from .utils.logger import init_logging
from .utils.sheet_cache import init_cache_scheduler

def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.from_object(Config)

    init_logging(app)

    if not app.config.get("SCHEDULER_STARTED", False):
        init_cache_scheduler(app)
        app.config["SCHEDULER_STARTED"] = True

    return app


