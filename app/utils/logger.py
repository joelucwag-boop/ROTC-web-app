import logging
import os


def init_logging(app):
    """Configure global logging for the entire Flask app."""
    log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "app.log")

    log_level = app.config.get("LOG_LEVEL", "DEBUG").upper()
    numeric_level = getattr(logging, log_level, logging.DEBUG)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] [%(name)s:%(lineno)d] - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    app.logger = logging.getLogger("gsu_attendance_dashboard")
    app.logger.setLevel(numeric_level)
    app.logger.info("Logging initialized at %s level", log_level)
