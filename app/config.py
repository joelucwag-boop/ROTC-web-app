import os
from datetime import time


class Config:
    """Base configuration loaded from environment variables."""

    # --- General ---
    SECRET_KEY = os.getenv("SECRET_KEY", "super-secret-key")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
    TZ = os.getenv("TZ", "America/Chicago")

    # --- Google Sheets / Roster ---
    SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
    WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Attendance Roster")
    GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    AVAILABILITY_CSV_URL = os.getenv("AVAILABILITY_CSV_URL", "")
    UMR_TAB_NAME = os.getenv("UMR_TAB_NAME", "UMR")
    PROGRAM_COLUMN = os.getenv("PROGRAM_COLUMN", "")
    AVAILABILITY_NAME_COLUMN = os.getenv("AVAILABILITY_NAME_COLUMN", "")
    AVAILABILITY_PASSWORD = os.getenv("AVAILABILITY_PASSWORD", "")
    WRITER_PASSWORD = os.getenv("WRITER_PASSWORD", "")
    UMR_MAPPING_JSON = os.getenv("UMR_MAPPING_JSON", "")

    # --- Access / Auth ---
    ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
    APP_PASSWORD = os.getenv("APP_PASSWORD", "")
    ENABLE_WRITES = os.getenv("ENABLE_WRITES", "False").lower() == "true"

    # --- Cache ---
    CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
    CACHE_REFRESH_HOUR = int(os.getenv("CACHE_REFRESH_HOUR", "5"))  # 0500 CST
    CACHE_REFRESH_MINUTE = int(os.getenv("CACHE_REFRESH_MINUTE", "0"))

    # --- Attendance Colors ---
    ATT_COLOR_PRESENT = os.getenv("ATT_COLOR_PRESENT", "#00ff00")
    ATT_COLOR_FTR = os.getenv("ATT_COLOR_FTR", "#ff0000")
    ATT_COLOR_EXCUSED = os.getenv("ATT_COLOR_EXCUSED", "#ffff00")

    # --- Misc ---
    DEBUG = os.getenv("FLASK_DEBUG", "False").lower() == "true"
    NAVIGATION_ITEMS = (
        {"label": "Home", "endpoint": "home.index"},
        {"label": "Writer", "endpoint": "writer.index"},
        {"label": "Reports", "endpoint": "reports.index"},
        {"label": "Directory", "endpoint": "directory.index"},
        {"label": "Availability", "endpoint": "availability.index"},
        {"label": "OML", "endpoint": "oml.index"},
        {"label": "Waterfall", "endpoint": "waterfall.index"},
        {"label": "Admin", "endpoint": "admin.index"},
    )

    @staticmethod
    def refresh_time():
        """Return the daily refresh time as a datetime.time object."""
        return time(hour=Config.CACHE_REFRESH_HOUR, minute=Config.CACHE_REFRESH_MINUTE)
