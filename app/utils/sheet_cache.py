import os
import json
import threading
import time
import pickle
from datetime import datetime, timedelta
import pytz
import logging

from pathlib import Path

# app/utils/sheet_cache.py

# app/utils/sheet_cache.py
from pathlib import Path
import logging
import sys, os, logging, pathlib
log = logging.getLogger(__name__)
log.info("SHEET_CACHE MARKER v7 | FILE=%s", __file__)
log.info("PYTHONPATH[0]=%s", sys.path[0])
log.info("INTEGRATIONS LIST=%s", os.listdir(os.path.join(os.path.dirname(__file__), "..", "integrations")))

log = logging.getLogger(__name__)



def _load_from_sheet(app):
    # <-- this is the ONLY place we import the integration
    log.info("About to import daily_report using relative import...")
try:
    from ..integrations.google_sheets_attendance import daily_report
    log.info("Imported daily_report RELATIVE from: %s", pathlib.Path(daily_report.__code__.co_filename))
except ImportError as e:
    log.exception("Relative import failed: %s", e)
    from app.integrations.google_sheets_attendance import daily_report
    log.info("Imported daily_report ABSOLUTE from: %s", pathlib.Path(daily_report.__code__.co_filename))

    try:
        from ..integrations.google_sheets_attendance import daily_report
        log.info("Loaded daily_report via relative import.")
    except ImportError:
        from app.integrations.google_sheets_attendance import daily_report
        log.info("Loaded daily_report via absolute import fallback.")
    return daily_report(app)



log = logging.getLogger(__name__)
log.info("INTEGRATIONS CONTENTS: %s",
         os.listdir(os.path.join(os.path.dirname(__file__), "..", "integrations")))
CACHE_DIR = Path(__file__).resolve().parent.parent / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
# Local import inside function to avoid circular dependency
# google_sheets_attendance.py must remain untouched.


CACHE_FILES = {
    "attendance": "attendance.pkl",
    "availability": "availability.pkl",
    "umr": "umr.pkl",
}

CACHE_DIR = Path(__file__).resolve().parent.parent / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "attendance.pkl"
def _cache_path(app, name):
    os.makedirs(app.config["CACHE_DIR"], exist_ok=True)
    return os.path.join(app.config["CACHE_DIR"], CACHE_FILES[name])


def _now_cst(app):
    tz = pytz.timezone(app.config.get("TZ", "America/Chicago"))
    return datetime.now(tz)


def _should_refresh(app, cache_name):
    """Check whether the given cache file is stale."""
    path = _cache_path(app, cache_name)
    if not os.path.exists(path):
        return True
    mtime = datetime.fromtimestamp(os.path.getmtime(path), pytz.timezone(app.config["TZ"]))
    now = _now_cst(app)
    refresh_hour = app.config.get("CACHE_REFRESH_HOUR", 5)
    refresh_minute = app.config.get("CACHE_REFRESH_MINUTE", 0)

    last_refresh = now.replace(hour=refresh_hour, minute=refresh_minute, second=0, microsecond=0)
    if now < last_refresh:
        last_refresh -= timedelta(days=1)

    # refresh if last modification < last_refresh (yesterday's)
    return mtime < last_refresh





def refresh_cache(app, cache_name="attendance"):
    """Force-refresh a specific cache file."""
    try:
        data = _load_from_sheet(app)
        path = _cache_path(app, cache_name)
        with open(path, "wb") as f:
            pickle.dump(data, f)
        app.logger.info("Cache refreshed for %s", cache_name)
        return True
    except Exception as e:
        app.logger.exception("Cache refresh failed for %s: %s", cache_name, e)
        return False


def get_cached_data(app, cache_name="attendance"):
    """Load data from cache (refresh if stale)."""
    if _should_refresh(app, cache_name):
        app.logger.debug("%s cache is stale; refreshing...", cache_name)
        refresh_cache(app, cache_name)

    path = _cache_path(app, cache_name)
    try:
        with open(path, "rb") as f:
            data = pickle.load(f)
        return data
    except Exception as e:
        app.logger.error("Error reading %s cache: %s", cache_name, e)
        return {}


def _scheduler_loop(app):
    """Background scheduler that refreshes cache daily at the configured time."""
    tz = pytz.timezone(app.config.get("TZ", "America/Chicago"))
    while True:
        now = datetime.now(tz)
        refresh_time = now.replace(
            hour=app.config["CACHE_REFRESH_HOUR"],
            minute=app.config["CACHE_REFRESH_MINUTE"],
            second=0,
            microsecond=0,
        )

        # if we've passed today's refresh time, schedule for tomorrow
        if now >= refresh_time:
            refresh_time += timedelta(days=1)

        sleep_seconds = (refresh_time - now).total_seconds()
        app.logger.debug("Next cache refresh in %.2f hours", sleep_seconds / 3600)

        time.sleep(sleep_seconds)
        app.logger.info("Running daily 0500 cache refresh...")
        for name in CACHE_FILES:
            try:
                refresh_cache(app, name)
            except Exception:
                app.logger.exception("Error during scheduled cache refresh for %s", name)


def init_cache_scheduler(app):
    """Start background thread that refreshes caches daily at 0500."""
    thread = threading.Thread(target=_scheduler_loop, args=(app,), daemon=True)
    thread.start()
    app.logger.info("Cache scheduler started (daily 0500 CST refresh).")
