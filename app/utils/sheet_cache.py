# app/utils/sheet_cache.py
import os
import json
import threading
import time
import pickle
from datetime import datetime, timedelta
from pathlib import Path
import pytz
import logging

log = logging.getLogger(__name__)

# --- cache paths ---
CACHE_DIR = Path(__file__).resolve().parent.parent / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_FILES = {
    "attendance": "attendance.pkl",
    "availability": "availability.pkl",
    "umr": "umr.pkl",
}

# --- helpers ---
def _cache_path(app, name: str) -> str:
    os.makedirs(app.config["CACHE_DIR"], exist_ok=True)
    return os.path.join(app.config["CACHE_DIR"], CACHE_FILES[name])

def _now_cst(app):
    tz = pytz.timezone(app.config.get("TZ", "America/Chicago"))
    return datetime.now(tz)

def _should_refresh(app, cache_name):
    """True if the cache file is missing or older than the last scheduled refresh time."""
    path = _cache_path(app, cache_name)
    if not os.path.exists(path):
        return True

    tz = pytz.timezone(app.config.get("TZ", "America/Chicago"))
    mtime = datetime.fromtimestamp(os.path.getmtime(path), tz)

    now = _now_cst(app)
    refresh_hour = app.config.get("CACHE_REFRESH_HOUR", 5)
    refresh_minute = app.config.get("CACHE_REFRESH_MINUTE", 0)
    last_refresh = now.replace(hour=refresh_hour, minute=refresh_minute, second=0, microsecond=0)
    if now < last_refresh:
        last_refresh -= timedelta(days=1)  # use yesterday's refresh point

    return mtime < last_refresh

# --- integrations loader (lazy & package-safe) ---
def _load_from_sheet(app):
    """
    Import the Google Sheets integration lazily so we avoid circular imports
    and so it works regardless of PYTHONPATH (relative first, absolute fallback).
    """
    try:
        from ..integrations.google_sheets_attendance import daily_report
        log.info("Loaded daily_report via relative import.")
    except ImportError:
        from app.integrations.google_sheets_attendance import daily_report
        log.info("Loaded daily_report via absolute import fallback.")
    return daily_report(app)

# --- cache ops ---
def refresh_cache(app, cache_name="attendance"):
    """Force-refresh a specific cache file."""
    try:
        data = _load_from_sheet(app)  # returns Python object/rows
        path = _cache_path(app, cache_name)
        with open(path, "wb") as f:
            pickle.dump(data, f)
        app.logger.info("Cache refreshed for %s -> %s", cache_name, path)
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
            return pickle.load(f)
    except Exception as e:
        app.logger.error("Error reading %s cache: %s", cache_name, e)
        return {}

# --- simple daily scheduler ---
def _scheduler_loop(app):
    tz = pytz.timezone(app.config.get("TZ", "America/Chicago"))
    while True:
        now = datetime.now(tz)
        refresh_time = now.replace(
            hour=app.config.get("CACHE_REFRESH_HOUR", 5),
            minute=app.config.get("CACHE_REFRESH_MINUTE", 0),
            second=0,
            microsecond=0,
        )
        if now >= refresh_time:
            refresh_time += timedelta(days=1)
        sleep_seconds = (refresh_time - now).total_seconds()
        app.logger.debug("Next cache refresh in %.2f hours", sleep_seconds / 3600)

        time.sleep(max(1, sleep_seconds))
        app.logger.info("Running daily cache refresh...")
        for name in CACHE_FILES:
            try:
                refresh_cache(app, name)
            except Exception:
                app.logger.exception("Error during scheduled cache refresh for %s", name)

def init_cache_scheduler(app):
    """Start background thread that refreshes caches daily."""
    thread = threading.Thread(target=_scheduler_loop, args=(app,), daemon=True)
    thread.start()
    app.logger.info("Cache scheduler started (daily 0500 CST refresh).")
