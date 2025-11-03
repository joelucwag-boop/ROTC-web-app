"""Centralised caching for Google Sheet integrations.

This module keeps a very small surface area: every cacheable dataset
is backed by a loader function that knows how to talk to Google Sheets
or other remote sources.  The cache on disk is refreshed once per day
at the configured time (defaults to 0500 America/Chicago) and may also
be refreshed manually through the admin route.

Each cache entry is a Python dict composed exclusively of basic data
types so it can be safely pickled and used across workers.
"""

from __future__ import annotations

import logging
import os
import pickle
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Any

import pytz

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths / registry
# ---------------------------------------------------------------------------

CACHE_DIR = Path(__file__).resolve().parent.parent / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_FILES = {
    "attendance": "attendance.pkl",
    "availability": "availability.pkl",
    "umr": "umr.pkl",
}


def _cache_path(app, name: str) -> str:
    cache_dir = app.config.get("CACHE_DIR", str(CACHE_DIR))
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, CACHE_FILES[name])


def _get_timezone(app):
    tz_name = app.config.get("TZ", "America/Chicago")
    try:
        return pytz.timezone(tz_name)
    except Exception:
        log.warning("Invalid timezone '%s'; defaulting to UTC", tz_name)
        return pytz.UTC


def _now_tz(app) -> datetime:
    tz = _get_timezone(app)


def _now_tz(app) -> datetime:
    tz_name = app.config.get("TZ", "America/Chicago")
    tz = pytz.timezone(tz_name)
    return datetime.now(tz)


def _should_refresh(app, cache_name: str) -> bool:
    path = _cache_path(app, cache_name)
    if not os.path.exists(path):
        return True

    tz = _get_timezone(app)
    try:
        mtime = datetime.fromtimestamp(os.path.getmtime(path), tz)
    except Exception:
        log.exception("Failed to interpret mtime for cache '%s'", cache_name)
        return True

    now = _now_tz(app)
    refresh_hour = app.config.get("CACHE_REFRESH_HOUR", 5)
    refresh_minute = app.config.get("CACHE_REFRESH_MINUTE", 0)
    last_refresh = now.replace(hour=refresh_hour, minute=refresh_minute, second=0, microsecond=0)
    if now < last_refresh:
        last_refresh -= timedelta(days=1)

    return mtime < last_refresh


# ---------------------------------------------------------------------------
# Loader registry
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# Loader registry
# ---------------------------------------------------------------------------


def _load_attendance(app) -> Dict[str, Any]:
    from ..integrations.google_sheets_attendance import build_attendance_cache

    sheet_id = app.config.get("SPREADSHEET_ID")
    tab_name = app.config.get("WORKSHEET_NAME", "Attendance Roster")
    if not sheet_id:
        raise RuntimeError("SPREADSHEET_ID is not configured.")

    return build_attendance_cache(
        sheet_id=sheet_id,
        tab_name=tab_name,
        program_hint=app.config.get("PROGRAM_COLUMN", ""),
    )


def _load_availability(app) -> Dict[str, Any]:
    from ..integrations.google_sheets_attendance import build_availability_cache

    url = app.config.get("AVAILABILITY_CSV_URL", "")
    if not url:
        log.info("Availability CSV URL not configured; returning empty dataset.")
        return {"generated_at": _now_tz(app).isoformat(), "entries": [], "index": {}}

    return build_availability_cache(
        csv_url=url,
        name_column_override=app.config.get("AVAILABILITY_NAME_COLUMN", ""),
    )



def _load_availability(app) -> Dict[str, Any]:
    from ..integrations.google_sheets_attendance import build_availability_cache

    url = app.config.get("AVAILABILITY_CSV_URL", "")
    if not url:
        log.info("Availability CSV URL not configured; returning empty dataset.")
        return {"generated_at": _now_tz(app).isoformat(), "entries": [], "index": {}}

    return build_availability_cache(
        csv_url=url,
        name_column_override=app.config.get("AVAILABILITY_NAME_COLUMN", ""),
    )


def _load_umr(app) -> Dict[str, Any]:
    from ..integrations.google_sheets_attendance import build_umr_cache

    sheet_id = app.config.get("SPREADSHEET_ID")
    tab_name = app.config.get("UMR_TAB_NAME", "UMR")
    if not sheet_id:
        raise RuntimeError("SPREADSHEET_ID is not configured.")

    return build_umr_cache(
        sheet_id=sheet_id,
        tab_name=tab_name,
        mapping_json=app.config.get("UMR_MAPPING_JSON", ""),
    )


CACHE_LOADERS: Dict[str, Callable] = {
    "attendance": _load_attendance,
    "availability": _load_availability,
    "umr": _load_umr,
}


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def refresh_cache(app, cache_name: str = "attendance") -> bool:
    """Force refresh for a specific cache file."""

    if cache_name not in CACHE_LOADERS:
        raise KeyError(f"Unknown cache: {cache_name}")

    loader = CACHE_LOADERS[cache_name]

    try:
        data = loader(app)
        path = _cache_path(app, cache_name)
        with open(path, "wb") as fh:
            pickle.dump(data, fh)
        app.logger.info("Cache refreshed for %s -> %s", cache_name, path)
        return True
    except Exception:
        app.logger.exception("Cache refresh failed for %s", cache_name)
        return False


def get_cached_data(app, cache_name: str = "attendance") -> Dict[str, Any]:
    if cache_name not in CACHE_LOADERS:
        raise KeyError(f"Unknown cache: {cache_name}")

    if _should_refresh(app, cache_name):
        app.logger.debug("Cache '%s' is stale; refreshing.", cache_name)
        if not refresh_cache(app, cache_name):
            app.logger.error(
                "Cache '%s' refresh failed; returning empty dataset.", cache_name
            )
            return {}
        refresh_cache(app, cache_name)

    path = _cache_path(app, cache_name)
    try:
        with open(path, "rb") as fh:
            return pickle.load(fh)
    except FileNotFoundError:
        app.logger.warning("Cache %s missing on disk; regenerating.", cache_name)
        if not refresh_cache(app, cache_name):
            app.logger.error(
                "Cache '%s' could not be created; returning empty dataset.", cache_name
            )
            return {}
        try:
            with open(path, "rb") as fh:
                return pickle.load(fh)
        except FileNotFoundError:
            app.logger.error(
                "Cache '%s' still missing after refresh; returning empty dataset.",
                cache_name,
            )
            return {}
        refresh_cache(app, cache_name)
        with open(path, "rb") as fh:
            return pickle.load(fh)
    except Exception:
        app.logger.exception("Error reading cache %s; returning empty dict", cache_name)
        return {}


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


def _scheduler_loop(app):
    tz = _get_timezone(app)
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

        sleep_seconds = max(1, int((refresh_time - now).total_seconds()))
        app.logger.debug(
            "Next cache refresh scheduled for %s (in %.2f hours)",
            refresh_time.isoformat(),
            sleep_seconds / 3600,
        )

        time.sleep(sleep_seconds)

        for cache_name in CACHE_LOADERS:
            try:
                app.logger.info("Running scheduled refresh for %s", cache_name)
                refresh_cache(app, cache_name)
            except Exception:
                app.logger.exception("Scheduled cache refresh failed for %s", cache_name)


def init_cache_scheduler(app):
    thread = threading.Thread(target=_scheduler_loop, args=(app,), daemon=True)
    thread.start()
    app.logger.info("Cache scheduler started (daily refresh).")

