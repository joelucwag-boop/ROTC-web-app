# app/routes/home.py
from datetime import datetime

from flask import Blueprint, current_app, render_template

from ..utils.sheet_cache import get_cached_data

URL_PREFIX = "/home"
bp = Blueprint("home", __name__)


def _chart_label(event: dict) -> str:
    label = event.get("header") or event.get("iso") or ""
    event_name = event.get("event") or ""
    if event_name and event_name.lower() not in label.lower():
        return f"{label} ({event_name})"
    return label


@bp.get("/")
def index():
    app = current_app
    app.logger.debug("Rendering dashboard home route")
    try:
        data = get_cached_data(app, "attendance")
    except Exception:
        app.logger.exception("Failed to load attendance cache for dashboard")
        data = {}
    data = get_cached_data(app, "attendance")

    events = data.get("events", [])
    latest_event = data.get("latest_event") or (events[-1] if events else None)
    ms_levels = data.get("ms_levels", [])

    chart_source = events[-30:] if len(events) > 30 else events
    labels = [_chart_label(e) for e in chart_source]
    presents = [e.get("counts", {}).get("Present", 0) for e in chart_source]
    ftrs = [e.get("counts", {}).get("FTR", 0) for e in chart_source]
    excused = [e.get("counts", {}).get("Excused", 0) for e in chart_source]

    generated_at = data.get("generated_at")
    generated_at_str = None
    if generated_at:
        try:
            generated_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            generated_at_str = generated_dt.strftime("%Y-%m-%d %H:%M %Z") if generated_dt.tzinfo else generated_dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            generated_at_str = generated_at

    app.logger.debug(
        "Dashboard data prepared",
        extra={
            "events": len(events),
            "latest_event": latest_event.get("iso") if latest_event else None,
            "ms_levels": len(ms_levels),
        },
    )
    return render_template(
        "home.html",
        latest_event=latest_event,
        ms_levels=ms_levels,
        events=events,
        labels=labels,
        presents=presents,
        ftrs=ftrs,
        excused=excused,
        generated_at=generated_at_str,
    )
