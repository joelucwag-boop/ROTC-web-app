from flask import Blueprint, current_app, render_template, request

from ..utils.sheet_cache import get_cached_data

bp = Blueprint("reports", __name__, url_prefix="/reports")


@bp.route("/")
def reports():
    app = current_app
    app.logger.debug(
        "Reports view requested", extra={"date": request.args.get("date")}
    )
    try:
        data = get_cached_data(app, "attendance")
    except Exception:
        app.logger.exception("Failed to load attendance cache for reports")
        data = {}
    data = get_cached_data(app, "attendance")

    events = list(reversed(data.get("events", [])))  # newest first
    ms_levels = [ms for ms in data.get("ms_levels", []) if ms]
    selected_iso = request.args.get("date")

    selected_event = None
    names_by_ms = {}

    if events:
        if selected_iso:
            selected_event = next((e for e in events if e.get("iso") == selected_iso), None)
        if not selected_event:
            selected_event = events[0]

        names = selected_event.get("names", {})
        for status, cadets in names.items():
            for cadet in cadets:
                ms = str(cadet.get("ms", ""))
                bucket = names_by_ms.setdefault(ms, {"Present": [], "FTR": [], "Excused": []})
                bucket.setdefault(status, []).append(cadet)

    app.logger.debug(
        "Reports data prepared",
        extra={
            "events": len(events),
            "selected_iso": selected_event.get("iso") if selected_event else None,
        },
    )

    return render_template(
        "reports.html",
        events=events,
        ms_levels=ms_levels,
        selected_event=selected_event,
        names_by_ms=names_by_ms,
    )
