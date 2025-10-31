from flask import Blueprint, current_app, render_template, request

from ..utils.sheet_cache import get_cached_data

bp = Blueprint("reports", __name__, url_prefix="/reports")


@bp.route("/")
def reports():
    app = current_app
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

    return render_template(
        "reports.html",
        events=events,
        ms_levels=ms_levels,
        selected_event=selected_event,
        names_by_ms=names_by_ms,
    )
