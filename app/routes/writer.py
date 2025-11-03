# app/routes/writer.py
import copy
import secrets
from datetime import date

from flask import (
    Blueprint,
    current_app,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from ..integrations.google_sheets_attendance import write_attendance_entries
from ..utils.sheet_cache import get_cached_data, refresh_cache

URL_PREFIX = "/writer"
bp = Blueprint("writer", __name__)

SESSION_KEY = "auth_writer"
STATUS_OPTIONS = ["Present", "FTR", "Excused"]
EVENT_PRESETS = ["PT", "Lab"]


def _writer_password():
    return (
        current_app.config.get("WRITER_PASSWORD")
        or current_app.config.get("ADMIN_PASSWORD")
        or current_app.config.get("APP_PASSWORD")
    )


@bp.before_request
def _require_password():
    password = _writer_password()
    if not password:
        return None
    if session.get(SESSION_KEY):
        return None
    if request.endpoint == "writer.login":
        return None
    current_app.logger.debug(
        "Redirecting to writer login", extra={"next": request.url}
    )
    return redirect(url_for("writer.login", next=request.url))


@bp.route("/login", methods=["GET", "POST"])
def login():
    password = _writer_password()
    if not password:
        return redirect(url_for("writer.index"))

    error = ""
    if request.method == "POST":
        provided = request.form.get("password", "")
        if secrets.compare_digest(provided, password):
            session[SESSION_KEY] = True
            current_app.logger.info(
                "Writer login succeeded", extra={"remote_addr": request.remote_addr}
            )
            return redirect(request.args.get("next") or url_for("writer.index"))
        error = "Incorrect password."
        current_app.logger.warning(
            "Writer login failed", extra={"remote_addr": request.remote_addr}
        )

    return render_template("password_prompt.html", title="Attendance Writer", error=error)


def _group_cadets(cadets: list[dict]) -> dict[str, list[dict]]:
    groups = {"GSU": [], "ULM": [], "Both": []}
    for cadet in cadets:
        school = (cadet.get("school") or "").upper()
        if "ULM" in school:
            groups.setdefault("ULM", []).append(cadet)
        else:
            groups.setdefault("GSU", []).append(cadet)
        groups["Both"].append(cadet)
    for key in groups:
        groups[key].sort(key=lambda c: (c.get("name", "").split(" ")[-1].lower(), c.get("name", "").split(" ")[0].lower()))
    return groups


@bp.route("/", methods=["GET", "POST"])
def index():
    app = current_app
    app.logger.debug("Writer interface accessed", extra={"method": request.method})
    try:
        attendance_data = get_cached_data(app, "attendance")
    except Exception:
        app.logger.exception("Failed to load attendance cache for writer")
        attendance_data = {}
    cadets = attendance_data.get("cadets", [])
    cadet_map = {c["id"]: c for c in cadets}
    groups = _group_cadets(cadets)

    selected_group = request.values.get("school", "GSU")
    selected_cadets = groups.get(selected_group, groups.get("GSU", []))

    message = ""
    status_summary = {}

    if request.method == "POST":
        if not app.config.get("ENABLE_WRITES", False):
            message = "Writes are disabled. Set ENABLE_WRITES=true to allow updates."
            app.logger.warning("Writer submission blocked because writes are disabled.")
        else:
            target_date = request.form.get("date") or date.today().isoformat()
            preset = request.form.get("event_preset", "")
            custom_event = request.form.get("event_custom", "").strip()
            event_label = custom_event or (preset if preset not in {"Custom", ""} else "")

            updates = []
            for cadet_id in cadet_map:
                status_key = f"status[{cadet_id}]"
                note_key = f"note[{cadet_id}]"
                status = request.form.get(status_key, "").strip()
                if status not in STATUS_OPTIONS:
                    continue
                note = request.form.get(note_key, "").strip()
                cadet_info = cadet_map.get(cadet_id)
                if not cadet_info:
                    continue
                updates.append(
                    {
                        "sheet_row": cadet_info.get("sheet_row"),
                        "status": status,
                        "note": note,
                        "cadet": cadet_info,
                    }
                )
                status_summary.setdefault(status, 0)
                status_summary[status] += 1

            if not updates:
                message = "No cadets were selected for update."
                app.logger.info("Writer submission ignored: no cadets selected.")
            else:
                sheet_id = app.config.get("SPREADSHEET_ID")
                tab_name = app.config.get("WORKSHEET_NAME", "Attendance Roster")
                if not sheet_id:
                    message = "SPREADSHEET_ID is not configured."
                    app.logger.error("Writer submission failed: SPREADSHEET_ID missing.")
                else:
                    try:
                        result = write_attendance_entries(
                            sheet_id=sheet_id,
                            tab_name=tab_name,
                            header_row=attendance_data.get("header_row", 1),
                            date_columns=copy.deepcopy(attendance_data.get("date_columns", [])),
                            last_column_index=attendance_data.get("last_column_index", 1),
                            target_iso=target_date,
                            event_label=event_label,
                            updates=updates,
                            color_present=app.config.get("ATT_COLOR_PRESENT", "#00ff00"),
                            color_ftr=app.config.get("ATT_COLOR_FTR", "#ff0000"),
                            color_excused=app.config.get("ATT_COLOR_EXCUSED", "#ffff00"),
                        )
                        app.logger.info(
                            "Writer submission applied",
                            extra={
                                "target_date": target_date,
                                "event_label": event_label,
                                "updated": result.get("updated"),
                            },
                        )
                        refresh_cache(app, "attendance")
                        message = f"Updated {result.get('updated', 0)} cells for {target_date}."
                    except Exception:
                        app.logger.exception(
                            "Writer submission failed",
                            extra={"target_date": target_date, "event_label": event_label},
                        )
                        message = "Failed to write attendance. Check logs for details."

            if status_summary:
                status_summary = {
                    status: status_summary[status]
                    for status in STATUS_OPTIONS
                    if status_summary.get(status)
                }

    app.logger.debug(
        "Writer page prepared",
        extra={
            "selected_group": selected_group,
            "cadet_count": len(selected_cadets),
            "message": message,
        },
    )
    return render_template(
        "writer.html",
        cadets=selected_cadets,
        all_groups=groups,
        selected_group=selected_group,
        today=date.today().isoformat(),
        message=message,
        status_options=STATUS_OPTIONS,
        event_presets=EVENT_PRESETS + ["Custom"],
        writes_enabled=app.config.get("ENABLE_WRITES", False),
        status_summary=status_summary,
    )
