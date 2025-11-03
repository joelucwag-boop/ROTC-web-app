from flask import Blueprint, current_app, render_template, request, abort

from ..utils.sheet_cache import get_cached_data

bp = Blueprint("directory", __name__, url_prefix="/directory")

STATUS_ORDER = ("Present", "FTR", "Excused")


def _has_availability(cadet: dict, availability_by_name: dict) -> bool:
    key = cadet.get("normalized_name")
    return bool(key and key in availability_by_name)


def _availability_entry(cadet: dict, availability: dict) -> dict | None:
    by_name = availability.get("by_name", {})
    key = cadet.get("normalized_name")
    return by_name.get(key)


@bp.route("/")
def directory():
    app = current_app
    app.logger.debug(
        "Directory listing requested",
        extra={
            "query": request.args.get("q"),
            "ms": request.args.get("ms"),
            "school": request.args.get("school"),
        },
    )
    try:
        attendance = get_cached_data(app, "attendance")
    except Exception:
        app.logger.exception("Failed to load attendance cache for directory")
        attendance = {}
    try:
        availability = get_cached_data(app, "availability")
    except Exception:
        app.logger.exception("Failed to load availability cache for directory")
        availability = {}
    attendance = get_cached_data(app, "attendance")
    availability = get_cached_data(app, "availability")

    cadets_raw = attendance.get("cadets", [])
    ms_levels = attendance.get("ms_levels", [])
    schools = sorted({c.get("school", "") for c in cadets_raw if c.get("school", "")})

    query = request.args.get("q", "").strip().lower()
    ms_filter = request.args.get("ms", "").strip()
    school_filter = request.args.get("school", "").strip()

    filtered = []
    availability_by_name = availability.get("by_name", {})

    for cadet in cadets_raw:
        name = cadet.get("name", "")
        school = cadet.get("school", "")
        ms_value = str(cadet.get("ms", ""))

        if query and query not in name.lower() and query not in school.lower():
            continue
        if ms_filter and ms_value != ms_filter:
            continue
        if school_filter and school.lower() != school_filter.lower():
            continue

        filtered.append(
            {
                "id": cadet["id"],
                "name": name,
                "ms": ms_value,
                "school": school,
                "status_counts": cadet.get("status_counts", {}),
                "has_availability": _has_availability(cadet, availability_by_name),
            }
        )

    filtered.sort(key=lambda item: (item["name"].split(" ")[-1].lower(), item["name"].split(" ")[0].lower()))

    app.logger.debug(
        "Directory filtered",
        extra={
            "total_cadets": len(cadets_raw),
            "filtered": len(filtered),
            "ms_levels": len(ms_levels),
            "schools": len(schools),
        },
    )
    return render_template(
        "directory.html",
        cadets=filtered,
        ms_levels=ms_levels,
        schools=schools,
        query=request.args.get("q", ""),
        ms_filter=ms_filter,
        school_filter=school_filter,
    )


@bp.route("/<cadet_id>")
def cadet_detail(cadet_id: str):
    app = current_app
    app.logger.debug("Directory detail requested", extra={"cadet_id": cadet_id})
    try:
        attendance = get_cached_data(app, "attendance")
    except Exception:
        app.logger.exception("Failed to load attendance cache for cadet detail")
        attendance = {}
    try:
        availability = get_cached_data(app, "availability")
    except Exception:
        app.logger.exception("Failed to load availability cache for cadet detail")
        availability = {}

    cadet = attendance.get("cadet_index", {}).get(cadet_id)
    if not cadet:
        app.logger.warning("Cadet not found in directory detail", extra={"cadet_id": cadet_id})
    attendance = get_cached_data(app, "attendance")
    availability = get_cached_data(app, "availability")

    cadet = attendance.get("cadet_index", {}).get(cadet_id)
    if not cadet:
        abort(404)

    availability_entry = _availability_entry(cadet, availability)

    app.logger.debug(
        "Rendering cadet detail",
        extra={
            "cadet_id": cadet_id,
            "has_availability": bool(availability_entry),
        },
    )

    return render_template(
        "directory_detail.html",
        cadet=cadet,
        availability_entry=availability_entry,
        status_order=STATUS_ORDER,
    )
