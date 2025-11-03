from flask import Blueprint, current_app, render_template

from ..utils.sheet_cache import get_cached_data

bp = Blueprint("oml", __name__, url_prefix="/oml")


def _sort_key(ms: str, entry: dict) -> tuple:
    present = entry.get("present", 0)
    ftr = entry.get("ftr", 0)
    name = entry.get("name", "")
    name_key = (name.split(" ")[-1].lower(), name.split(" ")[0].lower())
    if ms in {"1", "2"}:
        return (-present, name_key)
    return (ftr, -present, name_key)


@bp.route("/")
def oml():
    app = current_app
    app.logger.debug("OML leaderboard requested")
    try:
        data = get_cached_data(app, "attendance")
    except Exception:
        app.logger.exception("Failed to load attendance cache for OML")
        data = {}
    data = get_cached_data(app, "attendance")
    cadets = data.get("cadets", [])

    per_ms = {}
    for cadet in cadets:
        ms_value = str(cadet.get("ms", ""))
        counts = cadet.get("status_counts", {})
        per_ms.setdefault(ms_value, []).append(
            {
                "name": cadet.get("name"),
                "slug": cadet.get("id"),
                "present": counts.get("Present", 0),
                "ftr": counts.get("FTR", 0),
                "excused": counts.get("Excused", 0),
                "school": cadet.get("school"),
            }
        )

    leaderboard = []
    for ms, cadet_list in per_ms.items():
        if not ms:
            continue
        sorted_list = sorted(cadet_list, key=lambda entry: _sort_key(ms, entry))
        leaderboard.append({"ms": ms, "cadets": sorted_list})

    leaderboard.sort(key=lambda item: item["ms"])

    app.logger.debug(
        "OML leaderboard prepared",
        extra={
            "levels": len(leaderboard),
            "total_cadets": len(cadets),
        },
    )

    return render_template("oml.html", leaderboard=leaderboard)
