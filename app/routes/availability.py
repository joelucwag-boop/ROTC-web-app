import re
import secrets

from flask import (
    Blueprint,
    current_app,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from ..integrations.google_sheets_attendance import DAY_ALIASES
from ..utils.sheet_cache import get_cached_data

bp = Blueprint("availability", __name__, url_prefix="/availability")

SESSION_KEY = "auth_availability"


def _normalize_day(value: str) -> str | None:
    clean = (value or "").strip().lower()
    if not clean:
        return None
    for canonical, aliases in DAY_ALIASES.items():
        if clean == canonical or clean in aliases:
            return canonical
        if canonical.startswith(clean) or any(alias.startswith(clean) for alias in aliases):
            return canonical
    return None


def _tokenise(text: str) -> set[str]:
    cleaned = re.sub(r"[^a-z0-9:]+", " ", (text or "").lower())
    tokens = [p for p in cleaned.split() if p]
    extras = []
    for token in tokens:
        if "-" in token:
            extras.extend(token.split("-"))
        if ":" in token:
            extras.extend(token.split(":"))
    return set(tokens + extras)


@bp.before_request
def _require_password():
    password = (
        current_app.config.get("AVAILABILITY_PASSWORD")
        or current_app.config.get("APP_PASSWORD")
    )
    if not password:
        return None
    if session.get(SESSION_KEY):
        return None
    if request.endpoint == "availability.login":
        return None
    return redirect(url_for("availability.login", next=request.url))


@bp.route("/login", methods=["GET", "POST"])
def login():
    password = (
        current_app.config.get("AVAILABILITY_PASSWORD")
        or current_app.config.get("APP_PASSWORD")
    )
    if not password:
        return redirect(url_for("availability.availability"))

    error = ""
    if request.method == "POST":
        provided = request.form.get("password", "")
        if secrets.compare_digest(provided, password):
            session[SESSION_KEY] = True
            next_url = request.args.get("next") or url_for("availability.availability")
            return redirect(next_url)
        error = "Incorrect password."
    return render_template("password_prompt.html", error=error, title="Availability Checker Access")


def _build_day_options(availability_data: dict) -> list[str]:
    options = set()
    for entry in availability_data.get("entries", []):
        for day, responses in entry.get("days", {}).items():
            if responses:
                options.add(day)
    return sorted(options)


def _build_time_suggestions(availability_data: dict) -> list[str]:
    counts = {}
    for entry in availability_data.get("entries", []):
        for responses in entry.get("days", {}).values():
            for resp in responses:
                for token in resp.get("tokens", []):
                    if token.isdigit() or ":" in token or "am" in token or "pm" in token:
                        counts[token] = counts.get(token, 0) + 1
    common = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return [token for token, _ in common[:10]]


@bp.route("/", methods=["GET", "POST"])
def availability():
    app = current_app
    attendance_data = get_cached_data(app, "attendance")
    availability_data = get_cached_data(app, "availability")

    day_options = _build_day_options(availability_data)
    time_suggestions = _build_time_suggestions(availability_data)

    selected_day = ""
    selected_time = ""
    results = []

    if request.method == "POST":
        selected_day = request.form.get("day", "")
        selected_time = request.form.get("time", "")
        day_key = _normalize_day(selected_day)
        time_tokens = _tokenise(selected_time) if selected_time else set()

        cadets = attendance_data.get("cadets", [])
        availability_by_name = availability_data.get("by_name", {})

        for cadet in cadets:
            avail_entry = availability_by_name.get(cadet.get("normalized_name"))
            responses = []
            if avail_entry and day_key:
                responses = list(avail_entry.get("days", {}).get(day_key, []))
                if time_tokens:
                    responses = [r for r in responses if time_tokens & set(r.get("tokens", []))]

            available_state = None
            if responses:
                if any(resp.get("available") is False for resp in responses):
                    available_state = False
                elif any(resp.get("available") is True for resp in responses):
                    available_state = True

            results.append(
                {
                    "cadet": cadet,
                    "responses": responses,
                    "available": available_state,
                    "has_availability": bool(avail_entry),
                }
            )

        results.sort(
            key=lambda item: (
                item["available"] is not True,
                item["cadet"]["name"].split(" ")[-1].lower(),
                item["cadet"]["name"].split(" ")[0].lower(),
            )
        )

        app.logger.debug(
            "Availability search day=%s time=%s returned %d rows",
            day_key,
            selected_time,
            len(results),
        )

    return render_template(
        "availability.html",
        day_options=day_options,
        time_suggestions=time_suggestions,
        selected_day=selected_day,
        selected_time=selected_time,
        results=results,
    )
