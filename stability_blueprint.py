from calendar import month_name

import hmac

from flask import Blueprint, abort, flash, redirect, render_template, request, session, url_for

from cache_db import ScanCacheDB
from airport_resolution import CITY_AIRPORTS, archive_name, is_airport_specific, route_archive_fallback
from historical_stability import route_intelligence
from route_history import snapshot_latest_run, stability_rows
from recommendation_preferences import load_preferred_destinations, save_preferred_destinations
from scan_scope import DEFAULT_ORIGINS, load_scope, normalize_name
from stability_cache import refresh_stability_cache, upgrade_stability_cache
from scanner import _STATION_ALIASES
from trip_recommendations import SEASONS, period_months, recommend_trips, recommendation_destinations

bp = Blueprint("stability", __name__)


def _current_scan_observation(db: ScanCacheDB, origin: str, destination: str):
    run = db.latest_completed_pdf_run()
    if not run:
        return {"covered": False, "positive_dates": 0, "checked_dates": 0, "pdf_run_id": None}
    with db.connect() as conn:
        if is_airport_specific(origin) or is_airport_specific(destination):
            row = conn.execute(
                """SELECT COUNT(DISTINCT travel_date) positive_dates
                     FROM route_flights
                    WHERE pdf_run_id=?
                      AND COALESCE(NULLIF(physical_origin,''),origin)=?
                      AND COALESCE(NULLIF(physical_destination,''),destination)=?""",
                (run["run_id"], origin, destination),
            ).fetchone()
            positive = int(row["positive_dates"] or 0)
            return {"covered": positive > 0, "positive_dates": positive, "checked_dates": positive, "pdf_run_id": run["run_id"], "scanned_at": run.get("scanned_at"), "airport_specific": True}
        row = conn.execute(
            """SELECT COUNT(*) checked_dates,
                      SUM(CASE WHEN flight_count>0 THEN 1 ELSE 0 END) positive_dates
               FROM route_checks WHERE pdf_run_id=? AND origin=? AND destination=?""",
            (run["run_id"], origin, destination),
        ).fetchone()
    checked = int(row["checked_dates"] or 0)
    return {
        "covered": checked > 0,
        "positive_dates": int(row["positive_dates"] or 0),
        "checked_dates": checked,
        "pdf_run_id": run["run_id"],
        "scanned_at": run.get("scanned_at"),
    }


def _cache():
    cache = upgrade_stability_cache()
    if cache is None:
        refresh_stability_cache()
        cache = upgrade_stability_cache()
    return cache or {"rows": [], "stats": {}, "external": {}}


def _score(row):
    return row.get("archive_score") if row.get("archive_score") is not None else -1


def _place_matches(actual, selected):
    if not selected:
        return True
    if selected == "London":
        return actual == "London" or actual in CITY_AIRPORTS["London"]
    return actual == selected


def _csrf_ok() -> bool:
    expected = session.get("csrf_token", "")
    supplied = request.form.get("csrf_token", "") or request.headers.get("X-CSRF-Token", "")
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


def _airport_code(value: str) -> str:
    return _STATION_ALIASES.get(str(value or "").strip().casefold(), "")


def _route_matches_search(row, query: str) -> bool:
    terms = normalize_name(query).split()
    if not terms:
        return True
    values = [
        row.get("origin", ""),
        row.get("destination", ""),
        _airport_code(row.get("origin", "")),
        _airport_code(row.get("destination", "")),
    ]
    haystack = normalize_name(" ".join(str(value or "") for value in values))
    return all(term in haystack for term in terms)


def _destination_preferred(destination: str, preferred: set[str]) -> bool:
    if destination in preferred:
        return True
    return destination == "London" and any(archive_name(item) == "London" for item in preferred)


@bp.get("/stability")
def page():
    db = ScanCacheDB()
    snapshot_latest_run(db)
    cache = _cache()
    all_rows = list(cache["rows"])
    scope = load_scope()
    uk_origins = set(scope.get("origins") or [])
    hubs = set(scope.get("connection_hubs") or [])
    preferred_destinations = set(load_preferred_destinations())
    origin = (request.args.get("origin") or "").strip()
    destination = (request.args.get("destination") or "").strip()
    query = (request.args.get("q") or "").strip()[:100]
    sort = (request.args.get("sort") or "score").strip().lower()
    if sort not in {"score", "uk", "hub"}:
        sort = "score"
    rows = [dict(row) for row in all_rows]
    for row in rows:
        row["preferred_destination"] = _destination_preferred(
            row.get("destination", ""), preferred_destinations
        )
    if origin:
        rows = [r for r in rows if _place_matches(r["origin"], origin)]
    if destination:
        rows = [r for r in rows if _place_matches(r["destination"], destination)]
    if query:
        rows = [r for r in rows if _route_matches_search(r, query)]
    preferred_key = lambda row: 0 if row.get("preferred_destination") else 1
    if sort == "uk":
        rows.sort(key=lambda r: (preferred_key(r), 0 if r.get("origin") in uk_origins or archive_name(r.get("origin", "")) == "London" else 1, -_score(r), r.get("origin", ""), r.get("destination", "")))
    elif sort == "hub":
        rows.sort(key=lambda r: (preferred_key(r), 0 if r.get("origin") in hubs or r.get("destination") in hubs else 1, -_score(r), r.get("origin", ""), r.get("destination", "")))
    else:
        rows.sort(key=lambda r: (preferred_key(r), -_score(r), r.get("origin", ""), r.get("destination", "")))

    return render_template(
        "stability.html",
        rows=rows[:500], stats=cache.get("stats", {}), external=cache.get("external", {}),
        origins=sorted({r["origin"] for r in all_rows} | set(scope.get("origins") or []) | {"London"}),
        destinations=sorted({r["destination"] for r in all_rows} | set(CITY_AIRPORTS["London"]) | {"London"}),
        filters={"origin": origin, "destination": destination, "sort": sort, "q": query},
        preferred_destinations=sorted(preferred_destinations),
        stability_cache_generated_at=cache.get("generated_at"),
    )


@bp.route("/stability/recommendations", methods=["GET", "POST"])
def recommendations_page():
    cache = _cache()
    scope = load_scope()
    known_uk = {normalize_name(item) for item in DEFAULT_ORIGINS}
    uk_origins = [
        item for item in (scope.get("origins") or [])
        if normalize_name(item) in known_uk
    ]
    values = request.form if request.method == "POST" else request.args
    mode = (values.get("mode") or "season").strip().lower()
    season = (values.get("season") or "summer").strip().lower()
    if season not in SEASONS:
        season = "summer"
    try:
        month = max(1, min(12, int(values.get("month") or 7))) if mode == "month" else None
    except ValueError:
        month = 7
    months = period_months(month, season)
    origin = (values.get("origin") or "").strip()
    if origin not in set(uk_origins):
        origin = ""
    trip_type = (values.get("trip_type") or "all").strip().lower()
    if trip_type not in {"all", "direct", "connected"}:
        trip_type = "all"
    destination_options = recommendation_destinations(
        cache["rows"], uk_origins, scope.get("connection_hubs") or []
    )
    allowed_destinations = set(destination_options)
    if request.method == "POST":
        if not _csrf_ok():
            flash("Your preferences form expired. Please try again.", "warning")
            return redirect(url_for("stability.recommendations_page"))
        selected_destinations = []
        for value in request.form.getlist("destination"):
            destination = str(value or "").strip()
            if destination in allowed_destinations and destination not in selected_destinations:
                selected_destinations.append(destination)
        save_preferred_destinations(selected_destinations)
        flash("Preferred recommendation destinations saved.", "success")
        return redirect(url_for(
            "stability.recommendations_page", mode=mode, season=season,
            month=month or 7, origin=origin, trip_type=trip_type,
        ))
    selected_destinations = [
        destination for destination in load_preferred_destinations()
        if destination in allowed_destinations
    ]
    trips = recommend_trips(
        cache["rows"], uk_origins, scope.get("connection_hubs") or [],
        month=month, season=season,
        destination_mode="only" if selected_destinations else "all",
        destinations=selected_destinations, origin_filter=origin, trip_type=trip_type,
    )
    period_label = month_name[month] if month else season.title()
    return render_template(
        "stability_recommendations.html", trips=trips, mode=mode, month=month or 7,
        season=season, seasons=SEASONS, months=[(i, month_name[i]) for i in range(1, 13)],
        selected_months=months, period_label=period_label, scope=scope, uk_origins=uk_origins,
        origin=origin, trip_type=trip_type, destination_options=destination_options,
        selected_destinations=selected_destinations,
        direct_count=sum(1 for trip in trips if trip["is_direct"]),
        connected_count=sum(1 for trip in trips if not trip["is_direct"]),
        stability_cache_generated_at=cache.get("generated_at"),
    )


@bp.get("/stability/route")
def route_page():
    origin = (request.args.get("origin") or "").strip()
    destination = (request.args.get("destination") or "").strip()
    if not origin or not destination:
        abort(400)
    intelligence = route_intelligence(origin, destination)
    historical_scope = None
    archive_origin, archive_destination = origin, destination
    if not intelligence:
        fallback = route_archive_fallback(origin, destination)
        if fallback:
            archive_origin, archive_destination = fallback
            intelligence = route_intelligence(*fallback)
            historical_scope = "London-wide"
    if not intelligence:
        abort(404)
    db = ScanCacheDB()
    snapshot_latest_run(db)
    local = next((r for r in stability_rows(limit=2000) if r["origin"] == origin and r["destination"] == destination), None)
    return render_template("stability_route.html", route=intelligence, local=local, current=_current_scan_observation(db, origin, destination), origin=origin, destination=destination, historical_scope=historical_scope, archive_origin=archive_origin, archive_destination=archive_destination)
