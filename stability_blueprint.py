from calendar import month_name

from flask import Blueprint, abort, render_template, request

from cache_db import ScanCacheDB
from historical_stability import route_intelligence
from route_history import snapshot_latest_run, stability_rows
from scan_scope import load_scope
from stability_cache import read_stability_cache, refresh_stability_cache
from trip_recommendations import SEASONS, period_months, recommend_trips

bp = Blueprint("stability", __name__)


def _current_scan_observation(db: ScanCacheDB, origin: str, destination: str):
    run = db.latest_completed_pdf_run()
    if not run:
        return {"covered": False, "positive_dates": 0, "checked_dates": 0, "pdf_run_id": None}
    with db.connect() as conn:
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
    cache = read_stability_cache()
    if cache is None:
        refresh_stability_cache()
        cache = read_stability_cache()
    return cache or {"rows": [], "stats": {}, "external": {}}


def _score(row):
    return row.get("archive_score") if row.get("archive_score") is not None else -1


@bp.get("/stability")
def page():
    db = ScanCacheDB()
    snapshot_latest_run(db)
    cache = _cache()
    all_rows = list(cache["rows"])
    scope = load_scope()
    uk_origins = set(scope.get("origins") or [])
    hubs = set(scope.get("connection_hubs") or [])
    origin = (request.args.get("origin") or "").strip()
    destination = (request.args.get("destination") or "").strip()
    sort = (request.args.get("sort") or "score").strip().lower()
    if sort not in {"score", "uk", "hub"}:
        sort = "score"
    rows = all_rows
    if origin:
        rows = [r for r in rows if r["origin"] == origin]
    if destination:
        rows = [r for r in rows if r["destination"] == destination]
    if sort == "uk":
        rows.sort(key=lambda r: (0 if r.get("origin") in uk_origins else 1, -_score(r), r.get("origin", ""), r.get("destination", "")))
    elif sort == "hub":
        rows.sort(key=lambda r: (0 if r.get("origin") in hubs or r.get("destination") in hubs else 1, -_score(r), r.get("origin", ""), r.get("destination", "")))
    else:
        rows.sort(key=lambda r: (-_score(r), r.get("origin", ""), r.get("destination", "")))

    return render_template(
        "stability.html",
        rows=rows[:500], stats=cache.get("stats", {}), external=cache.get("external", {}),
        origins=sorted({r["origin"] for r in all_rows}), destinations=sorted({r["destination"] for r in all_rows}),
        filters={"origin": origin, "destination": destination, "sort": sort},
        stability_cache_generated_at=cache.get("generated_at"),
    )


@bp.get("/stability/recommendations")
def recommendations_page():
    cache = _cache()
    scope = load_scope()
    mode = (request.args.get("mode") or "season").strip().lower()
    season = (request.args.get("season") or "summer").strip().lower()
    if season not in SEASONS:
        season = "summer"
    try:
        month = max(1, min(12, int(request.args.get("month") or 7))) if mode == "month" else None
    except ValueError:
        month = 7
    months = period_months(month, season)
    trips = recommend_trips(cache["rows"], scope.get("origins") or [], scope.get("connection_hubs") or [], month=month, season=season)
    period_label = month_name[month] if month else season.title()
    return render_template(
        "stability_recommendations.html", trips=trips, mode=mode, month=month or 7,
        season=season, seasons=SEASONS, months=[(i, month_name[i]) for i in range(1, 13)],
        selected_months=months, period_label=period_label, scope=scope,
        stability_cache_generated_at=cache.get("generated_at"),
    )


@bp.get("/stability/route")
def route_page():
    origin = (request.args.get("origin") or "").strip()
    destination = (request.args.get("destination") or "").strip()
    if not origin or not destination:
        abort(400)
    intelligence = route_intelligence(origin, destination)
    if not intelligence:
        abort(404)
    db = ScanCacheDB()
    snapshot_latest_run(db)
    local = next((r for r in stability_rows(limit=2000) if r["origin"] == origin and r["destination"] == destination), None)
    return render_template("stability_route.html", route=intelligence, local=local, current=_current_scan_observation(db, origin, destination), origin=origin, destination=destination)

