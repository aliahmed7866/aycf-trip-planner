from flask import Blueprint, abort, render_template, request

from cache_db import ScanCacheDB
from historical_stability import archive_scores, external_stats, route_intelligence
from route_history import history_stats, snapshot_latest_run, stability_rows

bp = Blueprint("stability", __name__)


def _combined_rows(limit: int = 2000):
    local = {(r["origin"], r["destination"]): dict(r) for r in stability_rows(limit=limit)}
    archive = {(r["origin"], r["destination"]): r for r in archive_scores(limit=limit)}
    keys = set(local) | set(archive)
    rows = []
    for key in keys:
        item = local.get(key, {
            "origin": key[0],
            "destination": key[1],
            "observed_scans": 0,
            "positive_checks": 0,
            "total_checks": 0,
            "available_dates": 0,
            "last_seen": None,
            "flight_appearances": 0,
            "availability_rate": None,
        })
        hist = archive.get(key)
        item["archive"] = hist
        item["archive_score"] = hist["archive_score"] if hist else None
        item["recent_30d"] = hist["recent_30d"] if hist else None
        item["trend"] = hist["trend"] if hist else "insufficient"
        rows.append(item)
    rows.sort(key=lambda r: (
        -(r["recent_30d"] if r["recent_30d"] is not None else -1),
        -(r["archive_score"] if r["archive_score"] is not None else -1),
        -(r["availability_rate"] if r["availability_rate"] is not None else -1),
        r["origin"], r["destination"],
    ))
    return rows[:limit]


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


@bp.get("/stability")
def page():
    db = ScanCacheDB()
    snapshot_latest_run(db)
    rows = _combined_rows(limit=2000)
    origin = (request.args.get("origin") or "").strip()
    destination = (request.args.get("destination") or "").strip()
    if origin:
        rows = [r for r in rows if r["origin"] == origin]
    if destination:
        rows = [r for r in rows if r["destination"] == destination]
    all_rows = _combined_rows(limit=5000)
    origins = sorted({r["origin"] for r in all_rows})
    destinations = sorted({r["destination"] for r in all_rows})
    return render_template(
        "stability.html",
        rows=rows[:500],
        stats=history_stats(),
        external=external_stats(),
        origins=origins,
        destinations=destinations,
        filters={"origin": origin, "destination": destination},
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
    local = next((r for r in stability_rows(limit=5000) if r["origin"] == origin and r["destination"] == destination), None)
    current = _current_scan_observation(db, origin, destination)
    return render_template(
        "stability_route.html",
        route=intelligence,
        local=local,
        current=current,
        origin=origin,
        destination=destination,
    )
