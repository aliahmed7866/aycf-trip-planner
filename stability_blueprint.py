from flask import Blueprint, abort, render_template, request

from cache_db import ScanCacheDB
from historical_stability import route_intelligence
from route_history import snapshot_latest_run, stability_rows
from stability_cache import read_stability_cache, refresh_stability_cache

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


@bp.get("/stability")
def page():
    db = ScanCacheDB()
    snapshot_latest_run(db)
    cache = read_stability_cache()
    if cache is None:
        refresh_stability_cache()
        cache = read_stability_cache()

    all_rows = list(cache["rows"] if cache else [])
    origin = (request.args.get("origin") or "").strip()
    destination = (request.args.get("destination") or "").strip()
    rows = all_rows
    if origin:
        rows = [r for r in rows if r["origin"] == origin]
    if destination:
        rows = [r for r in rows if r["destination"] == destination]

    return render_template(
        "stability.html",
        rows=rows[:500],
        stats=(cache or {}).get("stats", {}),
        external=(cache or {}).get("external", {}),
        origins=sorted({r["origin"] for r in all_rows}),
        destinations=sorted({r["destination"] for r in all_rows}),
        filters={"origin": origin, "destination": destination},
        stability_cache_generated_at=(cache or {}).get("generated_at"),
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
    return render_template(
        "stability_route.html",
        route=intelligence,
        local=local,
        current=_current_scan_observation(db, origin, destination),
        origin=origin,
        destination=destination,
    )
