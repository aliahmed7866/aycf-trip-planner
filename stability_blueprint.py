from flask import Blueprint, render_template, request

from cache_db import ScanCacheDB
from route_history import history_stats, snapshot_latest_run, stability_rows

bp = Blueprint("stability", __name__)


@bp.get("/stability")
def page():
    db = ScanCacheDB()
    snapshot_latest_run(db)
    rows = stability_rows(limit=500)
    origin = (request.args.get("origin") or "").strip()
    destination = (request.args.get("destination") or "").strip()
    if origin:
        rows = [r for r in rows if r["origin"] == origin]
    if destination:
        rows = [r for r in rows if r["destination"] == destination]
    origins = sorted({r["origin"] for r in stability_rows(limit=2000)})
    destinations = sorted({r["destination"] for r in stability_rows(limit=2000)})
    return render_template(
        "stability.html",
        rows=rows,
        stats=history_stats(),
        origins=origins,
        destinations=destinations,
        filters={"origin": origin, "destination": destination},
    )
