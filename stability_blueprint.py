from flask import Blueprint, render_template, request

from cache_db import ScanCacheDB
from historical_stability import archive_scores, external_stats
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
        rows.append(item)
    rows.sort(key=lambda r: (
        -(r["recent_30d"] if r["recent_30d"] is not None else -1),
        -(r["archive_score"] if r["archive_score"] is not None else -1),
        -(r["availability_rate"] if r["availability_rate"] is not None else -1),
        r["origin"], r["destination"],
    ))
    return rows[:limit]


@bp.get("/stability")
def page():
    db = ScanCacheDB()
    snapshot_latest_run(db)

    # Archive scoring walks the imported historical dataset. Build it once per
    # request and reuse the result for filters as well as the table. The old
    # implementation could score the full archive three times for one page load,
    # which is especially expensive on Termux with a large history database.
    all_rows = _combined_rows(limit=5000)
    origin = (request.args.get("origin") or "").strip()
    destination = (request.args.get("destination") or "").strip()
    rows = all_rows
    if origin:
        rows = [r for r in rows if r["origin"] == origin]
    if destination:
        rows = [r for r in rows if r["destination"] == destination]

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
