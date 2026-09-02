from flask import Blueprint, render_template, request

from cache_db import ScanCacheDB
from route_history import snapshot_latest_run
from stability_cache import read_stability_cache, refresh_stability_cache

bp = Blueprint("stability", __name__)


@bp.get("/stability")
def page():
    db = ScanCacheDB()
    snapshot_latest_run(db)

    cache = read_stability_cache()
    if cache is None:
        # One-time fallback for an existing installation immediately after
        # upgrading. Daily scan runs rebuild this cache automatically thereafter.
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

    origins = sorted({r["origin"] for r in all_rows})
    destinations = sorted({r["destination"] for r in all_rows})
    return render_template(
        "stability.html",
        rows=rows[:500],
        stats=(cache or {}).get("stats", {}),
        external=(cache or {}).get("external", {}),
        origins=origins,
        destinations=destinations,
        filters={"origin": origin, "destination": destination},
        stability_cache_generated_at=(cache or {}).get("generated_at"),
    )
