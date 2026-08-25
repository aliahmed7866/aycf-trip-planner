"""Scheduled morning AYCF cache warmer.

Run this repeatedly around Wizz's morning publication window. It downloads the
official PDF directly, skips work if that publication timestamp was already
scanned, then checks every advertised route for each day in the PDF's 4-day
window and persists positive and zero-flight results in SQLite.
"""

import hashlib
import os
from datetime import date, timedelta

from cache_db import ScanCacheDB
from direct_pdf import refresh_direct_snapshot
from scanner import CurrentRouteGraph, WizzAYCFClient
from session_vault import SessionVault


def _cache_dir() -> str:
    return os.environ.get("AYCF_CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))


def run(force: bool = False) -> dict:
    cache_root = _cache_dir()
    data_dir, df, generated, departure_start, departure_end = refresh_direct_snapshot(
        cache_root,
        os.environ.get("AYCF_PDF_URL", "https://multipass.wizzair.com/aycf-availability.pdf"),
    )
    graph = CurrentRouteGraph(data_dir)
    route_pairs = sorted(set(zip(df["departure_from"], df["departure_to"])))
    run_id = hashlib.sha256((generated.isoformat() + "\n" + "\n".join(f"{a}>{b}" for a, b in route_pairs)).encode()).hexdigest()[:20]

    db = ScanCacheDB()
    db.upsert_pdf_run(run_id, generated.isoformat(), departure_start.isoformat(), departure_end.isoformat(), len(route_pairs))
    current = db.latest_pdf_run()
    if current and current.get("run_id") == run_id and current.get("scanned_at") and not force:
        return {"ok": True, "skipped": True, "reason": "PDF publication already scanned", "pdf_run_id": run_id}

    state = SessionVault().load()
    if not state:
        raise RuntimeError("No saved Wizz session. Run login_wizz.py before the scheduled scan.")

    client = WizzAYCFClient(
        state,
        cache_ttl=int(os.environ.get("AYCF_LIVE_CACHE_SECONDS", "300")),
        min_delay=float(os.environ.get("AYCF_MIN_REQUEST_DELAY", "1.0")),
    )
    client.bootstrap()
    scan_id = db.start_scan(run_id)
    route_day_checks = 0
    flights_found = 0
    try:
        start_day = departure_start.date()
        end_day = departure_end.date()
        day = start_day
        while day <= end_day:
            for origin, destination in sorted(graph.edges_for_day(day)):
                flights = client.check(origin, destination, day)
                db.replace_route_check(run_id, origin, destination, day, flights)
                route_day_checks += 1
                flights_found += len(flights)
            day += timedelta(days=1)

        db.mark_pdf_scanned(run_id)
        db.finish_scan(scan_id, "completed", route_day_checks, client.live_requests, flights_found)
        return {"ok": True, "skipped": False, "pdf_run_id": run_id, "generated_at": generated.isoformat(), "routes": len(route_pairs), "route_day_checks": route_day_checks, "live_requests": client.live_requests, "flights_found": flights_found}
    except Exception as exc:
        db.finish_scan(scan_id, "failed", route_day_checks, client.live_requests, flights_found, str(exc))
        raise


if __name__ == "__main__":
    import json
    print(json.dumps(run(force=os.environ.get("AYCF_FORCE_MORNING_SCAN", "false").lower() == "true"), indent=2))
