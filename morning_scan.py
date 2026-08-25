"""Scheduled morning AYCF cache warmer.

Designed to run from Railway Cron or another scheduler shortly after Wizz's
morning AYCF PDF publication. It refreshes the parsed PDF data, detects whether
a new publication appeared, and then checks every advertised route/date in the
current four-day window once, persisting normalized results in SQLite.
"""

import hashlib
import os
from datetime import date

from cache_db import ScanCacheDB
from data_updater import update_data_if_needed
from scanner import CurrentRouteGraph, WizzAYCFClient
from session_vault import SessionVault


def _cache_dir() -> str:
    return os.environ.get("AYCF_CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))


def _snapshot_metadata(graph: CurrentRouteGraph):
    df = graph.latest_frame()
    generated = ""
    if "_generated" in df.columns and df["_generated"].notna().any():
        generated = df["_generated"].max().isoformat()
    elif "data_generated" in df.columns and df["data_generated"].notna().any():
        generated = str(df["data_generated"].iloc[0])
    else:
        generated = date.today().isoformat()

    dep_start = str(df["availability_start"].min()) if "availability_start" in df.columns else None
    dep_end = str(df["availability_end"].max()) if "availability_end" in df.columns else None
    route_pairs = sorted(set(zip(df["departure_from"], df["departure_to"])))
    digest = hashlib.sha256((generated + "\n" + "\n".join(f"{a}>{b}" for a, b in route_pairs)).encode()).hexdigest()[:20]
    return digest, generated, dep_start, dep_end, route_pairs


def run(force: bool = False) -> dict:
    cache_root = _cache_dir()
    upstream_zip = os.environ.get(
        "AYCF_UPSTREAM_ZIP",
        "https://github.com/markvincevarga/wizzair-aycf-availability/archive/refs/heads/main.zip",
    )
    upd = update_data_if_needed(
        cache_root=cache_root,
        upstream_zip_url=upstream_zip,
        refresh_interval_seconds=0,
        force=True,
    )
    graph = CurrentRouteGraph(upd.data_dir)
    db = ScanCacheDB()
    run_id, generated_at, departure_start, departure_end, route_pairs = _snapshot_metadata(graph)
    db.upsert_pdf_run(run_id, generated_at, departure_start, departure_end, len(route_pairs))

    latest = db.latest_pdf_run()
    if latest and latest.get("run_id") == run_id and latest.get("scanned_at") and not force:
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
        df = graph.latest_frame()
        if "availability_start" in df.columns and "availability_end" in df.columns:
            candidate_days = sorted(set(
                d for d in [
                    *[x.date() for x in __import__('pandas').to_datetime(df["availability_start"], errors="coerce", utc=True).dropna()],
                    *[x.date() for x in __import__('pandas').to_datetime(df["availability_end"], errors="coerce", utc=True).dropna()],
                ]
            ))
        else:
            candidate_days = []

        today = date.today()
        days = [today.fromordinal(today.toordinal() + i) for i in range(4)]
        for day in days:
            edges = sorted(graph.edges_for_day(day))
            for origin, destination in edges:
                flights = client.check(origin, destination, day)
                db.replace_route_check(run_id, origin, destination, day, flights)
                route_day_checks += 1
                flights_found += len(flights)

        db.mark_pdf_scanned(run_id)
        db.finish_scan(scan_id, "completed", route_day_checks, client.live_requests, flights_found)
        return {
            "ok": True,
            "skipped": False,
            "pdf_run_id": run_id,
            "generated_at": generated_at,
            "routes": len(route_pairs),
            "route_day_checks": route_day_checks,
            "live_requests": client.live_requests,
            "flights_found": flights_found,
        }
    except Exception as exc:
        db.finish_scan(scan_id, "failed", route_day_checks, client.live_requests, flights_found, str(exc))
        raise


if __name__ == "__main__":
    import json
    print(json.dumps(run(force=os.environ.get("AYCF_FORCE_MORNING_SCAN", "false").lower() == "true"), indent=2))
