"""Priority morning scan: selected UK origins first, then selected reachable hubs."""

import hashlib
import os
import time

from cache_db import ScanCacheDB
from direct_pdf import refresh_direct_snapshot
from morning_scan import CapturedRequestWizzClient, _apply_wizz_runtime, _cache_dir, _mirror_for_web, _scan_days
from parallel_fetch import ParallelFetcher
from scan_scope import airport_variants, load_scope, scan_plan, scope_fingerprint, scope_summary
from session_vault import SessionVault
from station_resolver import prepare_required_stations


def _bounded_int(name: str, default: int, low: int, high: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return max(low, min(high, value))


def run(force: bool = False) -> dict:
    cache_root = _cache_dir()
    _, df, generated, departure_start, departure_end = refresh_direct_snapshot(cache_root, os.environ.get("AYCF_PDF_URL", "https://multipass.wizzair.com/aycf-availability.pdf"))
    _mirror_for_web(cache_root, df, generated)
    all_route_pairs = sorted(set(zip(df["departure_from"], df["departure_to"])))
    scope = load_scope()
    days = list(_scan_days(departure_start, departure_end))
    workers = _bounded_int("AYCF_SCAN_WORKERS", int(scope.get("workers", 3) or 3), 1, 5)
    start_interval = max(0.2, float(os.environ.get("AYCF_GLOBAL_REQUEST_INTERVAL", "1.0")))
    plan = scan_plan(all_route_pairs, scope, days=len(days), seconds_per_request=float(os.environ.get("AYCF_SCAN_SECONDS_PER_CHECK", "1.25")))
    primary_routes, hub_routes = plan["primary_routes"], plan["hub_routes"]
    route_entries = [("primary", a, b) for a, b in primary_routes] + [("hub", a, b) for a, b in hub_routes]
    if not route_entries:
        raise RuntimeError("Your scan scope matches no routes in the current AYCF PDF. Adjust Morning scan scope in the app.")

    scope_id = scope_fingerprint(scope)
    route_pairs = [(a, b) for _, a, b in route_entries]
    run_id = hashlib.sha256((generated.isoformat() + "\n" + scope_id + "\n" + "\n".join(f"{a}>{b}" for a, b in route_pairs)).encode()).hexdigest()[:20]

    station_names = set()
    for _, origin, destination in route_entries:
        station_names.update(airport_variants(origin, scope))
        station_names.update(airport_variants(destination, scope))

    db = ScanCacheDB()
    db.upsert_pdf_run(run_id, generated.isoformat(), departure_start.isoformat(), departure_end.isoformat(), len(route_pairs), scope_id=scope_id, scope=scope)
    current = db.get_pdf_run(run_id)
    if current and current.get("scanned_at") and not force:
        return {"ok": True, "skipped": True, "reason": "Current PDF and scan scope already scanned", "pdf_run_id": run_id, "scope_id": scope_id}
    if db.scan_in_progress(run_id) and not force:
        return {"ok": True, "skipped": True, "reason": "A scan for this PDF and scope is already running", "pdf_run_id": run_id, "scope_id": scope_id}

    state = SessionVault().load()
    if not state:
        raise RuntimeError("No saved Wizz session. Import a Wizz session before the scheduled scan.")
    coordinator = CapturedRequestWizzClient(state, cache_ttl=int(os.environ.get("AYCF_LIVE_CACHE_SECONDS", "300")), min_delay=0.2)
    if not _apply_wizz_runtime(coordinator):
        coordinator.bootstrap()
    station_report = prepare_required_stations(coordinator, sorted(station_names))
    print(f"[AYCF] PDF {generated.isoformat()} | scope {scope_id} | priority {len(primary_routes)} routes + hubs {len(hub_routes)} routes | {plan['checks']} checks | workers {workers} | global start interval {start_interval:.2f}s | stations {station_report['resolved']}/{station_report['required']} resolved", flush=True)
    print(f"[AYCF] Scope: {scope_summary(scope)}", flush=True)
    if station_report["unresolved"]:
        raise RuntimeError("Station preflight failed before live scanning. Unresolved scoped stations: " + ", ".join(station_report["unresolved"]))
    print("[AYCF] Station preflight OK for selected scope.", flush=True)
    preflight = coordinator.preflight()
    print(f"[AYCF] Captured-request preflight OK ({preflight.get('response')})." if preflight.get("ok") else f"[AYCF] Preflight skipped: {preflight.get('reason')}", flush=True)
    resolved_station_ids = dict(coordinator.station_ids)

    def client_factory():
        client = CapturedRequestWizzClient(state, cache_ttl=int(os.environ.get("AYCF_LIVE_CACHE_SECONDS", "300")), min_delay=0.2)
        if not _apply_wizz_runtime(client):
            client.dynamic_url = coordinator.dynamic_url
            client.captured_request_method = coordinator.captured_request_method
            client.captured_template_type = coordinator.captured_template_type
            client.captured_request_template = coordinator.captured_request_template
        client.station_ids.update(resolved_station_ids)
        return client

    fetcher = ParallelFetcher(client_factory, workers=workers, start_interval=start_interval)
    total_checks = len(route_entries) * len(days)
    progress_every = max(1, int(os.environ.get("AYCF_PROGRESS_EVERY", "10")))
    scan_id = db.start_scan(run_id)
    stats = {"route_day_checks": 0, "flights_found": 0, "resumed_flights": 0, "resumed": 0, "processed": 0, "live_requests": coordinator.live_requests, "no_availability": coordinator.no_availability_responses, "wallet_redirects": coordinator.wallet_redirects, "html_retries": coordinator.html_retries}
    started = time.time()

    def make_jobs(tier, routes):
        jobs = []
        for origin, destination in routes:
            origin_choices = airport_variants(origin, scope)
            destination_choices = airport_variants(destination, scope)
            for day in days:
                cached_count = db.route_flight_count(run_id, origin, destination, day)
                if cached_count is not None and not force:
                    stats["resumed"] += 1; stats["resumed_flights"] += cached_count; stats["flights_found"] += cached_count; stats["processed"] += 1
                    continue
                jobs.append((tier, origin, destination, day, origin_choices, destination_choices))
        return jobs

    def on_result(result):
        db.replace_route_check(run_id, result["origin"], result["destination"], result["day"], result["flights"])
        stats["route_day_checks"] += 1; stats["flights_found"] += len(result["flights"]); stats["processed"] += 1
        stats["live_requests"] += result["live_requests"]; stats["no_availability"] += result["no_availability"]; stats["wallet_redirects"] += result["wallet_redirects"]; stats["html_retries"] += result["html_retries"]
        if stats["processed"] == 1 or stats["processed"] % progress_every == 0 or stats["processed"] == total_checks:
            elapsed = max(1.0, time.time() - started); rate = stats["route_day_checks"] / elapsed if stats["route_day_checks"] else 0.0
            route_label = f"{'/'.join(result['origin_variants'])} -> {'/'.join(result['destination_variants'])}"
            print(f"[AYCF] {stats['processed']}/{total_checks} | {result['tier']} | {route_label} {result['day']} | live {stats['route_day_checks']} | resumed {stats['resumed']} | flights {stats['flights_found']} (cached {stats['resumed_flights']}) | requests {stats['live_requests']} | no-availability {stats['no_availability']} | {rate:.2f} checks/s", flush=True)

    try:
        primary_jobs = make_jobs("primary", primary_routes)
        if primary_jobs:
            print(f"[AYCF] Starting priority tier with {len(primary_jobs)} pending checks across {workers} workers; resumed {stats['resumed']} checks with {stats['resumed_flights']} cached flights.", flush=True)
            fetcher.run(primary_jobs, on_result)
        hub_jobs = make_jobs("hub", hub_routes)
        if hub_jobs:
            print(f"[AYCF] Priority tier complete. Starting hub tier with {len(hub_jobs)} pending checks; total resumed {stats['resumed']} checks with {stats['resumed_flights']} cached flights.", flush=True)
            fetcher.run(hub_jobs, on_result)
        if stats["processed"] == total_checks and total_checks and stats["route_day_checks"] == 0:
            print(f"[AYCF] {total_checks}/{total_checks} | all checks resumed from SQLite | flights {stats['flights_found']} cached.", flush=True)
        db.mark_pdf_scanned(run_id)
        db.finish_scan(scan_id, "completed", stats["route_day_checks"], stats["live_requests"], stats["flights_found"])
        return {"ok": True, "skipped": False, "pdf_run_id": run_id, "scope_id": scope_id, "scope": scope, "generated_at": generated.isoformat(), "priority_routes": len(primary_routes), "hub_routes": len(hub_routes), "routes": len(route_pairs), "pdf_routes": len(all_route_pairs), "total_route_day_checks": total_checks, "route_day_checks": stats["route_day_checks"], "resumed_checks": stats["resumed"], "resumed_flights": stats["resumed_flights"], "live_requests": stats["live_requests"], "flights_found": stats["flights_found"], "workers": workers, "global_request_interval": start_interval, "no_availability_responses": stats["no_availability"], "wallet_redirects": stats["wallet_redirects"], "html_retries": stats["html_retries"]}
    except Exception as exc:
        db.finish_scan(scan_id, "failed", stats["route_day_checks"], stats["live_requests"], stats["flights_found"], str(exc))
        print(f"[AYCF] Scan stopped after {stats['processed']}/{total_checks}; completed checks and {stats['flights_found']} known flights remain preserved in SQLite.", flush=True)
        raise
