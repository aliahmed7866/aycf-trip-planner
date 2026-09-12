"""Priority morning scan: selected UK origins first, then selected reachable hubs."""

import os
import time
from datetime import date

from cache_db import ScanCacheDB
from direct_pdf import refresh_direct_snapshot
from morning_scan import CapturedRequestWizzClient, _apply_wizz_runtime, _cache_dir, _mirror_for_web, _scan_days, verify_scan_requests
from parallel_fetch import ParallelFetcher
from recommendation_preferences import scan_scope_with_preferences
from scan_scope import airport_variants, load_scope, route_priority, scan_plan, scope_fingerprint, scan_run_id, scope_summary, scan_jobs, configured_workers, origin_variants
from session_vault import SessionVault
from scanner import WizzSessionExpired
from station_resolver import prepare_required_stations


def _bounded_int(name: str, default: int, low: int, high: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return max(low, min(high, value))


def _adaptive_refresh_ttl(travel_day, cached_count: int, high_value: bool) -> int:
    """Choose a conservative freshness window for AYCF's short booking window."""
    offset = (travel_day - date.today()).days
    has_seats = int(cached_count or 0) > 0
    if offset <= 0:
        ttl = 600 if has_seats else 1200
    elif offset == 1:
        ttl = 900 if has_seats else 1800
    elif offset == 2:
        ttl = 1800 if has_seats else 3600
    else:
        ttl = 3600 if has_seats else 5400
    if high_value:
        ttl = max(600, int(ttl * 0.75))
    return ttl


def run(force: bool = False) -> dict:
    db = ScanCacheDB()
    with db.scan_lock() as acquired:
        if not acquired:
            return {"ok": True, "skipped": True, "state": "already_running", "scan_performed": False,
                    "reason": "A scan is already running"}
        return _run_locked(db, force)


def _run_locked(db, force: bool = False) -> dict:
    cache_root = _cache_dir()
    _, df, generated, departure_start, departure_end = refresh_direct_snapshot(
        cache_root,
        os.environ.get("AYCF_PDF_URL", "https://multipass.wizzair.com/aycf-availability.pdf"),
    )
    _mirror_for_web(cache_root, df, generated)

    # IMPORTANT: the daily live scan topology comes only from the current AYCF
    # PDF. The public Wizz route sitemap is a reference/catalogue source only.
    all_route_pairs = sorted(set(zip(df["departure_from"], df["departure_to"])))
    scope = scan_scope_with_preferences(load_scope())
    days = list(_scan_days(departure_start, departure_end))
    workers = configured_workers(scope)
    start_interval = max(0.2, float(os.environ.get("AYCF_GLOBAL_REQUEST_INTERVAL", "1.0")))
    refresh_override_raw = os.environ.get("AYCF_MANUAL_REFRESH_TTL_SECONDS")
    manual_refresh_ttl = _bounded_int("AYCF_MANUAL_REFRESH_TTL_SECONDS", 1800, 0, 21600) if refresh_override_raw is not None else None

    plan = scan_plan(
        all_route_pairs,
        scope,
        days=len(days),
        seconds_per_request=float(os.environ.get("AYCF_SCAN_SECONDS_PER_CHECK", "1.25")),
    )
    primary_routes, hub_routes = plan["primary_routes"], plan["hub_routes"]
    route_entries = [("primary", a, b) for a, b in primary_routes] + [("hub", a, b) for a, b in hub_routes]
    if not route_entries:
        raise RuntimeError("Your scan scope matches no routes in the current AYCF PDF. Adjust Morning scan scope in the app.")

    scope_id = scope_fingerprint(scope)
    route_pairs = [(a, b) for _, a, b in route_entries]
    run_id = scan_run_id(generated, scope, route_pairs)

    station_names = set()
    for _, origin, destination in route_entries:
        station_names.update(airport_variants(origin, scope))
        station_names.update(airport_variants(destination, scope))

    db.upsert_pdf_run(run_id, generated.isoformat(), departure_start.isoformat(), departure_end.isoformat(), len(route_pairs), scope_id=scope_id, scope=scope)
    current = db.get_pdf_run(run_id)
    if current and current.get("scanned_at") and not force:
        return {"ok": True, "skipped": True, "state": "already_current", "reason": "Current PDF and scan scope already scanned", "pdf_run_id": run_id, "scope_id": scope_id}

    state = SessionVault().load()
    if not state:
        raise WizzSessionExpired("No saved Wizz session; authentication renewal is required.")
    coordinator = CapturedRequestWizzClient(state, cache_ttl=int(os.environ.get("AYCF_LIVE_CACHE_SECONDS", "300")), min_delay=0.2)
    if not _apply_wizz_runtime(coordinator):
        coordinator.bootstrap()
    station_report = prepare_required_stations(coordinator, sorted(station_names))

    print(f"[AYCF] Scan catalog: current AYCF PDF only | {len(all_route_pairs)} routes.", flush=True)
    print(f"[AYCF] PDF {generated.isoformat()} | scope {scope_id} | priority {len(primary_routes)} routes + hubs {len(hub_routes)} routes | {plan['checks']} checks | workers {workers} | global start interval {start_interval:.2f}s | stations {station_report['resolved']}/{station_report['required']} resolved", flush=True)
    print(f"[AYCF] Scope: {scope_summary(scope)}", flush=True)
    if force:
        if manual_refresh_ttl is None:
            print("[AYCF] Smart refresh: departure-aware TTLs; nearer/currently-available routes refresh sooner, stable empty checks later.", flush=True)
        elif manual_refresh_ttl == 0:
            print("[AYCF] Full live refresh requested; no route/day checks will be reused.", flush=True)
        else:
            print(f"[AYCF] Fixed incremental refresh: reusing route/day checks newer than {manual_refresh_ttl}s.", flush=True)
    if station_report["unresolved"]:
        raise RuntimeError("Station preflight failed before live scanning. Unresolved scoped stations: " + ", ".join(station_report["unresolved"]))
    directory = scope.get('_route_directory', {})
    if directory:
        print(f"[AYCF] Airport route directory active: {len(directory['routes'])} departure airports; captured {directory['captured_at']}. Requests use listed directional pairs where the origin is covered.", flush=True)
    else:
        print("[AYCF] No fresh airport route directory. City groups may include unconfirmed airport pairs; refresh via the Chrome connection capture.", flush=True)
    print("[AYCF] Station preflight OK for selected scope.", flush=True)
    preflight = verify_scan_requests(coordinator, scan_jobs(plan, scope, days))
    if not preflight.get("ok"):
        return preflight
    print(f"[AYCF] Captured-request preflight OK ({preflight.get('response')})." if preflight.get("ok") else f"[AYCF] Preflight skipped: {preflight.get('reason')}", flush=True)
    resolved_station_ids = dict(coordinator.station_ids)

    def client_factory():
        client = CapturedRequestWizzClient(state, cache_ttl=int(os.environ.get("AYCF_LIVE_CACHE_SECONDS", "300")), min_delay=0.2)
        # Preflight may have rotated the endpoint. Do not reload stale disk metadata.
        client.dynamic_url = coordinator.dynamic_url
        client.captured_request_method = coordinator.captured_request_method
        client.captured_template_type = coordinator.captured_template_type
        client.captured_request_template = coordinator.captured_request_template
        client.http.cookies.update(coordinator.http.cookies)
        client.station_ids.update(resolved_station_ids)
        return client

    fetcher = ParallelFetcher(client_factory, workers=workers, start_interval=start_interval)
    total_checks = len(route_entries) * len(days)
    progress_every = max(1, int(os.environ.get("AYCF_PROGRESS_EVERY", "10")))
    scan_id = db.start_scan(run_id)
    stats = {"route_day_checks": 0, "flights_found": 0, "resumed_flights": 0, "resumed": 0, "processed": 0, "live_requests": coordinator.live_requests, "no_availability": coordinator.no_availability_responses, "wallet_redirects": coordinator.wallet_redirects, "html_retries": coordinator.html_retries, "airport_verified": 0, "airport_unknown": 0}
    started = time.time()

    def make_jobs():
        jobs = []
        for job in scan_jobs(plan, scope, days):
            tier, origin, destination, day, _, _, _ = job
            high_value = route_priority(origin, destination, scope) <= 2 or bool(origin_variants(destination, scope))
            cached_count = None
            if force:
                info = db.route_check_info(run_id, origin, destination, day)
                if info:
                    ttl = manual_refresh_ttl if manual_refresh_ttl is not None else _adaptive_refresh_ttl(day, info["flight_count"], high_value)
                    if ttl > 0 and info["age_seconds"] <= ttl:
                        cached_count = int(info["flight_count"])
            else:
                cached_count = db.route_flight_count(run_id, origin, destination, day)
            if cached_count is not None:
                stats["resumed"] += 1
                stats["resumed_flights"] += cached_count
                stats["flights_found"] += cached_count
                stats["processed"] += 1
                continue
            jobs.append(job)
        return jobs

    def on_result(result):
        unknown = result.get("unknown", [])
        stats['airport_verified'] += len(result.get('checked_pairs', []))
        stats['airport_unknown'] += len(unknown)
        db.replace_route_check(run_id, result["origin"], result["destination"], result["day"], result["flights"], complete=not unknown, checked_pairs=result.get("checked_pairs"))
        if unknown:
            unknown_checks.append(result)
            print(f"[AYCF] {len(unknown)} airport checks pending for {result['origin']} -> {result['destination']} on {result['day']}: " + "; ".join(unknown), flush=True)
        else:
            stats["route_day_checks"] += 1
        stats["flights_found"] += len(result["flights"])
        stats["processed"] += 1
        stats["live_requests"] += result["live_requests"]
        stats["no_availability"] += result["no_availability"]
        stats["wallet_redirects"] += result["wallet_redirects"]
        stats["html_retries"] += result["html_retries"]
        if stats["processed"] == 1 or stats["processed"] % progress_every == 0 or stats["processed"] == total_checks:
            elapsed = max(1.0, time.time() - started)
            rate = (stats["processed"] - stats["resumed"]) / elapsed
            route_label = f"{'/'.join(result['origin_variants'])} -> {'/'.join(result['destination_variants'])}"
            print(f"[AYCF] {stats['processed']}/{total_checks} | {result['tier']} | {route_label} {result['day']} | complete groups {stats['route_day_checks']} | partial groups {len(unknown_checks)} | airport checks verified {stats['airport_verified']} / unknown {stats['airport_unknown']} | resumed {stats['resumed']} | flights {stats['flights_found']} (cached {stats['resumed_flights']}) | requests {stats['live_requests']} | no-availability {stats['no_availability']} | {rate:.2f} groups/s", flush=True)

    unknown_checks = []

    def on_unknown(job, exc):
        unknown_checks.append(job)
        print(f"[AYCF] Pending: {exc}", flush=True)

    try:
        jobs = make_jobs()
        if jobs:
            print(f"[AYCF] Starting {len(jobs)} pending checks across {workers} workers: nearest date, selected destinations, regional priority, then base/hub routes.", flush=True)
            fetcher.run(jobs, on_result, on_unknown=on_unknown)
        if stats["processed"] == total_checks and total_checks and stats["resumed"] == total_checks:
            print(f"[AYCF] {total_checks}/{total_checks} | all checks resumed from SQLite | flights {stats['flights_found']} cached.", flush=True)
        if unknown_checks:
            message = f"{len(unknown_checks)} route/date checks remain unverified; verified flights are preserved and usable. Wallet redirects remain unknown; repeated redirects need request or airport-directory diagnosis, not repeated full scans."
            db.finish_scan(scan_id, "partial", stats["route_day_checks"], stats["live_requests"], stats["flights_found"], message)
            return {"ok": False, "state": "partial", "reason": message, "unknown_checks": len(unknown_checks), "route_day_checks": stats["route_day_checks"], "resumed_checks": stats["resumed"], "airport_verified": stats["airport_verified"], "airport_unknown": stats["airport_unknown"], "flights_found": db.stats(run_id)["cached_flights"], "scan_performed": True, "pdf_run_id": run_id}
        db.mark_pdf_scanned(run_id)
        db.finish_scan(scan_id, "completed", stats["route_day_checks"], stats["live_requests"], stats["flights_found"])
        return {"ok": True, "skipped": False, "pdf_run_id": run_id, "scope_id": scope_id, "scope": scope, "generated_at": generated.isoformat(), "priority_routes": len(primary_routes), "hub_routes": len(hub_routes), "routes": len(route_pairs), "pdf_routes": len(all_route_pairs), "total_route_day_checks": total_checks, "route_day_checks": stats["route_day_checks"], "resumed_checks": stats["resumed"], "resumed_flights": stats["resumed_flights"], "live_requests": stats["live_requests"], "flights_found": stats["flights_found"], "workers": workers, "global_request_interval": start_interval, "manual_refresh_ttl_seconds": manual_refresh_ttl if force else None, "adaptive_refresh": bool(force and manual_refresh_ttl is None), "no_availability_responses": stats["no_availability"], "wallet_redirects": stats["wallet_redirects"], "html_retries": stats["html_retries"]}
    except Exception as exc:
        db.finish_scan(scan_id, "failed", stats["route_day_checks"], stats["live_requests"], stats["flights_found"], str(exc))
        print(f"[AYCF] Scan stopped after {stats['processed']}/{total_checks}; completed checks and {stats['flights_found']} known flights remain preserved in SQLite.", flush=True)
        raise
