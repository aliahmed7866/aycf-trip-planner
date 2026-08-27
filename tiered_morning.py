"""Priority morning scan: selected UK origins first, then selected reachable hubs."""

import hashlib
import os
import time

from cache_db import ScanCacheDB
from direct_pdf import refresh_direct_snapshot
from morning_scan import (
    CapturedRequestWizzClient,
    _apply_wizz_runtime,
    _cache_dir,
    _mirror_for_web,
    _scan_days,
)
from scan_scope import load_scope, origin_variants, scan_plan, scope_fingerprint, scope_summary
from session_vault import SessionVault
from station_resolver import prepare_required_stations


def _route_variants(origin: str, tier: str, scope: dict) -> list[str]:
    if tier == "primary":
        return origin_variants(origin, scope)
    return [origin]


def run(force: bool = False) -> dict:
    cache_root = _cache_dir()
    _, df, generated, departure_start, departure_end = refresh_direct_snapshot(
        cache_root,
        os.environ.get("AYCF_PDF_URL", "https://multipass.wizzair.com/aycf-availability.pdf"),
    )
    _mirror_for_web(cache_root, df, generated)

    all_route_pairs = sorted(set(zip(df["departure_from"], df["departure_to"])))
    scope = load_scope()
    days = list(_scan_days(departure_start, departure_end))
    estimate_seconds = float(os.environ.get("AYCF_SCAN_SECONDS_PER_CHECK", "1.25"))
    plan = scan_plan(all_route_pairs, scope, days=len(days), seconds_per_request=estimate_seconds)
    primary_routes = plan["primary_routes"]
    hub_routes = plan["hub_routes"]
    route_entries = [("primary", a, b) for a, b in primary_routes] + [("hub", a, b) for a, b in hub_routes]
    if not route_entries:
        raise RuntimeError("Your scan scope matches no routes in the current AYCF PDF. Adjust Morning scan scope in the app.")

    scope_id = scope_fingerprint(scope)
    route_pairs = [(a, b) for _, a, b in route_entries]
    run_id = hashlib.sha256(
        (generated.isoformat() + "\n" + scope_id + "\n" + "\n".join(f"{a}>{b}" for a, b in route_pairs)).encode()
    ).hexdigest()[:20]

    station_names = set()
    for tier, origin, destination in route_entries:
        station_names.update(_route_variants(origin, tier, scope))
        station_names.add(destination)

    db = ScanCacheDB()
    db.upsert_pdf_run(
        run_id,
        generated.isoformat(),
        departure_start.isoformat(),
        departure_end.isoformat(),
        len(route_pairs),
        scope_id=scope_id,
        scope=scope,
    )
    current = db.get_pdf_run(run_id)
    if current and current.get("scanned_at") and not force:
        return {"ok": True, "skipped": True, "reason": "Current PDF and scan scope already scanned", "pdf_run_id": run_id, "scope_id": scope_id}
    if db.scan_in_progress(run_id) and not force:
        return {"ok": True, "skipped": True, "reason": "A scan for this PDF and scope is already running", "pdf_run_id": run_id, "scope_id": scope_id}

    state = SessionVault().load()
    if not state:
        raise RuntimeError("No saved Wizz session. Import a Wizz session before the scheduled scan.")
    client = CapturedRequestWizzClient(
        state,
        cache_ttl=int(os.environ.get("AYCF_LIVE_CACHE_SECONDS", "300")),
        min_delay=float(os.environ.get("AYCF_MIN_REQUEST_DELAY", "1.0")),
    )
    if not _apply_wizz_runtime(client):
        client.bootstrap()

    station_report = prepare_required_stations(client, sorted(station_names))
    print(
        f"[AYCF] PDF {generated.isoformat()} | scope {scope_id} | "
        f"priority {len(primary_routes)} routes + hubs {len(hub_routes)} routes | "
        f"{plan['checks']} checks | ~{plan['estimated_minutes']} min estimated | "
        f"stations {station_report['resolved']}/{station_report['required']} resolved",
        flush=True,
    )
    print(f"[AYCF] Scope: {scope_summary(scope)}", flush=True)
    if station_report["unresolved"]:
        raise RuntimeError("Station preflight failed before live scanning. Unresolved scoped stations: " + ", ".join(station_report["unresolved"]))
    print("[AYCF] Station preflight OK for selected scope.", flush=True)

    preflight = client.preflight()
    print(
        f"[AYCF] Captured-request preflight OK ({preflight.get('response')})."
        if preflight.get("ok")
        else f"[AYCF] Preflight skipped: {preflight.get('reason')}",
        flush=True,
    )

    total_checks = len(route_entries) * len(days)
    progress_every = max(1, int(os.environ.get("AYCF_PROGRESS_EVERY", "10")))
    scan_id = db.start_scan(run_id)
    route_day_checks = flights_found = resumed_checks = processed = 0
    started = time.time()
    try:
        # Route tier is outermost deliberately: all priority UK routes complete
        # before lower-priority hub expansion begins.
        for tier, origin, destination in route_entries:
            for day in days:
                processed += 1
                if db.route_checked(run_id, origin, destination, day) and not force:
                    resumed_checks += 1
                    if processed == 1 or processed % progress_every == 0 or processed == total_checks:
                        print(f"[AYCF] {processed}/{total_checks} | {tier} | resumed {resumed_checks} | live {route_day_checks} | flights {flights_found}", flush=True)
                    continue

                variants = _route_variants(origin, tier, scope)
                merged_flights = []
                for concrete_origin in variants:
                    merged_flights.extend(client.check(concrete_origin, destination, day))
                merged_flights.sort(key=lambda f: f.departure)
                db.replace_route_check(run_id, origin, destination, day, merged_flights)
                route_day_checks += 1
                flights_found += len(merged_flights)

                if processed == 1 or processed % progress_every == 0 or processed == total_checks:
                    elapsed = max(1.0, time.time() - started)
                    rate = route_day_checks / elapsed if route_day_checks else 0.0
                    print(
                        f"[AYCF] {processed}/{total_checks} | {tier} | {'/'.join(variants)}->{destination} {day} | "
                        f"live {route_day_checks} | resumed {resumed_checks} | flights {flights_found} | "
                        f"no-availability {client.no_availability_responses} | {rate:.2f} checks/s",
                        flush=True,
                    )

        db.mark_pdf_scanned(run_id)
        db.finish_scan(scan_id, "completed", route_day_checks, client.live_requests, flights_found)
        return {
            "ok": True,
            "skipped": False,
            "pdf_run_id": run_id,
            "scope_id": scope_id,
            "scope": scope,
            "generated_at": generated.isoformat(),
            "priority_routes": len(primary_routes),
            "hub_routes": len(hub_routes),
            "routes": len(route_pairs),
            "pdf_routes": len(all_route_pairs),
            "total_route_day_checks": total_checks,
            "route_day_checks": route_day_checks,
            "resumed_checks": resumed_checks,
            "live_requests": client.live_requests,
            "flights_found": flights_found,
            "estimated_minutes": plan["estimated_minutes"],
            "no_availability_responses": client.no_availability_responses,
            "wallet_redirects": client.wallet_redirects,
            "html_retries": client.html_retries,
        }
    except Exception as exc:
        db.finish_scan(scan_id, "failed", route_day_checks, client.live_requests, flights_found, str(exc))
        print(f"[AYCF] Scan stopped after {processed}/{total_checks}; completed checks remain preserved in SQLite.", flush=True)
        raise
