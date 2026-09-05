"""Multi-origin / multi-destination AYCF journey search for the Termux web app."""

import hashlib
import os
from datetime import date, datetime
from pathlib import Path

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from cache_db import ScanCacheDB
from itinerary_search import cached_scan_itineraries
from recommendation_preferences import scan_scope_with_preferences
from scan_scope import AIRPORT_GROUPS, load_scope, normalize_name, scan_plan, scope_fingerprint
from scanner import CurrentRouteGraph

bp = Blueprint("multi_search", __name__)

ROOT = Path(__file__).resolve().parent.parent


def _cache_dir() -> str:
    return os.environ.get("AYCF_CACHE_DIR", str(ROOT / "cache"))


def _form_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(request.form.get(name) or default)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _graph() -> CurrentRouteGraph:
    direct_dir = Path(_cache_dir()) / "direct-data"
    return CurrentRouteGraph(str(direct_dir))


def _canonical_city(graph: CurrentRouteGraph, value: str):
    wanted = normalize_name(value)
    if not wanted:
        return None
    by_key = {normalize_name(city): city for city in graph.cities()}
    if wanted in by_key:
        return by_key[wanted]
    for group, members in AIRPORT_GROUPS.items():
        if wanted in {normalize_name(x) for x in members} and normalize_name(group) in by_key:
            return by_key[normalize_name(group)]
    return None


def _current_scope_run(graph: CurrentRouteGraph, db: ScanCacheDB):
    frame = graph.latest_frame()
    pairs = sorted(set(zip(frame["departure_from"], frame["departure_to"])))
    generated = str(frame["data_generated"].iloc[0]).strip() if "data_generated" in frame.columns and len(frame) else ""
    # Use the same enriched scope as the planner page and morning workers.
    # Preferences and enabled watches are part of the run fingerprint.
    scope = scan_scope_with_preferences(load_scope())
    plan = scan_plan(pairs, scope, days=4)
    selected_pairs = plan["routes"]
    scope_id = scope_fingerprint(scope)
    run_id = None
    if generated and selected_pairs:
        run_id = hashlib.sha256(
            (generated + "\n" + scope_id + "\n" + "\n".join(f"{a}>{b}" for a, b in selected_pairs)).encode()
        ).hexdigest()[:20]
    run = db.get_pdf_run(run_id) if run_id else None
    return {"scope": scope, "run_id": run_id, "ready": bool(run and run.get("scanned_at"))}


def _approved_connections(items, scope):
    approved = {normalize_name(x) for x in scope.get("connection_hubs") or []}
    out = []
    for item in items:
        path = item.get("path") or []
        if len(path) <= 2:
            out.append(item)
            continue
        intermediate = path[1:-1]
        if approved and all(normalize_name(hub) in approved for hub in intermediate):
            out.append(item)
    return out


def _decorate(items, max_journey_minutes=0):
    out = []
    for item in items:
        legs = item.get("legs") or []
        if not legs:
            continue
        first, last = legs[0], legs[-1]
        try:
            dep = datetime.fromisoformat(first["departure"])
            arr = datetime.fromisoformat(last["arrival"])
            total_minutes = max(0, int((arr - dep).total_seconds() // 60))
        except Exception:
            total_minutes = 0
        if max_journey_minutes and total_minutes > max_journey_minutes:
            continue
        path = item.get("path") or [first.get("origin"), last.get("destination")]
        waits = item.get("connection_minutes_list") or []
        connections = []
        for idx, minutes in enumerate(waits):
            minutes = int(minutes)
            connections.append({"hub": path[idx + 1] if idx + 1 < len(path) - 1 else "", "minutes": minutes, "risky": 120 <= minutes < 150})
        stop_count = max(0, len(legs) - 1)
        risky = any(c["risky"] for c in connections)
        layover_penalty = sum(max(0, int(m) - 240) for m in waits)
        journey_score = stop_count * 10000 + (2500 if risky else 0) + total_minutes + layover_penalty
        row = dict(item)
        row.update({"origin": path[0], "destination": path[-1], "hubs": path[1:-1], "hub": " + ".join(path[1:-1]), "stop_count": stop_count, "is_direct": len(legs) == 1, "total_minutes": total_minutes, "connections": connections, "connection_minutes_list": waits, "connection_minutes": min(waits) if waits else None, "risky_connection": risky, "departure_time": first.get("departure", "")[11:16], "arrival_time": last.get("arrival", "")[11:16], "journey_score": journey_score})
        out.append(row)
    out.sort(key=lambda r: (r["journey_score"], r["legs"][0].get("departure", ""), r["destination"]))
    return out


def _append_unique(target, seen, items):
    for item in items:
        sig = tuple((leg.get("flight_code"), leg.get("departure"), leg.get("arrival")) for leg in item.get("legs") or [])
        if sig and sig not in seen:
            seen.add(sig)
            target.append(item)


def _scanned_origins(db: ScanCacheDB, run_id: str):
    with db.connect() as conn:
        return [r["origin"] for r in conn.execute("SELECT DISTINCT origin FROM route_checks WHERE pdf_run_id=? ORDER BY origin", (run_id,)).fetchall()]


@bp.post("/multi-scan")
def scan():
    expected = session.get("csrf_token", "")
    supplied = request.form.get("csrf_token", "")
    if not expected or not supplied or expected != supplied:
        flash("Your form expired. Please submit the search again.", "warning")
        return redirect(url_for("index"))

    graph = _graph()
    db = ScanCacheDB()
    scope_ctx = _current_scope_run(graph, db)
    if not scope_ctx["ready"]:
        flash("The current scan scope has not completed yet. Run the morning cache first.", "warning")
        return redirect(url_for("index"))

    raw_destinations = [str(x).strip() for x in request.form.getlist("destinations") if str(x).strip()]
    destinations = []
    for raw in raw_destinations:
        city = _canonical_city(graph, raw)
        if city and city not in destinations:
            destinations.append(city)
    invalid_destinations = [x for x in raw_destinations if not _canonical_city(graph, x)]
    if invalid_destinations:
        flash("Choose destinations from the current AYCF route list.", "warning")
        return redirect(url_for("index"))

    raw_origins = [str(x).strip() for x in request.form.getlist("origins") if str(x).strip()]
    origins = []
    for raw in raw_origins:
        city = _canonical_city(graph, raw)
        if city and city not in origins:
            origins.append(city)

    destination_only = bool(destinations and not origins)
    if destination_only:
        # Discovery mode: a destination on its own means "show every scanned place I can start from".
        # This deliberately uses the completed scan cache rather than inventing routes from the public graph.
        origins = [o for o in _scanned_origins(db, scope_ctx["run_id"]) if o not in destinations]
    if not origins:
        flash("Select at least one starting airport, or choose a destination on its own to discover where you can fly from.", "warning")
        return redirect(url_for("index"))

    try:
        start_day = date.fromisoformat((request.form.get("start_date") or "").strip())
    except ValueError:
        start_day = date.today()
    start_day = max(start_day, date.today())

    days = _form_int("days", 4, 1, 4)
    max_stops = _form_int("max_stops", 1, 0, 2)
    min_transfer = _form_int("min_transfer_minutes", 120, 120, 600)
    max_layover = _form_int("max_layover_minutes", 480, 120, 1080)
    max_journey = _form_int("max_journey_minutes", 720, 0, 2160)
    max_results = max(1, min(500, int(os.environ.get("AYCF_MAX_RESULTS", "100"))))
    max_paths = max(10, min(1000, int(os.environ.get("AYCF_MAX_PATHS_PER_DAY", "250"))))

    wants_return = request.form.get("return_trip") == "on" and bool(destinations) and not destination_only
    try:
        return_start = date.fromisoformat((request.form.get("return_start_date") or "").strip()) if wants_return else start_day
    except ValueError:
        return_start = start_day
    return_start = max(return_start, start_day)

    outbound, returns = [], []
    seen_outbound, seen_return = set(), set()
    cache_misses = 0

    try:
        if not destinations:
            for origin in origins:
                found, misses = cached_scan_itineraries(graph, db, origin, None, start_day, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, limit=max_results, max_paths_per_day=max_paths, pdf_run_id=scope_ctx["run_id"], max_transfer_minutes=max_layover, approved_hubs=scope_ctx["scope"].get("connection_hubs") or [], max_journey_minutes=max_journey, requested_origins=raw_origins, requested_destinations=raw_destinations)
                cache_misses += misses
                _append_unique(outbound, seen_outbound, _approved_connections(found, scope_ctx["scope"]))
        else:
            for origin in origins:
                for destination in destinations:
                    if origin == destination:
                        continue
                    found, misses = cached_scan_itineraries(graph, db, origin, destination, start_day, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, limit=max_results, max_paths_per_day=max_paths, pdf_run_id=scope_ctx["run_id"], max_transfer_minutes=max_layover, approved_hubs=scope_ctx["scope"].get("connection_hubs") or [], max_journey_minutes=max_journey, requested_origins=raw_origins, requested_destinations=raw_destinations)
                    cache_misses += misses
                    _append_unique(outbound, seen_outbound, _approved_connections(found, scope_ctx["scope"]))
            if wants_return:
                for destination in destinations:
                    for origin in origins:
                        if destination == origin:
                            continue
                        found, misses = cached_scan_itineraries(graph, db, destination, origin, return_start, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, limit=max_results, max_paths_per_day=max_paths, pdf_run_id=scope_ctx["run_id"], max_transfer_minutes=max_layover, approved_hubs=scope_ctx["scope"].get("connection_hubs") or [], max_journey_minutes=max_journey, requested_origins=raw_destinations, requested_destinations=raw_origins)
                        cache_misses += misses
                        _append_unique(returns, seen_return, _approved_connections(found, scope_ctx["scope"]))
    except Exception as exc:
        flash(f"Cache search failed safely: {exc}", "danger")
        return redirect(url_for("index"))

    outbound = _decorate(outbound, max_journey)[:max_results]
    returns = _decorate(returns, max_journey)[:max_results]
    hubs = sorted({hub for row in outbound + returns for hub in row.get("hubs", [])})

    display_origins = raw_origins or (origins if not destination_only else [])
    display_destinations = raw_destinations or destinations
    destination_label = " + ".join(display_destinations) if display_destinations else None
    origin_label = " + ".join(display_origins) if display_origins else "Any scanned origin"

    return render_template("results.html", outbound=outbound, returns=returns, origins=display_origins, origin=origin_label, destination=destination_label, start_date=start_day.isoformat(), return_start_date=return_start.isoformat() if wants_return else None, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, max_layover_minutes=max_layover, max_journey_minutes=max_journey, live_requests=0, return_requested=wants_return, result_source="morning-cache", cache_misses=cache_misses, cache_stats=db.stats(), result_hubs=hubs, destination_only=destination_only)
