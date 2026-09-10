"""Multi-origin / multi-destination AYCF journey search for the Termux web app."""

import os
import hmac
from datetime import date
from pathlib import Path

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from cache_db import ScanCacheDB
from itinerary_search import cached_scan_itineraries
from search_cache import SearchCache
from search_support import (DEFAULT_MAX_STOPS, DEFAULT_MIN_TRANSFER, DEFAULT_MAX_LAYOVER,
                            DEFAULT_MAX_JOURNEY, env_int as _env_int, form_int as _form_int,
                            canonical_city as _canonical_city, approved_connections as _approved_connections,
                            decorate_itineraries as _decorate, append_unique as _append_unique)
from recommendation_preferences import scan_scope_with_preferences
from scan_scope import load_scope, scan_plan, scope_fingerprint, scan_window, scan_run_id
from scanner import CurrentRouteGraph

bp = Blueprint("multi_search", __name__)

ROOT = Path(__file__).resolve().parent.parent


def _cache_dir() -> str:
    return os.environ.get("AYCF_CACHE_DIR", str(ROOT / "cache"))


def _graph() -> CurrentRouteGraph:
    direct_dir = Path(_cache_dir()) / "direct-data"
    return CurrentRouteGraph(str(direct_dir))


def _current_scope_run(graph: CurrentRouteGraph, db: ScanCacheDB):
    frame = graph.latest_frame()
    pairs = sorted(set(zip(frame["departure_from"], frame["departure_to"])))
    generated = str(frame["data_generated"].iloc[0]).strip() if "data_generated" in frame.columns and len(frame) else ""
    # Use the same enriched scope as the planner page and morning workers.
    # Preferences and enabled watches are part of the run fingerprint.
    scope = scan_scope_with_preferences(load_scope())
    plan = scan_plan(pairs, scope, days=scan_window(frame)["days"])
    selected_pairs = plan["routes"]
    scope_id = scope_fingerprint(scope)
    run_id = scan_run_id(generated, scope, selected_pairs)
    run = db.get_pdf_run(run_id) if run_id else None
    return {"scope": scope, "run_id": run_id, "ready": bool(run and run.get("scanned_at"))}


def _scanned_origins(db: ScanCacheDB, run_id: str):
    with db.connect() as conn:
        return [r["origin"] for r in conn.execute("SELECT DISTINCT origin FROM route_checks WHERE pdf_run_id=? ORDER BY origin", (run_id,)).fetchall()]


@bp.post("/multi-scan")
def scan():
    expected = session.get("csrf_token", "")
    supplied = request.form.get("csrf_token", "")
    if not expected or not supplied or not hmac.compare_digest(expected, supplied):
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

    if any(not _canonical_city(graph, raw) for raw in raw_origins):
        flash("Choose starting airports from the current AYCF route list.", "warning")
        return redirect(url_for("index"))

    destination_only = bool(destinations and not raw_origins)
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
    max_stops = _form_int("max_stops", DEFAULT_MAX_STOPS, 0, 2)
    min_transfer = _form_int("min_transfer_minutes", DEFAULT_MIN_TRANSFER, 120, 600)
    max_layover = DEFAULT_MAX_LAYOVER  # Discover up to 48h per connection; results filters narrow this.
    max_journey = DEFAULT_MAX_JOURNEY  # Journey duration is a reversible results filter.
    max_results = _env_int("AYCF_MAX_RESULTS", 100, 1, 500)
    max_paths = _env_int("AYCF_MAX_PATHS_PER_DAY", 250, 10, 1000)

    wants_return = request.form.get("return_trip") == "on" and bool(destinations) and not destination_only
    try:
        return_start = date.fromisoformat((request.form.get("return_start_date") or "").strip()) if wants_return else start_day
    except ValueError:
        return_start = start_day
    return_start = max(return_start, start_day)

    outbound, returns = [], []
    seen_outbound, seen_return = set(), set()
    search_db = SearchCache(db, scope_ctx["run_id"])

    try:
        if not destinations:
            for origin in origins:
                found, _ = cached_scan_itineraries(graph, search_db, origin, None, start_day, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, limit=max_results, max_paths_per_day=max_paths, pdf_run_id=scope_ctx["run_id"], max_transfer_minutes=max_layover, scope=scope_ctx["scope"], approved_hubs=scope_ctx["scope"].get("connection_hubs") or [], max_journey_minutes=max_journey, requested_origins=raw_origins, requested_destinations=raw_destinations)
                _append_unique(outbound, seen_outbound, _approved_connections(found, scope_ctx["scope"]))
        else:
            for origin in origins:
                for destination in destinations:
                    if origin == destination:
                        continue
                    found, _ = cached_scan_itineraries(graph, search_db, origin, destination, start_day, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, limit=max_results, max_paths_per_day=max_paths, pdf_run_id=scope_ctx["run_id"], max_transfer_minutes=max_layover, scope=scope_ctx["scope"], approved_hubs=scope_ctx["scope"].get("connection_hubs") or [], max_journey_minutes=max_journey, requested_origins=raw_origins, requested_destinations=raw_destinations)
                    _append_unique(outbound, seen_outbound, _approved_connections(found, scope_ctx["scope"]))
            if wants_return:
                for destination in destinations:
                    for origin in origins:
                        if destination == origin:
                            continue
                        found, _ = cached_scan_itineraries(graph, search_db, destination, origin, return_start, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, limit=max_results, max_paths_per_day=max_paths, pdf_run_id=scope_ctx["run_id"], max_transfer_minutes=max_layover, scope=scope_ctx["scope"], approved_hubs=scope_ctx["scope"].get("connection_hubs") or [], max_journey_minutes=max_journey, requested_origins=raw_destinations, requested_destinations=raw_origins)
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

    return render_template("results.html", outbound=outbound, returns=returns, origins=display_origins, origin=origin_label, destination=destination_label, start_date=start_day.isoformat(), return_start_date=return_start.isoformat() if wants_return else None, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, max_layover_minutes=max_layover, max_journey_minutes=max_journey, live_requests=0, return_requested=wants_return, result_source="morning-cache", cache_misses=len(search_db.missing), cache_stats=db.stats(scope_ctx["run_id"]), results_limited=(len(outbound) >= max_results or len(returns) >= max_results), result_hubs=hubs, destination_only=destination_only)
