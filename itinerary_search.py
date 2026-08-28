"""Build multi-stop AYCF itineraries entirely from the morning SQLite cache."""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from scanner import Flight


def _graph_paths(graph, origin: str, destination: Optional[str], day: date, max_stops: int, max_paths: int) -> List[List[str]]:
    """Enumerate simple paths with direct routes first, bounded to at most two stops."""
    max_stops = max(0, min(2, int(max_stops)))
    max_legs = max_stops + 1
    edges = graph.edges_for_day(day)
    adj: Dict[str, List[str]] = {}
    for a, b in edges:
        adj.setdefault(a, []).append(b)
    for node in adj:
        adj[node] = sorted(set(adj[node]))

    results: List[List[str]] = []
    frontier: List[List[str]] = [[origin]]
    for _ in range(max_legs):
        next_frontier: List[List[str]] = []
        for path in frontier:
            for nxt in adj.get(path[-1], []):
                if nxt in path:  # no cycles / backtracking through a visited airport
                    continue
                candidate = path + [nxt]
                if destination:
                    if nxt == destination:
                        results.append(candidate)
                    elif len(candidate) - 1 < max_legs:
                        next_frontier.append(candidate)
                else:
                    results.append(candidate)
                    if len(candidate) - 1 < max_legs:
                        next_frontier.append(candidate)
                if len(results) >= max_paths:
                    return results[:max_paths]
        frontier = next_frontier
        if not frontier:
            break
    return results[:max_paths]


def _flight_options(db, pdf_run_id: str, origin: str, destination: str, around: datetime) -> List[Flight]:
    """Load candidate flights on the arrival day and following day from the persisted cache."""
    rows: List[Flight] = []
    for travel_day in (around.date(), around.date() + timedelta(days=1)):
        found = db.get_flights(origin, destination, travel_day, pdf_run_id)
        if found:
            rows.extend(found)
    rows.sort(key=lambda f: f.departure)
    return rows


def _combine_cached_path(
    db,
    pdf_run_id: str,
    path: List[str],
    search_day: date,
    min_transfer_minutes: int,
    max_transfer_minutes: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """Combine cached legs for an arbitrary 1-3 leg simple path."""
    misses = 0
    first = db.get_flights(path[0], path[1], search_day, pdf_run_id)
    if first is None:
        return [], 1
    partials = [[flight] for flight in first]

    for segment in range(1, len(path) - 1):
        origin, destination = path[segment], path[segment + 1]
        expanded: List[List[Flight]] = []
        for legs in partials:
            previous = legs[-1]
            candidates = _flight_options(db, pdf_run_id, origin, destination, previous.arrival)
            # Distinguish an unchecked route/day from a checked route with zero flights.
            if not candidates:
                checked_today = db.get_flights(origin, destination, previous.arrival.date(), pdf_run_id)
                checked_next = db.get_flights(origin, destination, previous.arrival.date() + timedelta(days=1), pdf_run_id)
                if checked_today is None and checked_next is None:
                    misses += 1
            for flight in candidates:
                wait = int((flight.departure - previous.arrival).total_seconds() // 60)
                if min_transfer_minutes <= wait <= max_transfer_minutes:
                    expanded.append(legs + [flight])
        partials = expanded
        if not partials:
            break

    combos: List[Dict[str, Any]] = []
    for legs in partials:
        waits = [int((legs[i + 1].departure - legs[i].arrival).total_seconds() // 60) for i in range(len(legs) - 1)]
        combos.append({
            "path": path,
            "legs": [f.to_dict() for f in legs],
            "connection_minutes": min(waits) if waits else None,
            "connection_minutes_list": waits,
        })
    return combos, misses


def cached_scan_itineraries(
    graph,
    db,
    origin: str,
    destination: Optional[str],
    start_day: date,
    days: int = 4,
    max_stops: int = 1,
    min_transfer_minutes: int = 120,
    limit: int = 100,
    max_paths_per_day: int = 250,
    pdf_run_id: Optional[str] = None,
    max_transfer_minutes: int = 1080,
) -> Tuple[List[Dict[str, Any]], int]:
    """Search the morning cache, supporting direct, one-stop and two-stop itineraries."""
    if not pdf_run_id:
        latest = db.latest_completed_pdf_run()
        pdf_run_id = latest.get("run_id") if latest else None
    if not pdf_run_id:
        return [], 0

    results: List[Dict[str, Any]] = []
    seen = set()
    misses = 0
    for offset in range(max(1, min(4, int(days)))):
        day = start_day + timedelta(days=offset)
        paths = _graph_paths(graph, origin, destination, day, max_stops, max_paths_per_day)
        for path in paths:
            combos, path_misses = _combine_cached_path(
                db,
                pdf_run_id,
                path,
                day,
                max(120, int(min_transfer_minutes)),
                max(120, int(max_transfer_minutes)),
            )
            misses += path_misses
            for combo in combos:
                signature = tuple((leg["flight_code"], leg["departure"], leg["arrival"]) for leg in combo["legs"])
                if signature in seen:
                    continue
                seen.add(signature)
                combo["date"] = day.isoformat()
                combo["source"] = "morning-cache"
                results.append(combo)
                if len(results) >= limit:
                    results.sort(key=lambda r: r["legs"][0]["departure"])
                    return results, misses
    results.sort(key=lambda r: r["legs"][0]["departure"])
    return results, misses
