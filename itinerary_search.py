"""Build multi-stop AYCF itineraries entirely from the morning SQLite cache."""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from scanner import Flight
from scan_scope import AIRPORT_GROUPS, normalize_name


def _graph_paths(graph, origin: str, destination: Optional[str], day: date, max_stops: int, edges=None, approved_hubs=None):
    """Enumerate simple paths with direct routes first, bounded to at most two stops."""
    max_stops = max(0, min(2, int(max_stops)))
    max_legs = max_stops + 1
    edges = graph.edges_for_day(day) if edges is None else edges
    approved = None if approved_hubs is None else {normalize_name(x) for x in approved_hubs}
    adj: Dict[str, List[str]] = {}
    for a, b in edges:
        adj.setdefault(a, []).append(b)
    for node in adj:
        adj[node] = sorted(set(adj[node]))

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
                        yield candidate
                    elif len(candidate) - 1 < max_legs and (approved is None or normalize_name(nxt) in approved):
                        next_frontier.append(candidate)
                else:
                    yield candidate
                    if len(candidate) - 1 < max_legs and (approved is None or normalize_name(nxt) in approved):
                        next_frontier.append(candidate)
        frontier = next_frontier
        if not frontier:
            break



def _flight_options(db, pdf_run_id: str, origin: str, destination: str, around: datetime) -> List[Flight]:
    """Load candidate flights on the arrival day and following day from the persisted cache."""
    rows: List[Flight] = []
    for travel_day in (around.date(), around.date() + timedelta(days=1)):
        found = db.get_flights(origin, destination, travel_day, pdf_run_id)
        if found:
            rows.extend(found)
    rows.sort(key=lambda f: f.departure)
    return rows


def _same_physical_connection(previous: Flight, following: Flight, logical_hub: str) -> bool:
    """Require a self-transfer to stay at the same concrete airport when known.

    The route graph may use a grouped city such as ``London`` while persisted
    flights carry concrete airports such as London Gatwick or London Luton.
    Combining different concrete airports would silently invent a cross-city
    airport transfer, so reject it. Legacy rows have only the logical hub on
    both sides; those remain usable but explicitly ambiguous in the UI.
    """
    previous_airport = (previous.destination or "").strip()
    following_airport = (following.origin or "").strip()
    logical = (logical_hub or "").strip()
    if not previous_airport or not following_airport:
        return True
    if previous_airport.casefold() == following_airport.casefold():
        return True
    # If either side is still the legacy logical label, there is not enough
    # historical information to prove an airport mismatch. Do not guess.
    if previous_airport.casefold() == logical.casefold() or following_airport.casefold() == logical.casefold():
        return True
    return False


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
                if not _same_physical_connection(previous, flight, origin):
                    continue
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
    approved_hubs=None,
    max_journey_minutes: int = 0,
    requested_origins=None,
    requested_destinations=None,
) -> Tuple[List[Dict[str, Any]], int]:
    """Search cached 1-3 leg journeys, applying eligibility before truncation.

    max_paths_per_day counts paths with eligible flights, so empty, overlong,
    unapproved, or wrong-airport candidates cannot hide valid alternatives.
    """
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
        # Cache checks include preferred reverse legs and explicit watches absent
        # from the PDF. Restrict them to this run; flight lookup enforces dates.
        edges = set(graph.edges_for_day(day))
        if hasattr(db, "checked_routes"):
            edges.update(db.checked_routes(pdf_run_id))
        paths = _graph_paths(graph, origin, destination, day, max_stops,
                             edges=edges, approved_hubs=approved_hubs)
        eligible_paths = 0
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
            path_eligible = False
            for combo in combos:
                legs = combo["legs"]
                if not endpoint_matches(legs[0]["origin"], requested_origins):
                    continue
                if not endpoint_matches(legs[-1]["destination"], requested_destinations):
                    continue
                duration = (datetime.fromisoformat(legs[-1]["arrival"]) - datetime.fromisoformat(legs[0]["departure"])).total_seconds() / 60
                if max_journey_minutes and duration > max_journey_minutes:
                    continue
                path_eligible = True
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
            if path_eligible:
                eligible_paths += 1
                if eligible_paths >= max_paths_per_day:
                    break
    results.sort(key=lambda r: r["legs"][0]["departure"])
    return results, misses

def endpoint_matches(actual, requested):
    """A concrete airport selection must have concrete cached evidence."""
    if not requested:
        return True
    actual = normalize_name(actual)
    for name in requested:
        wanted = normalize_name(name)
        if actual == wanted:
            return True
        for group, members in AIRPORT_GROUPS.items():
            if wanted == normalize_name(group) and actual in {normalize_name(x) for x in members}:
                return True
    return False
