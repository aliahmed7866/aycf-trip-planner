"""Build multi-stop AYCF itineraries entirely from the morning SQLite cache."""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from scanner import Flight
from search_cache import SearchCache
from scan_scope import endpoint_matches as physical_endpoint_matches
from scan_scope import AIRPORT_GROUPS, normalize_name, endpoint_excluded, journey_hubs


def _graph_paths(adj, origin: str, destination: Optional[str], max_stops: int, approved_hubs=None):
    """Enumerate simple paths with direct routes first, bounded to at most two stops."""
    max_stops = max(0, min(2, int(max_stops)))
    max_legs = max_stops + 1
    approved = None if approved_hubs is None else list(approved_hubs)
    def permitted_hub(name):
        return approved is None or any(physical_endpoint_matches(name, hub) for hub in approved)
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
                    elif len(candidate) - 1 < max_legs and permitted_hub(nxt):
                        next_frontier.append(candidate)
                else:
                    yield candidate
                    if len(candidate) - 1 < max_legs and permitted_hub(nxt):
                        next_frontier.append(candidate)
        frontier = next_frontier
        if not frontier:
            break



def _flight_options(db, pdf_run_id: str, origin: str, destination: str, around: datetime,
                    min_transfer_minutes=120, max_transfer_minutes=48 * 60) -> List[Flight]:
    """Load onward flights across this run's cached dates, including long stopovers."""
    rows: List[Flight] = []
    earliest = around + timedelta(minutes=min_transfer_minutes)
    latest = around + timedelta(minutes=max_transfer_minutes) if max_transfer_minutes else None
    window_end = getattr(db, "end", None)
    if window_end:
        # Inspect every eligible date, not just completed checks. This preserves
        # partial flights and records genuine gaps even when another date has seats.
        last_day = min(window_end, latest.date()) if latest else window_end
        first_day = max(earliest.date(), getattr(db, "start", None) or earliest.date())
        travel_days = (first_day + timedelta(days=i) for i in range((last_day - first_day).days + 1))
    elif hasattr(db, "searchable_route_days"):
        travel_days = db.searchable_route_days(pdf_run_id, origin, destination)
    elif hasattr(db, "checked_route_days"):
        travel_days = db.checked_route_days(pdf_run_id, origin, destination)
    else:
        travel_days = (around.date(), around.date() + timedelta(days=1))
    for travel_day in travel_days:
        if travel_day < earliest.date() or (latest and travel_day > latest.date()):
            continue
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


def _combine_cached_path(db, run_id, path, search_day, min_transfer_minutes, max_transfer_minutes):
    """Yield in flight order so the caller can stop once enough eligible trips exist."""
    def extend(legs):
        if len(legs) == len(path) - 1:
            waits = [int((nxt.departure - prev.arrival).total_seconds() // 60)
                     for prev, nxt in zip(legs, legs[1:])]
            yield {"path": path, "legs": [flight.to_dict() for flight in legs],
                   "connection_minutes": min(waits) if waits else None,
                   "connection_minutes_list": waits}
            return
        previous = legs[-1]
        origin, dest = path[len(legs):len(legs) + 2]
        for flight in _flight_options(db, run_id, origin, dest, previous.arrival,
                                      min_transfer_minutes, max_transfer_minutes):
            wait = (flight.departure - previous.arrival).total_seconds() / 60
            if wait < min_transfer_minutes or (max_transfer_minutes and wait > max_transfer_minutes):
                continue
            if _same_physical_connection(previous, flight, origin):
                yield from extend(legs + [flight])

    for first in db.get_flights(path[0], path[1], search_day, run_id) or ():
        yield from extend([first])


def cached_scan_itineraries(
    graph,
    db,
    origin: str,
    destination: Optional[str],
    start_day: date,
    days: int = 4,
    max_stops: int = 2,
    min_transfer_minutes: int = 120,
    limit: int = 100,
    max_paths_per_day: int = 250,
    pdf_run_id: Optional[str] = None,
    max_transfer_minutes: int = 48 * 60,
    approved_hubs=None,
    max_journey_minutes: int = 0,
    requested_origins=None,
    requested_destinations=None,
    scope=None,
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

    if scope is not None and approved_hubs is not None:
        # Keep a caller's hub restriction; add only the explicit transit choices.
        approved_hubs = list(approved_hubs) + journey_hubs(dict(scope, connection_hubs=[]))

    if not isinstance(db, SearchCache):
        db = SearchCache(db, pdf_run_id)
    elif db.run_id != pdf_run_id:
        db = SearchCache(db.db, pdf_run_id)
    results: List[Dict[str, Any]] = []
    seen = set()
    for offset in range(max(1, min(4, int(days)))):
        day = start_day + timedelta(days=offset)
        if not db.in_window(day):
            continue
        # Cache checks include preferred reverse legs and explicit watches absent
        # from the PDF. Restrict them to this run; flight lookup enforces dates.
        adjacency = db.adjacency(graph, day, scope)
        paths = _graph_paths(adjacency, origin, destination, max_stops, approved_hubs)
        eligible_paths = 0
        for path in paths:
            if scope is not None and any(endpoint_excluded(name, scope) for name in (path[0], path[-1])):
                continue
            combos = _combine_cached_path(
                db,
                pdf_run_id,
                path,
                day,
                max(120, int(min_transfer_minutes)),
                max(120, int(max_transfer_minutes)) if max_transfer_minutes else 0,
            )
            path_eligible = False
            for combo in combos:
                legs = combo["legs"]
                if scope is not None and any(endpoint_excluded(name, scope) for name in (legs[0]['origin'], legs[-1]['destination'])):
                    continue
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
                    return results, len(db.missing)
            if path_eligible:
                eligible_paths += 1
                if eligible_paths >= max_paths_per_day:
                    break
    results.sort(key=lambda r: r["legs"][0]["departure"])
    return results, len(db.missing)

def endpoint_matches(actual, requested):
    """A concrete airport selection must have concrete cached evidence."""
    if not requested:
        return True
    return any(physical_endpoint_matches(actual, wanted) for wanted in requested)
