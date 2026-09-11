"""Persisted scan scope shared by the Termux UI and morning worker."""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Iterable
from datetime import date

from airport_catalog import airport_code, country_for

DEFAULT_ORIGINS = ["Liverpool", "Leeds/Bradford", "Birmingham", "London Gatwick", "London Luton", "London Stansted"]
DEFAULT_HUBS = ["Bucharest", "Budapest", "Rome", "Milan Malpensa", "Warsaw", "Gdansk", "Krakow", "Katowice"]
DEFAULT_WORKERS = 3
VALID_DESTINATION_MODES = {"all", "only", "exclude"}
AIRPORT_GROUPS = {"london": ["London Gatwick", "London Luton", "London Stansted"]}

# These endpoints receive full PDF-backed coverage in either direction, not just
# UK-origin/hub coverage. A route is still never invented: it must exist in the
# current AYCF PDF. This gives persistent coverage for the regions where AYCF
# availability is most valuable/volatile.
SPECIAL_COVERAGE_ENDPOINTS = {
    # UAE
    "abu dhabi", "dubai", "sharjah", "ras al khaimah", "ras al-khaimah",
    # Egypt
    "alexandria", "cairo", "giza", "giza sphinx", "sphinx", "hurghada", "sharm el sheikh", "sharm el-sheikh",
    # Jordan
    "amman", "aqaba",
    # Georgia / Armenia / Azerbaijan
    "kutaisi", "tbilisi", "batumi", "yerevan", "baku",
    # Saudi / Gulf long-haul priorities
    "jeddah", "riyadh", "dammam", "medina", "madinah", "kuwait city", "kuwait",
}

PRIMARY_PRIORITY_DESTINATIONS = set(SPECIAL_COVERAGE_ENDPOINTS)
SECONDARY_PRIORITY_DESTINATIONS = {
    "belgrade", "pristina", "skopje", "ohrid", "sofia", "varna", "burgas", "tirana",
    "oslo", "oslo torp", "bergen", "tromso", "aalesund", "alesund",
    "reykjavik", "reykjavik keflavik", "keflavik",
}
HIGH_VALUE_DESTINATIONS = PRIMARY_PRIORITY_DESTINATIONS | SECONDARY_PRIORITY_DESTINATIONS


@lru_cache(maxsize=8192)
def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _priority_match(key: str, candidates: set[str]) -> bool:
    if key in candidates:
        return True
    aliases = {
        "sharm el sheikh": ("sharm el sheikh",),
        "giza": ("giza", "sphinx"),
        "reykjavik": ("reykjavik", "keflavik"),
        "oslo": ("oslo",),
        "ras al khaimah": ("ras al khaimah",),
    }
    for canonical, tokens in aliases.items():
        if canonical in candidates and any(token in key for token in tokens):
            return True
    return False


def is_special_coverage_endpoint(name: str) -> bool:
    return _priority_match(normalize_name(name), SPECIAL_COVERAGE_ENDPOINTS)


def destination_priority(name: str, scope: dict | None = None) -> int:
    """Return priority: 0 UI-selected, 1 long-haul, 2 regional, 3 normal, 4 excluded."""
    if normalize_name(name) in AIRPORT_GROUPS:
        return min((destination_priority(item, scope) for item in airport_variants(name, scope or {})), default=4)
    key = normalize_name(name)
    selected = {normalize_name(x) for x in (scope or {}).get("destinations") or []}
    preferred = {normalize_name(x) for x in (scope or {}).get("preferred_destinations") or []}
    mode = str((scope or {}).get("destination_mode") or "all")
    if endpoint_excluded(name, scope or {}):
        return 4
    if any(endpoint_matches(name, item) for item in preferred) or (mode != "exclude" and any(endpoint_matches(name, item) for item in selected)):
        return 0
    if _priority_match(key, PRIMARY_PRIORITY_DESTINATIONS):
        return 1
    if _priority_match(key, SECONDARY_PRIORITY_DESTINATIONS):
        return 2
    return 3


def route_priority(origin: str, destination: str, scope: dict | None = None) -> int:
    requests = route_requests(origin, destination, scope or {})
    if not requests:
        return 4
    for pair in (scope or {}).get("watch_routes") or []:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        a, b = pair
        if any(endpoint_matches(first, a) and endpoint_matches(second, b)
               for first, second in requests):
            return 0
    return min(min(destination_priority(a, scope), destination_priority(b, scope)) for a, b in requests)


def is_high_value_destination(name: str) -> bool:
    return destination_priority(name) <= 2


def is_high_value_route(origin: str, destination: str) -> bool:
    return route_priority(origin, destination) <= 2


def config_dir() -> Path:
    return Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf")))


def scope_path() -> Path:
    return config_dir() / "scan_scope.json"


def _clean_names(values: Iterable[str]) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        values = []
    out, seen = [], set()
    for value in values:
        item = str(value or "").strip()
        key = normalize_name(item)
        if not item or not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _workers(value) -> int:
    try:
        value = int(value)
    except (TypeError, ValueError):
        value = DEFAULT_WORKERS
    return max(1, min(5, value))


def configured_workers(scope: dict) -> int:
    fallback = _workers(scope.get("workers", DEFAULT_WORKERS))
    try:
        return _workers(int(os.environ.get("AYCF_SCAN_WORKERS", fallback)))
    except (TypeError, ValueError):
        return fallback


def default_scope() -> dict:
    return {"origins": list(DEFAULT_ORIGINS), "destination_mode": "all", "destinations": [], "connection_hubs": list(DEFAULT_HUBS), "workers": DEFAULT_WORKERS, "excluded_airports": [], "excluded_countries": [], "excluded_routes": []}


def load_scope() -> dict:
    data = {}
    try:
        data = json.loads(scope_path().read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        pass
    if not isinstance(data, dict):
        data = {}
    origins = _clean_names(data.get("origins") or DEFAULT_ORIGINS) or list(DEFAULT_ORIGINS)
    mode = str(data.get("destination_mode") or "all").strip().lower()
    if mode not in VALID_DESTINATION_MODES:
        mode = "all"
    destinations = _clean_names(data.get("destinations") or [])
    hubs = _clean_names(data.get("connection_hubs") if "connection_hubs" in data else DEFAULT_HUBS)
    return {"origins": origins, "destination_mode": mode, "destinations": destinations, "connection_hubs": hubs, "workers": _workers(data.get("workers", DEFAULT_WORKERS)), **clean_exclusions(data)}


def save_scope(origins: Iterable[str], destination_mode: str, destinations: Iterable[str], connection_hubs: Iterable[str] = (), workers: int = DEFAULT_WORKERS, *, excluded_airports=None, excluded_countries=None, excluded_routes=None) -> dict:
    mode = str(destination_mode or "all").strip().lower()
    if mode not in VALID_DESTINATION_MODES:
        raise ValueError("Invalid destination mode")
    scope = {"origins": _clean_names(origins), "destination_mode": mode, "destinations": _clean_names(destinations), "connection_hubs": _clean_names(connection_hubs), "workers": _workers(workers)}
    existing = load_scope()
    scope.update(clean_exclusions({key: existing.get(key, []) if value is None else value for key, value in {
        "excluded_airports": excluded_airports, "excluded_countries": excluded_countries, "excluded_routes": excluded_routes,
    }.items()}))
    if not scope["origins"]:
        raise ValueError("Select at least one origin airport")
    if mode == "only" and not scope["destinations"]:
        raise ValueError("Choose at least one destination when using Only selected destinations")
    config_dir().mkdir(parents=True, exist_ok=True)
    target = scope_path()
    fd, name = tempfile.mkstemp(prefix=".scan-scope-", suffix=".tmp", dir=config_dir())
    temp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(scope, indent=2, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temp.replace(target)
    finally:
        temp.unlink(missing_ok=True)
    return scope


def scan_run_id(generated, scope: dict, routes) -> str | None:
    """Version the shared PDF/scope identity so older unvalidated empty checks are not reused."""
    if not generated or not routes:
        return None
    generated_text = generated.isoformat() if hasattr(generated, "isoformat") else str(generated)
    payload = "validated-availability-v2\n" + generated_text + "\n" + scope_fingerprint(scope) + "\n" + "\n".join(f"{a}>{b}" for a, b in routes)
    return hashlib.sha256(payload.encode()).hexdigest()[:20]


def scope_fingerprint(scope: dict) -> str:
    canonical = {
        "origins": sorted(normalize_name(x) for x in scope.get("origins") or []),
        "destination_mode": scope.get("destination_mode") or "all",
        "destinations": sorted(normalize_name(x) for x in scope.get("destinations") or []),
        "connection_hubs": sorted(normalize_name(x) for x in scope.get("connection_hubs") or []),
        "preferred_destinations": sorted(normalize_name(x) for x in scope.get("preferred_destinations") or []),
        "watch_routes": sorted({
            f"{normalize_name(route[0])}>{normalize_name(route[1])}"
            for route in scope.get("watch_routes") or []
            if isinstance(route, (list, tuple)) and len(route) == 2
        }),
        "exclusions": {
            "airports": sorted({endpoint_key(x) for x in scope.get("excluded_airports") or []}),
            "countries": sorted({normalize_name(x) for x in scope.get("excluded_countries") or []}),
            "routes": sorted({tuple(sorted(endpoint_key(x) for x in pair)) for pair in clean_exclusions(scope)["excluded_routes"]}),
        },
        "route_policy": "pdf-exclusions-v6",
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def origin_variants(origin: str, scope: dict) -> list[str]:
    origin_key = normalize_name(origin)
    selected = {normalize_name(x): x for x in scope.get("origins") or []}
    if origin_key in selected:
        return [selected[origin_key]]
    members = AIRPORT_GROUPS.get(origin_key, [])
    return [member for member in members if normalize_name(member) in selected]


def airport_variants(name: str, scope: dict) -> list[str]:
    variants = origin_variants(name, scope)
    if not variants:
        variants = AIRPORT_GROUPS.get(normalize_name(name), [name])
    return [item for item in variants if not endpoint_excluded(item, scope)]


def clean_exclusions(scope: dict) -> dict:
    routes = set()
    raw_routes = scope.get("excluded_routes") or []
    if not isinstance(raw_routes, (tuple, list)):
        raw_routes = []
    for pair in raw_routes:
        if isinstance(pair, (tuple, list)) and len(pair) == 2:
            a, b = (str(value or "").strip() for value in pair)
            if a and b and normalize_name(a) != normalize_name(b):
                routes.add(tuple(sorted((a, b), key=normalize_name)))
    return {
        "excluded_airports": sorted(_clean_names(scope.get("excluded_airports") or []), key=normalize_name),
        "excluded_countries": sorted(_clean_names(scope.get("excluded_countries") or []), key=normalize_name),
        "excluded_routes": [list(pair) for pair in sorted(routes)],
    }


def exclusions_fingerprint(scope: dict) -> str:
    exclusions = clean_exclusions(scope)
    if scope.get("destination_mode") == "exclude":
        exclusions["excluded_airports"] += _clean_names(scope.get("destinations") or [])
    return scope_fingerprint(exclusions)


def endpoint_key(name: str) -> str:
    """Stable physical-airport identity, keeping city groups separate."""
    return (airport_code(name) or normalize_name(name)).casefold()


@lru_cache(maxsize=32768)
def endpoint_matches(actual: str, selected: str) -> bool:
    """A selected city group matches its members; individual airports stay exact."""
    if normalize_name(actual) == normalize_name(selected):
        return True
    members = AIRPORT_GROUPS.get(normalize_name(selected))
    if members:
        return any(endpoint_matches(actual, member) for member in members)
    code = airport_code(actual)
    return bool(code and code == airport_code(selected))


def endpoint_excluded(name: str, scope: dict) -> bool:
    excluded = list(scope.get("excluded_airports") or [])
    if scope.get("destination_mode") == "exclude":
        excluded += list(scope.get("destinations") or [])
    return (any(endpoint_matches(name, item) for item in excluded)
            or normalize_name(country_for(name)) in {normalize_name(x) for x in scope.get("excluded_countries") or []})


def concrete_route_allowed(origin: str, destination: str, scope: dict) -> bool:
    if endpoint_excluded(origin, scope) or endpoint_excluded(destination, scope):
        return False
    for pair in scope.get("excluded_routes") or []:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        a, b = pair
        if ((endpoint_matches(origin, a) and endpoint_matches(destination, b))
                or (endpoint_matches(origin, b) and endpoint_matches(destination, a))):
            return False
    return True


def route_requests(origin: str, destination: str, scope: dict) -> list[tuple[str, str]]:
    """The exact allowed airport requests, shared by estimates and both workers."""
    return [(a, b) for a in airport_variants(origin, scope) for b in airport_variants(destination, scope)
            if normalize_name(a) != normalize_name(b) and concrete_route_allowed(a, b, scope)]


def route_allowed(origin: str, destination: str, scope: dict) -> bool:
    return bool(route_requests(origin, destination, scope))


def scan_jobs(plan: dict, scope: dict, days) -> list:
    """Nearest date, user priority, regional priority, then base/hub and name."""
    jobs = []
    for tier, routes in (("primary", plan["primary_routes"]), ("hub", plan["hub_routes"])):
        for origin, destination in routes:
            requests = route_requests(origin, destination, scope)
            if not requests:
                continue
            for day in days:
                jobs.append((tier, origin, destination, day,
                             sorted({a for a, _ in requests}), sorted({b for _, b in requests}), requests))
    jobs.sort(key=lambda job: (job[3], route_priority(job[1], job[2], scope),
                               0 if origin_variants(job[2], scope) else 1 if origin_variants(job[1], scope) else 2,
                               job[0] != "primary",
                               normalize_name(job[1]), normalize_name(job[2])))
    return jobs


def scan_window(frame) -> dict:
    """Use the same inclusive PDF travel window as the workers; label fallback."""
    if frame is not None and len(frame) and {"availability_start", "availability_end"}.issubset(frame.columns):
        try:
            start = date.fromisoformat(str(frame["availability_start"].iloc[0])[:10])
            end = date.fromisoformat(str(frame["availability_end"].iloc[0])[:10])
            if end >= start:
                return {"days": (end - start).days + 1, "label": f"{start.isoformat()} to {end.isoformat()}", "estimated": False}
        except (ValueError, TypeError):
            pass
    return {"days": 4, "label": "estimated four-day window", "estimated": True}


def origin_options(pdf_origins: Iterable[str]) -> list[str]:
    out = []
    for origin in pdf_origins:
        members = AIRPORT_GROUPS.get(normalize_name(origin))
        out.extend(members if members else [origin])
    return sorted(_clean_names(out))


def _destination_equivalents(destination: str) -> set[str]:
    key = normalize_name(destination)
    equivalents = {key}
    for group, members in AIRPORT_GROUPS.items():
        member_keys = {normalize_name(x) for x in members}
        if key == group or key in member_keys:
            equivalents.add(group)
            equivalents.update(member_keys)
    return equivalents


def _destination_matches(destination: str, scope: dict) -> bool:
    mode = scope.get("destination_mode") or "all"
    wanted = {normalize_name(x) for x in scope.get("destinations") or []}
    hit = bool(_destination_equivalents(destination) & wanted)
    if mode == "only":
        return hit
    if mode == "exclude":
        return bool(airport_variants(destination, scope))
    return True


def filter_routes(route_pairs: Iterable[tuple[str, str]], scope: dict) -> list[tuple[str, str]]:
    return sorted({(origin, destination) for origin, destination in route_pairs if origin_variants(origin, scope) and _destination_matches(destination, scope) and route_allowed(origin, destination, scope)})


def _topology_backed_two_way(routes: Iterable[tuple[str, str]]) -> set[tuple[str, str]]:
    """Return both directions for edges proven by at least one current-PDF leg."""
    edges = set(routes)
    return edges | {(destination, origin) for origin, destination in edges}


def expand_scan_routes(route_pairs: Iterable[tuple[str, str]], scope: dict) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Build bounded scan coverage from current-PDF topology.

    Normal coverage follows selected UK origins and configured hubs. Selected UK, reachable hub, preferred
    and actively watched edges are allowed in both directions when at least one
    direction is present in the current PDF. This avoids gaps caused by the PDF
    publishing only one side of an otherwise queryable directional route while
    still refusing to invent unrelated network edges.
    """
    all_pairs = sorted({(a, b) for a, b in route_pairs if route_allowed(a, b, scope)})
    pair_set = set(all_pairs)
    configured = {normalize_name(hub) for hub in scope.get("connection_hubs") or []}
    primary_forward = set(filter_routes(all_pairs, scope))

    # Full, topology-backed special coverage in both available directions.
    special_pairs = {(a, b) for a, b in all_pairs if is_special_coverage_endpoint(a) or is_special_coverage_endpoint(b)}
    primary_forward.update(special_pairs)

    # Recommendation preferences broaden only the topology needed for a direct
    # UK trip or a one-stop trip through an approved hub. Once either direction
    # proves an edge exists, scan both directional AYCF availability queries.
    preferred = [item for item in scope.get("preferred_destinations") or [] if not endpoint_excluded(item, scope)]

    def is_uk_endpoint(name: str) -> bool:
        return bool(origin_variants(name, scope))

    def is_preferred_endpoint(name: str, other: str) -> bool:
        return any(endpoint_matches(airport, item) for airport, _ in route_requests(name, other, scope) for item in preferred)

    uk_connected_hubs, preferred_connected_hubs = set(), set()
    for first, second in all_pairs:
        first_key, second_key = normalize_name(first), normalize_name(second)
        if is_uk_endpoint(first) and second_key in configured:
            uk_connected_hubs.add(second_key)
        if is_uk_endpoint(second) and first_key in configured:
            uk_connected_hubs.add(first_key)
        if is_preferred_endpoint(first, second) and second_key in configured:
            preferred_connected_hubs.add(second_key)
        if is_preferred_endpoint(second, first) and first_key in configured:
            preferred_connected_hubs.add(first_key)
    preferred_hubs = uk_connected_hubs & preferred_connected_hubs

    preferred_direct = _topology_backed_two_way({
        (first, second) for first, second in all_pairs
        if (is_uk_endpoint(first) and is_preferred_endpoint(second, first))
        or (is_uk_endpoint(second) and is_preferred_endpoint(first, second))
    })
    preferred_uk_hub = _topology_backed_two_way({
        (first, second) for first, second in all_pairs
        if (is_uk_endpoint(first) and normalize_name(second) in preferred_hubs)
        or (is_uk_endpoint(second) and normalize_name(first) in preferred_hubs)
    })
    preferred_hub_legs = _topology_backed_two_way({
        (first, second) for first, second in all_pairs
        if (is_preferred_endpoint(first, second) and normalize_name(second) in preferred_hubs)
        or (is_preferred_endpoint(second, first) and normalize_name(first) in preferred_hubs)
    })

    # An enabled route watch is also a promise that the morning scan will cover
    # that direction. Accept it only when the current PDF proves the undirected
    # edge; this keeps typo/stale watches from expanding into arbitrary routes.
    watched_routes = set()
    for requested in scope.get("watch_routes") or []:
        if not isinstance(requested, (list, tuple)) or len(requested) != 2:
            continue
        requested_origin, requested_destination = requested
        if not route_allowed(requested_origin, requested_destination, scope):
            continue
        def matches_watch(first, second):
            return any(endpoint_matches(a, requested_origin) and endpoint_matches(b, requested_destination)
                       for a, b in route_requests(first, second, scope))
        for first, second in all_pairs:
            if matches_watch(first, second):
                watched_routes.add((first, second))
            elif matches_watch(second, first):
                watched_routes.add((second, first))

    # One published direction is evidence of a route worth checking in both
    # directions, not evidence that seats are available on its reverse. Apply
    # the same bounded policy to ordinary short trips as to preferred trips.
    two_way_pairs = _topology_backed_two_way(all_pairs)
    uk_direct = {
        (first, second) for first, second in two_way_pairs
        if (is_uk_endpoint(first) and _destination_matches(second, scope))
        or (is_uk_endpoint(second) and _destination_matches(first, scope))
    }
    primary_forward.update(uk_direct | preferred_direct | preferred_uk_hub | watched_routes)

    mode = scope.get("destination_mode") or "all"
    excluded = {normalize_name(x) for x in scope.get("destinations") or []} if mode == "exclude" else set()
    ingress = {
        (origin, destination)
        for origin, destination in two_way_pairs
        if origin_variants(origin, scope)
        and normalize_name(destination) in configured
        and normalize_name(destination) not in excluded
    }
    primary_forward.update(_topology_backed_two_way(ingress))
    active_hubs = {normalize_name(destination) for _, destination in ingress}
    hub_forward = _topology_backed_two_way({(origin, destination) for origin, destination in two_way_pairs if normalize_name(origin) in active_hubs and _destination_matches(destination, scope)})
    hub_forward.update(preferred_hub_legs)
    hub_forward -= primary_forward

    primary_reverse = {(b, a) for a, b in primary_forward if (b, a) in pair_set}
    hub_reverse = {(b, a) for a, b in hub_forward if (b, a) in pair_set}
    primary = primary_forward | primary_reverse
    hubs = (hub_forward | hub_reverse) - primary
    # Apply the veto again after preferred/watch/reverse expansion. Nothing may
    # reintroduce a blocked endpoint or physical route.
    return (sorted((a, b) for a, b in primary if route_allowed(a, b, scope)),
            sorted((a, b) for a, b in hubs if route_allowed(a, b, scope)))


def scan_plan(route_pairs: Iterable[tuple[str, str]], scope: dict, days: int = 4, seconds_per_request: float = 1.25) -> dict:
    primary, hubs = expand_scan_routes(route_pairs, scope)
    day_count = max(1, int(days))
    checks = (len(primary) + len(hubs)) * day_count
    request_units = 0
    for origin, destination in primary + hubs:
        request_units += len(route_requests(origin, destination, scope))
    request_units *= day_count
    workers = configured_workers(scope)
    global_interval = max(0.2, float(os.environ.get("AYCF_GLOBAL_REQUEST_INTERVAL", "1.0")))
    serial_seconds = request_units * max(0.2, float(seconds_per_request))
    rate_floor_seconds = request_units * global_interval
    estimated_seconds = int(round(max(rate_floor_seconds, serial_seconds / workers)))
    return {"primary_routes": primary, "hub_routes": hubs, "routes": primary + hubs, "primary_count": len(primary), "hub_count": len(hubs), "route_count": len(primary) + len(hubs), "checks": checks, "request_units": request_units, "workers": workers, "estimated_seconds": estimated_seconds, "estimated_minutes": max(1, round(estimated_seconds / 60)) if request_units else 0}


def scope_summary(scope: dict) -> str:
    origins = ", ".join(scope.get("origins") or [])
    mode = scope.get("destination_mode") or "all"
    destinations = ", ".join(scope.get("destinations") or [])
    hubs = ", ".join(scope.get("connection_hubs") or []) or "none"
    preferred = ", ".join(scope.get("preferred_destinations") or []) or "none"
    if mode == "only":
        base = f"{origins} ↔ only {destinations}"
    elif mode == "exclude":
        base = f"{origins} ↔ all except {destinations}"
    elif destinations:
        base = f"{origins} ↔ all PDF destinations; priority: {destinations}"
    else:
        base = f"{origins} ↔ all PDF destinations"
    watch_count = len(scope.get("watch_routes") or [])
    exclusions = clean_exclusions(scope)
    count = sum(len(items) for items in exclusions.values())
    return f"{base}; preferred two-way endpoints: {preferred}; active watch routes: {watch_count}; priority-region coverage subject to exclusions; two-way via hubs: {hubs}; {count} exclusions (both directions); workers: {configured_workers(scope)}"
