"""Persisted scan scope shared by the Termux UI and morning worker."""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Iterable

DEFAULT_ORIGINS = ["Liverpool", "Leeds/Bradford", "Birmingham", "London Gatwick", "London Luton", "London Stansted"]
DEFAULT_HUBS = ["Budapest", "Warsaw", "Bucharest", "Krakow", "Katowice"]
DEFAULT_WORKERS = 3
VALID_DESTINATION_MODES = {"all", "only", "exclude"}
AIRPORT_GROUPS = {"london": ["London Gatwick", "London Luton", "London Stansted"]}

# Destinations that are often materially more expensive than short intra-EU
# trips and are therefore useful to surface early in a bounded AYCF scan.
# Keep this city-based because the AYCF PDF exposes airport/city labels rather
# than country metadata.
HIGH_VALUE_DESTINATIONS = {
    "amman",
    "aqaba",
    "kutaisi",
    "tbilisi",
    "baku",
    "yerevan",
    "alexandria",
    "cairo",
    "giza sphinx",
    "sphinx",
    "hurghada",
    "sharm el sheikh",
}


def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def is_high_value_destination(name: str) -> bool:
    """Return whether an AYCF city label belongs to the high-value shortlist."""
    key = normalize_name(name)
    return key in HIGH_VALUE_DESTINATIONS or any(token in key for token in ("sharm el sheikh", "sphinx"))


def is_high_value_route(origin: str, destination: str) -> bool:
    """Prioritise both outbound and reverse legs touching a high-value city."""
    return is_high_value_destination(origin) or is_high_value_destination(destination)


def config_dir() -> Path:
    return Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf")))


def scope_path() -> Path:
    return config_dir() / "scan_scope.json"


def _clean_names(values: Iterable[str]) -> list[str]:
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


def default_scope() -> dict:
    return {"origins": list(DEFAULT_ORIGINS), "destination_mode": "all", "destinations": [], "connection_hubs": list(DEFAULT_HUBS), "workers": DEFAULT_WORKERS}


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
    return {"origins": origins, "destination_mode": mode, "destinations": destinations, "connection_hubs": hubs, "workers": _workers(data.get("workers", DEFAULT_WORKERS))}


def save_scope(origins: Iterable[str], destination_mode: str, destinations: Iterable[str], connection_hubs: Iterable[str] = (), workers: int = DEFAULT_WORKERS) -> dict:
    mode = str(destination_mode or "all").strip().lower()
    if mode not in VALID_DESTINATION_MODES:
        raise ValueError("Invalid destination mode")
    scope = {"origins": _clean_names(origins), "destination_mode": mode, "destinations": _clean_names(destinations), "connection_hubs": _clean_names(connection_hubs), "workers": _workers(workers)}
    if not scope["origins"]:
        raise ValueError("Select at least one origin airport")
    if mode == "only" and not scope["destinations"]:
        raise ValueError("Choose at least one destination when using Only selected destinations")
    config_dir().mkdir(parents=True, exist_ok=True)
    target = scope_path()
    temp = target.with_suffix(".tmp")
    temp.write_text(json.dumps(scope, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.chmod(temp, 0o600)
    temp.replace(target)
    return scope


def scope_fingerprint(scope: dict) -> str:
    canonical = {
        "origins": sorted(normalize_name(x) for x in scope.get("origins") or []),
        "destination_mode": scope.get("destination_mode") or "all",
        "destinations": sorted(normalize_name(x) for x in scope.get("destinations") or []),
        "connection_hubs": sorted(normalize_name(x) for x in scope.get("connection_hubs") or []),
        "route_policy": "bidirectional-v1",
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
    """Expand grouped UK airport labels on either side of a live request."""
    variants = origin_variants(name, scope)
    return variants or [name]


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
        return not hit
    return True


def filter_routes(route_pairs: Iterable[tuple[str, str]], scope: dict) -> list[tuple[str, str]]:
    return sorted({(origin, destination) for origin, destination in route_pairs if origin_variants(origin, scope) and _destination_matches(destination, scope)})


def expand_scan_routes(route_pairs: Iterable[tuple[str, str]], scope: dict) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Build a bounded two-way cache around selected UK bases and approved hubs.

    Forward routes remain the priority. Whenever the PDF contains the reverse
    route, we include it too. This makes return searches and inbound hub legs
    available from SQLite without expanding into unrelated network pairs.
    """
    all_pairs = sorted(set(route_pairs))
    pair_set = set(all_pairs)
    configured = {normalize_name(hub) for hub in scope.get("connection_hubs") or []}
    primary_forward = set(filter_routes(all_pairs, scope))
    mode = scope.get("destination_mode") or "all"
    excluded = {normalize_name(x) for x in scope.get("destinations") or []} if mode == "exclude" else set()

    ingress = {
        (origin, destination)
        for origin, destination in all_pairs
        if origin_variants(origin, scope)
        and normalize_name(destination) in configured
        and normalize_name(destination) not in excluded
    }
    primary_forward.update(ingress)
    active_hubs = {normalize_name(destination) for _, destination in ingress}

    hub_forward = {
        (origin, destination)
        for origin, destination in all_pairs
        if normalize_name(origin) in active_hubs and _destination_matches(destination, scope)
    }
    hub_forward -= primary_forward

    primary_reverse = {(b, a) for a, b in primary_forward if (b, a) in pair_set}
    hub_reverse = {(b, a) for a, b in hub_forward if (b, a) in pair_set}

    primary = primary_forward | primary_reverse
    hubs = (hub_forward | hub_reverse) - primary
    return sorted(primary), sorted(hubs)


def scan_plan(route_pairs: Iterable[tuple[str, str]], scope: dict, days: int = 4, seconds_per_request: float = 1.25) -> dict:
    primary, hubs = expand_scan_routes(route_pairs, scope)
    day_count = max(1, int(days))
    checks = (len(primary) + len(hubs)) * day_count
    request_units = 0
    for origin, destination in primary + hubs:
        request_units += len(airport_variants(origin, scope)) * len(airport_variants(destination, scope))
    request_units *= day_count
    workers = _workers(scope.get("workers", DEFAULT_WORKERS))
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
    if mode == "only":
        base = f"{origins} ↔ only {destinations}"
    elif mode == "exclude":
        base = f"{origins} ↔ all except {destinations}"
    else:
        base = f"{origins} ↔ all PDF destinations"
    return f"{base}; two-way via hubs: {hubs}; workers: {_workers(scope.get('workers', DEFAULT_WORKERS))}"
