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
VALID_DESTINATION_MODES = {"all", "only", "exclude"}
AIRPORT_GROUPS = {"london": ["London Gatwick", "London Luton", "London Stansted"]}


def normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


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


def default_scope() -> dict:
    return {
        "origins": list(DEFAULT_ORIGINS),
        "destination_mode": "all",
        "destinations": [],
        "connection_hubs": list(DEFAULT_HUBS),
    }


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
    return {"origins": origins, "destination_mode": mode, "destinations": destinations, "connection_hubs": hubs}


def save_scope(origins: Iterable[str], destination_mode: str, destinations: Iterable[str], connection_hubs: Iterable[str] = ()) -> dict:
    mode = str(destination_mode or "all").strip().lower()
    if mode not in VALID_DESTINATION_MODES:
        raise ValueError("Invalid destination mode")
    scope = {
        "origins": _clean_names(origins),
        "destination_mode": mode,
        "destinations": _clean_names(destinations),
        "connection_hubs": _clean_names(connection_hubs),
    }
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


def origin_options(pdf_origins: Iterable[str]) -> list[str]:
    """Expand generic PDF city labels into selectable concrete airports."""
    out = []
    for origin in pdf_origins:
        key = normalize_name(origin)
        members = AIRPORT_GROUPS.get(key)
        if members:
            out.extend(members)
        else:
            out.append(origin)
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
    selected = []
    for origin, destination in route_pairs:
        if not origin_variants(origin, scope):
            continue
        if not _destination_matches(destination, scope):
            continue
        selected.append((origin, destination))
    return sorted(set(selected))


def expand_scan_routes(route_pairs: Iterable[tuple[str, str]], scope: dict) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Return priority UK/direct routes plus eligible onward hub routes.

    A configured hub is expanded only when that hub is directly reachable from
    the selected origin tier in the current PDF. This keeps request volume
    bounded while still caching the second leg needed for one-stop itineraries.
    """
    all_pairs = sorted(set(route_pairs))
    primary = filter_routes(all_pairs, scope)
    reachable = {normalize_name(destination) for _, destination in primary}
    configured = {normalize_name(hub) for hub in scope.get("connection_hubs") or []}
    active_hubs = reachable & configured
    hub_routes = []
    for origin, destination in all_pairs:
        if normalize_name(origin) not in active_hubs:
            continue
        if not _destination_matches(destination, scope):
            continue
        hub_routes.append((origin, destination))
    primary_set = set(primary)
    hub_routes = sorted(set(hub_routes) - primary_set)
    return primary, hub_routes


def scan_plan(route_pairs: Iterable[tuple[str, str]], scope: dict, days: int = 4, seconds_per_request: float = 1.25) -> dict:
    primary, hubs = expand_scan_routes(route_pairs, scope)
    checks = (len(primary) + len(hubs)) * max(1, int(days))
    estimated_seconds = int(round(checks * max(0.2, float(seconds_per_request))))
    return {
        "primary_routes": primary,
        "hub_routes": hubs,
        "routes": primary + hubs,
        "primary_count": len(primary),
        "hub_count": len(hubs),
        "route_count": len(primary) + len(hubs),
        "checks": checks,
        "estimated_seconds": estimated_seconds,
        "estimated_minutes": max(1, round(estimated_seconds / 60)) if checks else 0,
    }


def scope_summary(scope: dict) -> str:
    origins = ", ".join(scope.get("origins") or [])
    mode = scope.get("destination_mode") or "all"
    destinations = ", ".join(scope.get("destinations") or [])
    hubs = ", ".join(scope.get("connection_hubs") or []) or "none"
    if mode == "only":
        base = f"{origins} → only {destinations}"
    elif mode == "exclude":
        base = f"{origins} → all except {destinations}"
    else:
        base = f"{origins} → all PDF destinations"
    return f"{base}; onward hubs: {hubs}"
