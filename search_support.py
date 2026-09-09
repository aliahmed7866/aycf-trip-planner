"""Shared search defaults, validation and presentation for both HTTP endpoints."""
import os
from datetime import datetime

from flask import request
from scan_scope import AIRPORT_GROUPS, normalize_name
from scanner import CurrentRouteGraph

DEFAULT_MAX_STOPS = 2
DEFAULT_MIN_TRANSFER = 120
DEFAULT_MAX_LAYOVER = 48 * 60
DEFAULT_MAX_JOURNEY = 0


def bounded_int(value, default, minimum, maximum):
    try:
        value = int(value)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def form_int(name, default, minimum, maximum):
    return bounded_int(request.form.get(name), default, minimum, maximum)


def env_int(name, default, minimum, maximum):
    return bounded_int(os.environ.get(name), default, minimum, maximum)


def canonical_city(graph: CurrentRouteGraph, value: str):
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


def approved_connections(items, scope):
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


def decorate_itineraries(items, max_journey_minutes=0):
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
        waits = item.get("connection_minutes_list")
        if not isinstance(waits, list):
            waits = []
            for previous, following in zip(legs, legs[1:]):
                try:
                    wait = datetime.fromisoformat(following["departure"]) - datetime.fromisoformat(previous["arrival"])
                    waits.append(max(0, int(wait.total_seconds() // 60)))
                except Exception:
                    waits.append(0)
        connections = []
        for idx, minutes in enumerate(waits):
            connections.append({"hub": path[idx + 1] if idx + 1 < len(path) - 1 else "", "minutes": int(minutes), "risky": 120 <= int(minutes) < 150})
        row = dict(item)
        row.update({
            "origin": path[0], "destination": path[-1],
            "hubs": path[1:-1], "hub": " + ".join(path[1:-1]),
            "stop_count": max(0, len(legs) - 1), "is_direct": len(legs) == 1,
            "total_minutes": total_minutes, "connections": connections,
            "connection_minutes_list": waits,
            "connection_minutes": min(waits) if waits else None,
            "risky_connection": any(c["risky"] for c in connections),
            "departure_time": first.get("departure", "")[11:16],
            "arrival_time": last.get("arrival", "")[11:16],
        })
        out.append(row)
    out.sort(key=lambda r: (r["stop_count"], r["risky_connection"], r.get("total_minutes", 0), r["legs"][0].get("departure", "")))
    return out


def append_unique(target, seen, items):
    for item in items:
        sig = tuple((leg.get("flight_code"), leg.get("departure"), leg.get("arrival")) for leg in item.get("legs") or [])
        if sig and sig not in seen:
            seen.add(sig)
            target.append(item)
