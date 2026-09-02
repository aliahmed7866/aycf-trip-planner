"""Keep airport identity separate from city-level historical archive evidence."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

CITY_AIRPORTS = {
    "London": ("London Gatwick", "London Luton", "London Stansted"),
}
AIRPORT_CITY = {airport: city for city, airports in CITY_AIRPORTS.items() for airport in airports}


def archive_name(name: str) -> str:
    """Return the coarser place name used by the historical archive."""
    return AIRPORT_CITY.get(name, name)


def archive_pair(origin: str, destination: str) -> tuple[str, str]:
    return archive_name(origin), archive_name(destination)


def is_airport_specific(name: str) -> bool:
    return name in AIRPORT_CITY


def resolve_airport_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Attach shared city history only to airport routes supported by local scans.

    Exact rows already exist when current PDF topology caused the scanner to
    check that airport route. We never fan a city-level archive row out to every
    airport, because that would fabricate route-level precision.
    """
    source = [dict(row) for row in rows]
    by_pair = {(r.get("origin"), r.get("destination")): r for r in source}
    output: List[Dict[str, Any]] = []
    for row in source:
        origin, destination = row.get("origin", ""), row.get("destination", "")
        shared_key = archive_pair(origin, destination)
        shared = by_pair.get(shared_key) if shared_key != (origin, destination) else None
        item = dict(row)
        item["archive_origin"], item["archive_destination"] = shared_key
        item["airport_specific"] = is_airport_specific(origin) or is_airport_specific(destination)
        if shared and item.get("archive_score") is None and shared.get("archive_score") is not None:
            for key in ("archive", "archive_score", "recent_30d", "previous_30d", "trend"):
                item[key] = shared.get(key)
            item["historical_scope"] = "London-wide"
        elif item["airport_specific"] and shared_key != (origin, destination):
            item["historical_scope"] = "London-wide" if item.get("archive_score") is not None else None
        else:
            item["historical_scope"] = None
        if item["airport_specific"]:
            if int(item.get("positive_checks") or 0) > 0:
                item["airport_evidence"] = "Observed in local AYCF scans"
            elif int(item.get("total_checks") or 0) > 0:
                item["airport_evidence"] = "Supported by current scanned topology"
            else:
                item["airport_evidence"] = "Airport not yet confirmed"
        else:
            item["airport_evidence"] = None
        output.append(item)
    return output


def route_archive_fallback(origin: str, destination: str) -> Optional[tuple[str, str]]:
    shared = archive_pair(origin, destination)
    return shared if shared != (origin, destination) else None

