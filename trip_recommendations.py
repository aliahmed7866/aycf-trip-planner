"""Fast, explainable monthly and seasonal recommendations from materialized Stability data."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from historical_stability import SOURCE_ID, _connect
from airport_resolution import archive_name, resolve_airport_rows

SEASONS = {
    "winter": (12, 1, 2),
    "spring": (3, 4, 5),
    "summer": (6, 7, 8),
    "autumn": (9, 10, 11),
}


def period_months(month: Optional[int] = None, season: str = "") -> tuple[int, ...]:
    if month is not None:
        value = max(1, min(12, int(month)))
        return (value,)
    return SEASONS.get(str(season or "").strip().lower(), SEASONS["summer"])


def _period_rates(months: Iterable[int], path: Optional[str] = None) -> Dict[tuple[str, str], float]:
    wanted = tuple(sorted({max(1, min(12, int(m))) for m in months}))
    placeholders = ",".join("?" for _ in wanted)
    with _connect(path) as conn:
        total = conn.execute(
            f"SELECT COUNT(*) c FROM external_snapshot_days WHERE source=? AND CAST(substr(snapshot_date,6,2) AS INTEGER) IN ({placeholders})",
            (SOURCE_ID, *wanted),
        ).fetchone()["c"]
        if not total:
            return {}
        rows = conn.execute(
            f"""SELECT origin,destination,COUNT(DISTINCT snapshot_date) seen
                  FROM external_route_appearances
                 WHERE source=? AND CAST(substr(snapshot_date,6,2) AS INTEGER) IN ({placeholders})
                 GROUP BY origin,destination""",
            (SOURCE_ID, *wanted),
        ).fetchall()
    return {(r["origin"], r["destination"]): round(100.0 * int(r["seen"]) / int(total), 1) for r in rows}


def recommend_trips(
    stability_rows: Iterable[Dict[str, Any]],
    uk_origins: Iterable[str],
    hubs: Iterable[str],
    *,
    month: Optional[int] = None,
    season: str = "",
    path: Optional[str] = None,
    limit: int = 80,
    destination_mode: str = "all",
    destinations: Iterable[str] = (),
    origin_filter: str = "",
    trip_type: str = "all",
) -> List[Dict[str, Any]]:
    """Rank direct UK trips and one-stop trips through configured hubs.

    Period score is historical AYCF appearance frequency in the chosen calendar
    month(s). Overall score blends period fit with all-year archive reliability.
    A connection is limited by its weaker leg and receives a small complexity
    penalty, so a fragile second leg cannot hide behind a strong first leg.
    """
    months = period_months(month, season)
    period = _period_rates(months, path)
    rows = resolve_airport_rows(stability_rows)
    by_pair = {(r.get("origin"), r.get("destination")): r for r in rows}
    origins, configured_hubs = set(uk_origins), set(hubs)
    if origin_filter:
        origins = {origin for origin in origins if origin == origin_filter}
    selected_destinations = {archive_name(str(value)) for value in destinations if str(value).strip()}
    mode = destination_mode if destination_mode in {"all", "only", "exclude"} else "all"
    kind = trip_type if trip_type in {"all", "direct", "connected"} else "all"

    def destination_allowed(destination: str) -> bool:
        selected = archive_name(destination) in selected_destinations
        if mode == "only":
            return selected
        if mode == "exclude":
            return not selected
        return True

    def leg(origin: str, destination: str) -> Optional[Dict[str, Any]]:
        row = by_pair.get((origin, destination))
        if not row:
            return None
        archive = row.get("archive_score")
        period_score = period.get((row.get("archive_origin", origin), row.get("archive_destination", destination)))
        if archive is None or period_score is None:
            return None
        score = round(0.65 * float(period_score) + 0.35 * float(archive), 1)
        return {"origin": origin, "destination": destination, "archive_origin": row.get("archive_origin", origin), "archive_destination": row.get("archive_destination", destination), "historical_scope": row.get("historical_scope"), "airport_evidence": row.get("airport_evidence"), "period_score": period_score, "archive_score": archive, "score": score, "trend": row.get("trend", "insufficient")}

    output: List[Dict[str, Any]] = []
    for (origin, destination) in by_pair:
        if origin not in origins or destination in origins or not destination_allowed(destination) or kind == "connected":
            continue
        direct = leg(origin, destination)
        if direct:
            output.append({"origin": origin, "destination": destination, "hub": None, "legs": [direct], "is_direct": True, "score": direct["score"], "period_score": direct["period_score"]})

    seen_connections = set()
    if kind == "direct":
        output.sort(key=lambda r: (-r["score"], -r["period_score"], r["origin"], r["destination"]))
        return output[: max(1, min(int(limit), 200))]
    for origin in origins:
        for hub in configured_hubs:
            first = leg(origin, hub)
            if not first:
                continue
            for (leg_origin, destination) in by_pair:
                if leg_origin != hub or destination in origins or destination == origin or destination == hub or not destination_allowed(destination):
                    continue
                second = leg(hub, destination)
                if not second:
                    continue
                signature = (origin, hub, destination)
                if signature in seen_connections:
                    continue
                seen_connections.add(signature)
                period_score = min(float(first["period_score"]), float(second["period_score"]))
                score = round(max(0.0, min(float(first["score"]), float(second["score"])) - 5.0), 1)
                output.append({"origin": origin, "destination": destination, "hub": hub, "legs": [first, second], "is_direct": False, "score": score, "period_score": period_score, "weakest_leg_score": min(float(first["score"]), float(second["score"]))})

    output.sort(key=lambda r: (-r["score"], 0 if r["is_direct"] else 1, -r["period_score"], r["origin"], r["destination"]))
    return output[: max(1, min(int(limit), 200))]
