import csv
import glob
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dateutil import parser as dtparser

REQUIRED_COLS = {"departure_from", "departure_to"}

DEFAULT_BASES = ["Liverpool", "London Luton", "Birmingham", "Leeds/Bradford"]
DEFAULT_HUBS = ["Bucharest", "Budapest", "Warsaw", "Gdansk", "Krakow", "Katowice", "Liverpool", "London Luton"]
DEFAULT_TARGETS = [
    "Kutaisi", "Yerevan", "Amman", "Dubai", "Abu Dhabi",
    "Hurghada", "Sharm el-Sheikh", "Tel Aviv", "Marrakech",
]

CITY_ALIASES = {
    "London Luton": "London",
    "London (Luton)": "London",
}


def normalise_city(name: str) -> str:
    name = (name or "").strip()
    return CITY_ALIASES.get(name, name)


def _safe_parse_dt(value: Optional[str]) -> Optional[datetime]:
    if value is None or not str(value).strip():
        return None
    try:
        dt = dtparser.parse(str(value))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


@dataclass
class Suggestion:
    base: str
    hub: str
    target: str
    return_hub: str
    base_to_hub_freq: int
    hub_to_target_freq: int
    target_to_return_hub_freq: int
    return_hub_to_base_freq: int
    score: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "itinerary": f"{self.base} → {self.hub} → {self.target}",
            "return": f"{self.target} → {self.return_hub} → {self.base}",
            "base_to_hub": self.base_to_hub_freq,
            "hub_to_target": self.hub_to_target_freq,
            "target_to_hub": self.target_to_return_hub_freq,
            "hub_to_base": self.return_hub_to_base_freq,
            "score": round(self.score, 2),
        }


class AYCFPlanner:
    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        self.file_count = 0
        self.last_run_count = 0

    def _load_runs(self) -> List[Dict[str, Any]]:
        paths = sorted(glob.glob(os.path.join(self.data_dir, "**", "*.csv"), recursive=True))
        if not paths:
            raise FileNotFoundError(
                f"No CSV runs found in {self.data_dir}. "
                "Set AYCF_DATA_DIR to the repo's data folder."
            )

        rows: List[Dict[str, Any]] = []
        used_files = set()
        for path in paths:
            try:
                with open(path, "r", encoding="utf-8-sig", newline="") as handle:
                    reader = csv.DictReader(handle)
                    if not reader.fieldnames or not REQUIRED_COLS.issubset(reader.fieldnames):
                        continue
                    for raw in reader:
                        departure_from = normalise_city(str(raw.get("departure_from") or "").strip())
                        departure_to = normalise_city(str(raw.get("departure_to") or "").strip())
                        if not departure_from or not departure_to:
                            continue
                        run_value = raw.get("data_generated") or raw.get("run_ts")
                        row = dict(raw)
                        row["departure_from"] = departure_from
                        row["departure_to"] = departure_to
                        row["run_ts"] = _safe_parse_dt(run_value)
                        row["source_file"] = os.path.basename(path)
                        rows.append(row)
                        used_files.add(path)
            except Exception:
                continue

        if not rows:
            raise ValueError("Found CSV files but none with expected columns (departure_from, departure_to).")

        self.file_count = len(paths)
        self.last_run_count = len(used_files)
        return rows

    def _filter_by_date(self, rows: List[Dict[str, Any]], start_date: Optional[str], end_date: Optional[str]) -> List[Dict[str, Any]]:
        start = _safe_parse_dt(start_date) if start_date else datetime.now() - timedelta(days=180)
        end = (_safe_parse_dt(end_date) + timedelta(days=1)) if end_date and _safe_parse_dt(end_date) else datetime.now() + timedelta(days=1)
        return [r for r in rows if r.get("run_ts") is None or (start <= r["run_ts"] < end)]

    def _filter_by_lookback(self, rows: List[Dict[str, Any]], lookback_days: int) -> List[Dict[str, Any]]:
        cutoff = datetime.now() - timedelta(days=int(lookback_days))
        return [r for r in rows if r.get("run_ts") is None or r["run_ts"] >= cutoff]

    def route_counts(self, lookback_days: int) -> List[Dict[str, Any]]:
        rows = self._filter_by_lookback(self._load_runs(), lookback_days)
        counts = Counter((r["departure_from"], r["departure_to"]) for r in rows)
        result = [
            {"departure_from": origin, "departure_to": destination, "appearances": appearances}
            for (origin, destination), appearances in counts.items()
        ]
        return sorted(result, key=lambda r: r["appearances"], reverse=True)

    def suggest_itineraries(
        self,
        lookback_days: int,
        min_transfer_minutes: int,
        start_date: Optional[str],
        end_date: Optional[str],
        bases: List[str],
        hubs: List[str],
        targets: List[str],
        require_return_to_base: bool,
        top_n: int = 25,
    ) -> List[Dict[str, Any]]:
        counts = self.route_counts(lookback_days)
        route_map = {(r["departure_from"], r["departure_to"]): int(r["appearances"]) for r in counts}

        bases_set = {normalise_city(b) for b in bases if str(b).strip()}
        hubs_set = {normalise_city(h) for h in hubs if str(h).strip()}
        targets_set = {normalise_city(t) for t in targets if str(t).strip()}

        suggestions: List[Suggestion] = []
        for base in bases_set:
            for hub in hubs_set:
                base_to_hub = route_map.get((base, hub), 0)
                if not base_to_hub:
                    continue
                for target in targets_set:
                    hub_to_target = route_map.get((hub, target), 0)
                    if not hub_to_target:
                        continue
                    return_candidates = [(rh, route_map.get((target, rh), 0)) for rh in hubs_set]
                    return_candidates = [(rh, freq) for rh, freq in return_candidates if freq > 0]
                    if not return_candidates:
                        return_candidates = [(hub, 0)]

                    for return_hub, target_to_hub in return_candidates:
                        hub_to_base = route_map.get((return_hub, base), 0)
                        if require_return_to_base and hub_to_base <= 0:
                            continue
                        score = (
                            float(base_to_hub)
                            + float(hub_to_target)
                            + 1.2 * float(target_to_hub)
                            + (0.8 if require_return_to_base else 0.3) * float(hub_to_base)
                        )
                        suggestions.append(Suggestion(
                            base=base,
                            hub=hub,
                            target=target,
                            return_hub=return_hub,
                            base_to_hub_freq=base_to_hub,
                            hub_to_target_freq=hub_to_target,
                            target_to_return_hub_freq=target_to_hub,
                            return_hub_to_base_freq=hub_to_base,
                            score=score,
                        ))

        suggestions.sort(key=lambda s: s.score, reverse=True)
        return [s.to_dict() for s in suggestions[:top_n]]

    def city_options(self, lookback_days: int = 365):
        rows = self._filter_by_lookback(self._load_runs(), lookback_days)
        cities = {
            normalise_city(city)
            for row in rows
            for city in (row.get("departure_from"), row.get("departure_to"))
            if city and str(city).strip()
        }
        return sorted(cities)

    def top_cities(self, lookback_days: int = 365, top_n: int = 80):
        totals = Counter()
        for row in self.route_counts(lookback_days):
            appearances = int(row["appearances"])
            totals[row["departure_from"]] += appearances
            totals[row["departure_to"]] += appearances
        return [city for city, _ in totals.most_common(top_n)]

    def ui_defaults(self) -> Dict[str, Any]:
        try:
            all_cities = self.city_options(lookback_days=365)
        except Exception:
            all_cities = DEFAULT_BASES + DEFAULT_HUBS + DEFAULT_TARGETS

        return {
            "base_options": ["Liverpool", "London Luton", "Birmingham", "Leeds/Bradford"],
            "hub_options": all_cities,
            "target_options": all_cities,
            "default_bases": ["Liverpool", "London Luton"],
            "default_hubs": ["Bucharest", "Budapest", "Warsaw", "Gdansk", "Krakow", "Katowice", "Liverpool", "London Luton"],
            "default_targets": ["Kutaisi", "Yerevan", "Amman", "Dubai", "Abu Dhabi", "Hurghada", "Sharm el-Sheikh"],
        }
