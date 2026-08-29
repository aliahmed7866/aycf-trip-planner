import csv
import glob
import json
import os
import threading
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dateutil import parser as dtparser

REQUIRED_COLS = {"departure_from", "departure_to"}
DEFAULT_BASES = ["Liverpool", "London Luton", "Birmingham", "Leeds/Bradford"]
DEFAULT_HUBS = ["Bucharest", "Budapest", "Warsaw", "Gdansk", "Krakow", "Katowice", "Liverpool", "London Luton"]
DEFAULT_TARGETS = ["Kutaisi", "Yerevan", "Amman", "Dubai", "Abu Dhabi", "Hurghada", "Sharm el-Sheikh", "Tel Aviv", "Marrakech"]
CITY_ALIASES = {"London Luton": "London", "London (Luton)": "London"}


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


class RouteCountRows(list):
    """List result with temporary pandas-style iterrows compatibility."""
    def iterrows(self):
        for index, row in enumerate(self):
            yield index, row


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
        self._city_cache_path = os.path.join(self.data_dir, ".aycf-city-options.json")
        self._route_cache_dir = os.path.join(self.data_dir, ".aycf-index")
        self._index_warming = False
        self._index_lock = threading.Lock()

    def _csv_paths(self) -> List[str]:
        return sorted(glob.glob(os.path.join(self.data_dir, "**", "*.csv"), recursive=True))

    def _dataset_fingerprint(self) -> Dict[str, Any]:
        paths = self._csv_paths()
        latest_mtime_ns = 0
        total_size = 0
        for path in paths:
            try:
                stat = os.stat(path)
                latest_mtime_ns = max(latest_mtime_ns, stat.st_mtime_ns)
                total_size += stat.st_size
            except OSError:
                continue
        return {"file_count": len(paths), "latest_mtime_ns": latest_mtime_ns, "total_size": total_size}

    def _read_city_cache(self, validate: bool = False) -> tuple[List[str], bool]:
        try:
            with open(self._city_cache_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            cities = payload.get("cities") if isinstance(payload, dict) else None
            if not isinstance(cities, list) or not cities:
                return [], True
            if not validate:
                return sorted({str(city) for city in cities if str(city).strip()}), False
            stale = (payload.get("fingerprint") or {}) != self._dataset_fingerprint()
            return sorted({str(city) for city in cities if str(city).strip()}), stale
        except Exception:
            return [], True

    def _write_city_cache(self, cities: List[str], fingerprint: Optional[Dict[str, Any]] = None) -> None:
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            payload = {"cities": sorted({str(city) for city in cities if str(city).strip()}), "fingerprint": fingerprint or self._dataset_fingerprint()}
            tmp = self._city_cache_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, self._city_cache_path)
        except Exception:
            pass

    def _route_cache_path(self, lookback_days: int) -> str:
        return os.path.join(self._route_cache_dir, f"route-counts-{int(lookback_days)}.json")

    def _read_route_cache(self, lookback_days: int, fingerprint: Dict[str, Any]) -> Optional[RouteCountRows]:
        try:
            with open(self._route_cache_path(lookback_days), "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if payload.get("fingerprint") != fingerprint or not isinstance(payload.get("rows"), list):
                return None
            return RouteCountRows(payload["rows"])
        except Exception:
            return None

    def _write_route_cache(self, lookback_days: int, rows: List[Dict[str, Any]], fingerprint: Dict[str, Any]) -> None:
        try:
            os.makedirs(self._route_cache_dir, exist_ok=True)
            path = self._route_cache_path(lookback_days)
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump({"fingerprint": fingerprint, "rows": rows}, handle, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, path)
        except Exception:
            pass

    def _load_runs(self) -> List[Dict[str, Any]]:
        paths = self._csv_paths()
        if not paths:
            raise FileNotFoundError(f"No CSV runs found in {self.data_dir}. Set AYCF_DATA_DIR to the repo's data folder.")
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
        parsed_end = _safe_parse_dt(end_date) if end_date else None
        end = (parsed_end + timedelta(days=1)) if parsed_end else datetime.now() + timedelta(days=1)
        return [r for r in rows if r.get("run_ts") is None or (start <= r["run_ts"] < end)]

    def _filter_by_lookback(self, rows: List[Dict[str, Any]], lookback_days: int) -> List[Dict[str, Any]]:
        cutoff = datetime.now() - timedelta(days=int(lookback_days))
        return [r for r in rows if r.get("run_ts") is None or r["run_ts"] >= cutoff]

    def _counts_from_rows(self, rows: List[Dict[str, Any]], lookback_days: int) -> RouteCountRows:
        filtered = self._filter_by_lookback(rows, lookback_days)
        counts = Counter((r["departure_from"], r["departure_to"]) for r in filtered)
        result = RouteCountRows({"departure_from": a, "departure_to": b, "appearances": n} for (a, b), n in counts.items())
        result.sort(key=lambda r: r["appearances"], reverse=True)
        return result

    def route_counts(self, lookback_days: int) -> RouteCountRows:
        lookback_days = int(lookback_days)
        fingerprint = self._dataset_fingerprint()
        cached = self._read_route_cache(lookback_days, fingerprint)
        if cached is not None:
            return cached
        rows = self._load_runs()
        result = self._counts_from_rows(rows, lookback_days)
        self._write_route_cache(lookback_days, result, fingerprint)
        cities = sorted({c for row in rows for c in (row.get("departure_from"), row.get("departure_to")) if c})
        self._write_city_cache(cities, fingerprint)
        return result

    def _warm_indexes_background(self) -> None:
        with self._index_lock:
            if self._index_warming:
                return
            self._index_warming = True
        def worker() -> None:
            try:
                fingerprint = self._dataset_fingerprint()
                if self._read_route_cache(180, fingerprint) is not None:
                    return
                rows = self._load_runs()
                cities = sorted({c for row in rows for c in (row.get("departure_from"), row.get("departure_to")) if c})
                self._write_city_cache(cities, fingerprint)
                for days in (180, 365):
                    self._write_route_cache(days, self._counts_from_rows(rows, days), fingerprint)
            except Exception:
                pass
            finally:
                with self._index_lock:
                    self._index_warming = False
        threading.Thread(target=worker, name="aycf-index-warm", daemon=True).start()

    def suggest_itineraries(self, lookback_days: int, min_transfer_minutes: int, start_date: Optional[str], end_date: Optional[str], bases: List[str], hubs: List[str], targets: List[str], require_return_to_base: bool, top_n: int = 25) -> List[Dict[str, Any]]:
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
                    return_candidates = [(rh, freq) for rh, freq in return_candidates if freq > 0] or [(hub, 0)]
                    for return_hub, target_to_hub in return_candidates:
                        hub_to_base = route_map.get((return_hub, base), 0)
                        if require_return_to_base and hub_to_base <= 0:
                            continue
                        score = float(base_to_hub) + float(hub_to_target) + 1.2 * float(target_to_hub) + (0.8 if require_return_to_base else 0.3) * float(hub_to_base)
                        suggestions.append(Suggestion(base, hub, target, return_hub, base_to_hub, hub_to_target, target_to_hub, hub_to_base, score))
        suggestions.sort(key=lambda s: s.score, reverse=True)
        return [s.to_dict() for s in suggestions[:top_n]]

    def city_options(self, lookback_days: int = 365):
        cities, _ = self._read_city_cache(validate=False)
        if cities:
            return cities
        rows = self._load_runs()
        fingerprint = self._dataset_fingerprint()
        cities = sorted({c for row in rows for c in (row.get("departure_from"), row.get("departure_to")) if c})
        self._write_city_cache(cities, fingerprint)
        return cities

    def top_cities(self, lookback_days: int = 365, top_n: int = 80):
        totals = Counter()
        for row in self.route_counts(lookback_days):
            appearances = int(row["appearances"])
            totals[row["departure_from"]] += appearances
            totals[row["departure_to"]] += appearances
        return [city for city, _ in totals.most_common(top_n)]

    def ui_defaults(self) -> Dict[str, Any]:
        # GET / must never walk or parse the historical CSV dataset.
        cached_cities, _ = self._read_city_cache(validate=False)
        all_cities = cached_cities or sorted(set(DEFAULT_BASES + DEFAULT_HUBS + DEFAULT_TARGETS))
        self._warm_indexes_background()
        return {
            "base_options": DEFAULT_BASES,
            "hub_options": all_cities,
            "target_options": all_cities,
            "default_bases": ["Liverpool", "London Luton"],
            "default_hubs": DEFAULT_HUBS,
            "default_targets": [],
        }
