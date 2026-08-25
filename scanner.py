import glob
import json
import os
import random
import re
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

PRIVATE_PAGE = "https://multipass.wizzair.com/w6/subscriptions/spa/private-page/wallets"
_DYNAMIC_PATTERNS = [
    re.compile(r'"searchFlight":"(https:\\/\\/multipass\.wizzair\.com[^"]+)"'),
    re.compile(r'window\.CVO\.flightSearchUrlJson\s*=\s*"([^"]+)"'),
]
_ROUTES_PATTERNS = [re.compile(r'window\.CVO\.routes\s*=\s*(\[.*?\]);', re.S)]


@dataclass(frozen=True)
class Flight:
    origin: str
    destination: str
    flight_code: str
    departure: datetime
    arrival: datetime
    departure_text: str
    arrival_text: str
    duration: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "origin": self.origin,
            "destination": self.destination,
            "flight_code": self.flight_code,
            "departure": self.departure.isoformat(),
            "arrival": self.arrival.isoformat(),
            "departure_text": self.departure_text,
            "arrival_text": self.arrival_text,
            "duration": self.duration,
        }


def _parse_dt(day: str, value: str) -> datetime:
    value = (value or "").strip()
    if not value:
        return datetime.fromisoformat(day + "T00:00:00")
    if "T" in value or ("-" in value[:10] and " " in value):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    return datetime.fromisoformat(f"{day}T{value[:5]}:00")


class TTLCache:
    def __init__(self, ttl_seconds: int = 300):
        self.ttl = ttl_seconds
        self._lock = threading.Lock()
        self._data: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str):
        with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            ts, value = item
            if time.time() - ts > self.ttl:
                self._data.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any):
        with self._lock:
            self._data[key] = (time.time(), value)


class CurrentRouteGraph:
    """Uses the newest parsed AYCF PDF snapshot, never historical frequency scores."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    def latest_frame(self) -> pd.DataFrame:
        paths = glob.glob(os.path.join(self.data_dir, "**", "*.csv"), recursive=True)
        if not paths:
            raise FileNotFoundError("No AYCF CSV snapshots found.")
        frames = []
        for path in paths:
            try:
                df = pd.read_csv(path)
                if {"departure_from", "departure_to"}.issubset(df.columns):
                    if "data_generated" in df.columns:
                        df["_generated"] = pd.to_datetime(df["data_generated"], errors="coerce")
                    else:
                        df["_generated"] = pd.NaT
                    frames.append(df)
            except Exception:
                continue
        if not frames:
            raise ValueError("No valid AYCF snapshot CSVs found.")
        all_df = pd.concat(frames, ignore_index=True)
        if all_df["_generated"].notna().any():
            newest = all_df["_generated"].max()
            out = all_df[all_df["_generated"] == newest].copy()
        else:
            newest_path = max(paths, key=os.path.getmtime)
            out = pd.read_csv(newest_path)
        for c in ("departure_from", "departure_to"):
            out[c] = out[c].astype(str).str.strip()
        return out

    def edges_for_day(self, day: date) -> set[Tuple[str, str]]:
        df = self.latest_frame()
        if "availability_start" in df.columns and "availability_end" in df.columns:
            start = pd.to_datetime(df["availability_start"], errors="coerce").dt.date
            end = pd.to_datetime(df["availability_end"], errors="coerce").dt.date
            mask = (start.isna() | (start <= day)) & (end.isna() | (end >= day))
            df = df[mask]
        return set(zip(df["departure_from"], df["departure_to"]))

    def cities(self) -> List[str]:
        df = self.latest_frame()
        return sorted(set(df["departure_from"]).union(set(df["departure_to"])))

    def paths(self, origin: str, destination: Optional[str], day: date, max_stops: int = 1) -> List[List[str]]:
        edges = self.edges_for_day(day)
        adj: Dict[str, set[str]] = {}
        for a, b in edges:
            adj.setdefault(a, set()).add(b)
        paths: List[List[str]] = []
        if destination:
            if destination in adj.get(origin, set()):
                paths.append([origin, destination])
            if max_stops >= 1:
                for hub in sorted(adj.get(origin, set())):
                    if hub != destination and destination in adj.get(hub, set()):
                        paths.append([origin, hub, destination])
        else:
            for dest in sorted(adj.get(origin, set())):
                paths.append([origin, dest])
            if max_stops >= 1:
                seen = {tuple(p) for p in paths}
                for hub in sorted(adj.get(origin, set())):
                    for dest in sorted(adj.get(hub, set())):
                        p = (origin, hub, dest)
                        if dest != origin and dest != hub and p not in seen:
                            paths.append(list(p))
        return paths


class WizzAYCFClient:
    def __init__(self, storage_state: Dict[str, Any], cache_ttl: int = 300, min_delay: float = 0.8):
        self.storage_state = storage_state
        self.http = requests.Session()
        self.http.headers.update({
            "User-Agent": os.environ.get("WIZZ_USER_AGENT", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36"),
            "Accept": "application/json,text/plain,*/*",
            "Referer": PRIVATE_PAGE,
        })
        for cookie in storage_state.get("cookies", []):
            try:
                self.http.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain"), path=cookie.get("path", "/"))
            except Exception:
                pass
        self.cache = TTLCache(cache_ttl)
        self.min_delay = min_delay
        self._last_request = 0.0
        self._request_count = 0
        self.dynamic_url: Optional[str] = None
        self.station_ids: Dict[str, str] = {}

    def _throttle(self):
        if self._request_count and self._request_count % 25 == 0:
            time.sleep(float(os.environ.get("AYCF_BATCH_COOLDOWN_SECONDS", "15")))
        wait = self.min_delay + random.uniform(0.15, 0.45) - (time.time() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.time()
        self._request_count += 1

    def bootstrap(self) -> Dict[str, Any]:
        self._throttle()
        r = self.http.get(PRIVATE_PAGE, timeout=25, allow_redirects=True)
        if "protocol/openid-connect/auth" in r.url or "/login" in r.url.lower():
            raise RuntimeError("Wizz session expired. Reconnect your Wizz account.")
        r.raise_for_status()
        html = r.text
        dynamic = None
        for p in _DYNAMIC_PATTERNS:
            m = p.search(html)
            if m:
                dynamic = m.group(1).replace("\\/", "/")
                break
        if not dynamic:
            uuid = re.search(r'"searchFlight":"https:\\/\\/multipass\.wizzair\.com[^"]+\\/([^"\\/]+)"', html)
            if uuid:
                dynamic = f"https://multipass.wizzair.com/w6/subscriptions/json/availability/{uuid.group(1)}"
        if not dynamic:
            raise RuntimeError("Could not discover the AYCF availability endpoint from Wizz.")
        self.dynamic_url = dynamic

        routes_obj = None
        for p in _ROUTES_PATTERNS:
            m = p.search(html)
            if m:
                try:
                    routes_obj = json.loads(m.group(1))
                except Exception:
                    pass
                break
        if routes_obj:
            for route in routes_obj:
                stations = [route.get("departureStation")] + list(route.get("arrivalStations") or [])
                for station in stations:
                    if not isinstance(station, dict) or not station.get("id"):
                        continue
                    sid = str(station["id"]).upper()
                    self.station_ids[sid.casefold()] = sid
                    for key in ("name", "shortName", "city", "displayName", "nameWithCountry"):
                        val = station.get(key)
                        if val:
                            self.station_ids[str(val).strip().casefold()] = sid
        return {"ok": True, "endpoint_found": True, "stations": len(set(self.station_ids.values()))}

    def resolve_station(self, name: str) -> str:
        if not self.dynamic_url:
            self.bootstrap()
        raw = (name or "").strip()
        if len(raw) == 3 and raw.isalpha():
            return raw.upper()
        sid = self.station_ids.get(raw.casefold())
        if sid:
            return sid
        aliases = {
            "london luton": "LTN", "london": "LTN", "liverpool": "LPL", "budapest": "BUD",
            "bucharest": "OTP", "warsaw": "WAW", "kutaisi": "KUT", "yerevan": "EVN",
            "abu dhabi": "AUH", "dubai": "DWC", "amman": "AMM", "hurghada": "HRG",
            "sharm el-sheikh": "SSH", "gdansk": "GDN", "krakow": "KRK", "katowice": "KTW",
            "birmingham": "BHX", "leeds/bradford": "LBA",
        }
        if raw.casefold() in aliases:
            return aliases[raw.casefold()]
        raise RuntimeError(f"Could not map '{raw}' to a Wizz airport code.")

    def check(self, origin: str, destination: str, day: date) -> List[Flight]:
        key = f"{origin}|{destination}|{day.isoformat()}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        if not self.dynamic_url:
            self.bootstrap()
        origin_id = self.resolve_station(origin)
        destination_id = self.resolve_station(destination)
        payload = {"flightType": "OW", "origin": origin_id, "destination": destination_id, "departure": day.isoformat(), "arrival": "", "intervalSubtype": None}
        self._throttle()
        r = self.http.post(self.dynamic_url, json=payload, headers={"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}, timeout=25)
        if r.status_code in (401, 403):
            raise RuntimeError("Wizz rejected the saved session. Reconnect your Wizz account.")
        if r.status_code == 429:
            raise RuntimeError("Wizz rate limit reached. Try again after a short break.")
        r.raise_for_status()
        data = r.json()
        rows = data.get("flightsOutbound") or []
        flights: List[Flight] = []
        for row in rows:
            dep_raw = row.get("departure") or row.get("departureDate") or ""
            arr_raw = row.get("arrival") or ""
            dep = _parse_dt(day.isoformat(), dep_raw)
            arr = _parse_dt(day.isoformat(), arr_raw)
            if arr < dep:
                arr += timedelta(days=1)
            flights.append(Flight(origin=origin, destination=destination, flight_code=str(row.get("flightCode") or ""), departure=dep, arrival=arr, departure_text=str(dep_raw), arrival_text=str(arr_raw), duration=str(row.get("duration") or "")))
        self.cache.set(key, flights)
        return flights


def combine_path(client: WizzAYCFClient, path: List[str], day: date, min_transfer_minutes: int = 150) -> List[Dict[str, Any]]:
    if len(path) == 2:
        return [{"path": path, "legs": [f.to_dict()]} for f in client.check(path[0], path[1], day)]
    if len(path) != 3:
        return []
    first = client.check(path[0], path[1], day)
    second = client.check(path[1], path[2], day) + client.check(path[1], path[2], day + timedelta(days=1))
    minimum = timedelta(minutes=min_transfer_minutes)
    maximum = timedelta(hours=18)
    out = []
    for a in first:
        for b in second:
            gap = b.departure - a.arrival
            if minimum <= gap <= maximum:
                out.append({"path": path, "legs": [a.to_dict(), b.to_dict()], "connection_minutes": int(gap.total_seconds() // 60)})
    return out


def scan_itineraries(graph: CurrentRouteGraph, client: WizzAYCFClient, origin: str, destination: Optional[str], start_day: date, days: int = 4, max_stops: int = 1, min_transfer_minutes: int = 150, limit: int = 100) -> List[Dict[str, Any]]:
    results = []
    for offset in range(days):
        day = start_day + timedelta(days=offset)
        for path in graph.paths(origin, destination, day, max_stops=max_stops):
            for combo in combine_path(client, path, day, min_transfer_minutes):
                combo["date"] = day.isoformat()
                results.append(combo)
                if len(results) >= limit:
                    return results
    results.sort(key=lambda r: r["legs"][0]["departure"])
    return results
