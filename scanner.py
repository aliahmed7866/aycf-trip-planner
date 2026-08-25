import glob
import json
import os
import random
import re
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dateutil import parser as dtparser

PRIVATE_PAGE = "https://multipass.wizzair.com/w6/subscriptions/spa/private-page/wallets"
_DYNAMIC_PATTERNS = [
    re.compile(r'"searchFlight":"(https:\\/\\/multipass\.wizzair\.com[^"]+)"'),
    re.compile(r'window\.CVO\.flightSearchUrlJson\s*=\s*"([^"]+)"'),
]
_ROUTES_PATTERNS = [re.compile(r'window\.CVO\.routes\s*=\s*(\[.*?\]);', re.S)]


class WizzSessionExpired(RuntimeError):
    pass


class WizzRateLimited(RuntimeError):
    pass


class WizzIntegrationChanged(RuntimeError):
    pass


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


def _parse_dt(day: str, value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return datetime.fromisoformat(day + "T00:00:00")
    try:
        if re.fullmatch(r"\d{1,2}:\d{2}(?::\d{2})?", raw):
            hh, mm, *ss = raw.split(":")
            return datetime.fromisoformat(
                f"{day}T{int(hh):02d}:{int(mm):02d}:{int(ss[0]) if ss else 0:02d}"
            )
        parsed = dtparser.parse(raw)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except Exception as exc:
        raise ValueError(f"Unsupported Wizz flight time: {raw!r}") from exc


def _retry_after_seconds(response: requests.Response, default: float = 4.0) -> float:
    raw = (response.headers.get("Retry-After") or "").strip()
    if not raw:
        return default
    try:
        return max(0.0, min(20.0, float(raw)))
    except ValueError:
        try:
            when = parsedate_to_datetime(raw)
            now = datetime.now(when.tzinfo) if when.tzinfo else datetime.now()
            return max(0.0, min(20.0, (when - now).total_seconds()))
        except Exception:
            return default


class TTLCache:
    """Thread-safe in-process cache shared across scanner requests."""

    def __init__(self):
        self._lock = threading.Lock()
        self._data: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str):
        with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            expires_at, value = item
            if time.time() >= expires_at:
                self._data.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl_seconds: int):
        with self._lock:
            self._data[key] = (time.time() + max(1, ttl_seconds), value)

    def clear(self):
        with self._lock:
            self._data.clear()


_SHARED_LIVE_CACHE = TTLCache()


class CurrentRouteGraph:
    """Uses only the newest parsed AYCF PDF snapshot, never historical scores."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self._frame_cache: Optional[pd.DataFrame] = None
        self._frame_cache_mtime: Optional[float] = None

    def latest_frame(self) -> pd.DataFrame:
        paths = glob.glob(os.path.join(self.data_dir, "**", "*.csv"), recursive=True)
        if not paths:
            raise FileNotFoundError("No AYCF CSV snapshots found.")
        latest_mtime = max(os.path.getmtime(p) for p in paths)
        if self._frame_cache is not None and self._frame_cache_mtime == latest_mtime:
            return self._frame_cache.copy()

        frames = []
        for path in paths:
            try:
                df = pd.read_csv(path)
                if {"departure_from", "departure_to"}.issubset(df.columns):
                    if "data_generated" in df.columns:
                        df["_generated"] = pd.to_datetime(df["data_generated"], errors="coerce", utc=True)
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

        for column in ("departure_from", "departure_to"):
            out[column] = out[column].astype(str).str.strip()
        out = out[(out["departure_from"] != "") & (out["departure_to"] != "")]
        subset = ["departure_from", "departure_to"]
        if {"availability_start", "availability_end"}.issubset(out.columns):
            subset += ["availability_start", "availability_end"]
        out = out.drop_duplicates(subset=subset)

        self._frame_cache = out.copy()
        self._frame_cache_mtime = latest_mtime
        return out

    def invalidate(self) -> None:
        self._frame_cache = None
        self._frame_cache_mtime = None

    def edges_for_day(self, day: date) -> set[Tuple[str, str]]:
        df = self.latest_frame()
        if "availability_start" in df.columns and "availability_end" in df.columns:
            start = pd.to_datetime(df["availability_start"], errors="coerce", utc=True).dt.date
            end = pd.to_datetime(df["availability_end"], errors="coerce", utc=True).dt.date
            mask = (start.isna() | (start <= day)) & (end.isna() | (end >= day))
            df = df[mask]
        return set(zip(df["departure_from"], df["departure_to"]))

    def cities(self) -> List[str]:
        df = self.latest_frame()
        return sorted(set(df["departure_from"]).union(set(df["departure_to"])))

    def paths(
        self,
        origin: str,
        destination: Optional[str],
        day: date,
        max_stops: int = 1,
        max_paths: int = 250,
    ) -> List[List[str]]:
        edges = self.edges_for_day(day)
        adj: Dict[str, set[str]] = {}
        for a, b in edges:
            adj.setdefault(a, set()).add(b)

        paths: List[List[str]] = []
        direct = sorted(adj.get(origin, set()))
        if destination:
            if destination in direct:
                paths.append([origin, destination])
            if max_stops >= 1:
                for hub in direct:
                    if hub != destination and destination in adj.get(hub, set()):
                        paths.append([origin, hub, destination])
        else:
            for dest in direct:
                if dest != origin:
                    paths.append([origin, dest])
            if max_stops >= 1:
                seen = {tuple(p) for p in paths}
                for hub in direct:
                    for dest in sorted(adj.get(hub, set())):
                        p = (origin, hub, dest)
                        if dest != origin and dest != hub and p not in seen:
                            paths.append(list(p))
                            seen.add(p)
                            if len(paths) >= max_paths:
                                return paths
        return paths[:max_paths]


class WizzAYCFClient:
    def __init__(
        self,
        storage_state: Dict[str, Any],
        cache_ttl: int = 300,
        min_delay: float = 0.8,
        cache: Optional[TTLCache] = None,
    ):
        self.storage_state = storage_state
        self.http = requests.Session()
        self.http.headers.update(
            {
                "User-Agent": os.environ.get(
                    "WIZZ_USER_AGENT",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
                ),
                "Accept": "application/json,text/plain,*/*",
                "Referer": PRIVATE_PAGE,
            }
        )
        for cookie in storage_state.get("cookies", []):
            try:
                kwargs = {"path": cookie.get("path", "/")}
                if cookie.get("domain"):
                    kwargs["domain"] = cookie["domain"]
                self.http.cookies.set(cookie["name"], cookie["value"], **kwargs)
            except Exception:
                pass
        self.cache = cache or _SHARED_LIVE_CACHE
        self.cache_ttl = max(1, int(cache_ttl))
        self.min_delay = max(0.2, float(min_delay))
        self._last_request = 0.0
        self.dynamic_url: Optional[str] = None
        self.station_ids: Dict[str, str] = {}
        self.live_requests = 0

    def _throttle(self):
        wait = self.min_delay + random.uniform(0.10, 0.35) - (time.time() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.time()

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        last_response: Optional[requests.Response] = None
        for attempt in range(2):
            self._throttle()
            self.live_requests += 1
            response = self.http.request(method, url, timeout=25, **kwargs)
            last_response = response
            if response.status_code in (401, 403):
                raise WizzSessionExpired("Wizz session expired or was rejected. Reconnect your Wizz account.")
            if response.status_code == 429:
                if attempt == 0:
                    time.sleep(_retry_after_seconds(response))
                    continue
                raise WizzRateLimited("Wizz rate limit reached. Reduce the scan scope and try again later.")
            if 500 <= response.status_code < 600 and attempt == 0:
                time.sleep(1.5)
                continue
            response.raise_for_status()
            return response
        assert last_response is not None
        last_response.raise_for_status()
        return last_response

    def bootstrap(self) -> Dict[str, Any]:
        response = self._request("GET", PRIVATE_PAGE, allow_redirects=True)
        if "protocol/openid-connect/auth" in response.url or "/login" in response.url.lower():
            raise WizzSessionExpired("Wizz session expired. Reconnect your Wizz account.")
        html = response.text

        dynamic = None
        for pattern in _DYNAMIC_PATTERNS:
            match = pattern.search(html)
            if match:
                dynamic = match.group(1).replace("\\/", "/")
                break
        if not dynamic:
            uuid = re.search(
                r'"searchFlight":"https:\\/\\/multipass\.wizzair\.com[^"]+\\/([^"\\/]+)"',
                html,
            )
            if uuid:
                dynamic = f"https://multipass.wizzair.com/w6/subscriptions/json/availability/{uuid.group(1)}"
        if not dynamic:
            raise WizzIntegrationChanged(
                "Wizz is logged in, but the AYCF availability endpoint could not be discovered. Multipass may have changed its page format."
            )
        self.dynamic_url = dynamic

        routes_obj = None
        for pattern in _ROUTES_PATTERNS:
            match = pattern.search(html)
            if match:
                try:
                    routes_obj = json.loads(match.group(1))
                except Exception:
                    routes_obj = None
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

    @staticmethod
    def _flight_rows(payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        candidates = [payload]
        for key in ("data", "result", "availability"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                candidates.append(nested)
        for obj in candidates:
            for key in ("flightsOutbound", "outboundFlights", "flights"):
                rows = obj.get(key)
                if isinstance(rows, list):
                    return [row for row in rows if isinstance(row, dict)]
        return []

    def check(self, origin: str, destination: str, day: date) -> List[Flight]:
        key = f"{origin.casefold()}|{destination.casefold()}|{day.isoformat()}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        if not self.dynamic_url:
            self.bootstrap()
        origin_id = self.resolve_station(origin)
        destination_id = self.resolve_station(destination)
        payload = {"flightType": "OW", "origin": origin_id, "destination": destination_id, "departure": day.isoformat(), "arrival": "", "intervalSubtype": None}
        response = self._request(
            "POST",
            self.dynamic_url,
            json=payload,
            headers={"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )
        try:
            data = response.json()
        except ValueError as exc:
            raise WizzIntegrationChanged("Wizz returned a non-JSON response for AYCF availability.") from exc

        flights: List[Flight] = []
        for row in self._flight_rows(data):
            dep_raw = row.get("departure") or row.get("departureDate") or row.get("departureTime") or ""
            arr_raw = row.get("arrival") or row.get("arrivalDate") or row.get("arrivalTime") or ""
            try:
                dep = _parse_dt(day.isoformat(), dep_raw)
                arr = _parse_dt(day.isoformat(), arr_raw)
            except ValueError:
                continue
            if arr < dep:
                arr += timedelta(days=1)
            flights.append(
                Flight(
                    origin=origin,
                    destination=destination,
                    flight_code=str(row.get("flightCode") or row.get("flightNumber") or ""),
                    departure=dep,
                    arrival=arr,
                    departure_text=str(dep_raw),
                    arrival_text=str(arr_raw),
                    duration=str(row.get("duration") or ""),
                )
            )
        flights.sort(key=lambda f: f.departure)
        self.cache.set(key, flights, self.cache_ttl)
        return flights


def combine_path(client: WizzAYCFClient, path: List[str], day: date, min_transfer_minutes: int = 150, max_transfer_hours: int = 18) -> List[Dict[str, Any]]:
    if len(path) == 2:
        return [{"path": path, "legs": [f.to_dict()]} for f in client.check(path[0], path[1], day)]
    if len(path) != 3:
        return []
    first = client.check(path[0], path[1], day)
    second = client.check(path[1], path[2], day) + client.check(path[1], path[2], day + timedelta(days=1))
    minimum = timedelta(minutes=min_transfer_minutes)
    maximum = timedelta(hours=max_transfer_hours)
    out = []
    for a in first:
        for b in second:
            gap = b.departure - a.arrival
            if minimum <= gap <= maximum:
                out.append({"path": path, "legs": [a.to_dict(), b.to_dict()], "connection_minutes": int(gap.total_seconds() // 60)})
    return out


def scan_itineraries(
    graph: CurrentRouteGraph,
    client: WizzAYCFClient,
    origin: str,
    destination: Optional[str],
    start_day: date,
    days: int = 4,
    max_stops: int = 1,
    min_transfer_minutes: int = 150,
    limit: int = 100,
    max_paths_per_day: int = 250,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    seen = set()
    for offset in range(days):
        day = start_day + timedelta(days=offset)
        for path in graph.paths(origin, destination, day, max_stops=max_stops, max_paths=max_paths_per_day):
            for combo in combine_path(client, path, day, min_transfer_minutes):
                combo["date"] = day.isoformat()
                signature = tuple((leg["flight_code"], leg["departure"], leg["arrival"]) for leg in combo["legs"])
                if signature in seen:
                    continue
                seen.add(signature)
                results.append(combo)
                if len(results) >= limit:
                    results.sort(key=lambda r: r["legs"][0]["departure"])
                    return results
    results.sort(key=lambda r: r["legs"][0]["departure"])
    return results
