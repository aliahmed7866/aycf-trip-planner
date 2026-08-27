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

PRIVATE_PAGE = os.environ.get(
    "WIZZ_PRIVATE_PAGE",
    "https://multipass.wizzair.com/en/w6/subscriptions/spa/private-page/wallets",
)
_DYNAMIC_PATTERNS = [
    re.compile(r'"searchFlight":"(https:\\/\\/multipass\.wizzair\.com[^"]+)"'),
    re.compile(r'window\.CVO\.flightSearchUrlJson\s*=\s*"([^"]+)"'),
]
_ROUTES_PATTERNS = [re.compile(r'window\.CVO\.routes\s*=\s*(\[.*?\]);', re.S)]

# Deterministic aliases are a fallback only. Runtime aliases captured from the
# authenticated Wizz page always take priority. Keep this map local so a missing
# public-map request cannot abort the morning scan.
_STATION_ALIASES = {
    "alghero": "AHO", "belgrade": "BEG", "london luton": "LTN", "london": "LTN",
    "liverpool": "LPL", "budapest": "BUD", "bucharest": "OTP", "bucharest otopeni": "OTP",
    "warsaw": "WAW", "warsaw chopin": "WAW", "kutaisi": "KUT", "yerevan": "EVN",
    "abu dhabi": "AUH", "dubai": "DWC", "dubai world central": "DWC", "amman": "AMM",
    "hurghada": "HRG", "sharm el-sheikh": "SSH", "sharm el sheikh": "SSH", "gdansk": "GDN",
    "krakow": "KRK", "kraków": "KRK", "katowice": "KTW", "birmingham": "BHX",
    "leeds/bradford": "LBA", "leeds bradford": "LBA", "tirana": "TIA", "sofia": "SOF",
    "skopje": "SKP", "sarajevo": "SJJ", "pristina": "PRN", "podgorica": "TGD",
    "zagreb": "ZAG", "split": "SPU", "dubrovnik": "DBV", "ljubljana": "LJU",
    "vienna": "VIE", "bratislava": "BTS", "prague": "PRG", "brno": "BRQ",
    "wroclaw": "WRO", "wrocław": "WRO", "poznan": "POZ", "poznań": "POZ",
    "lodz": "LCJ", "łódź": "LCJ", "rzeszow": "RZE", "rzeszów": "RZE",
    "iasi": "IAS", "iași": "IAS", "cluj-napoca": "CLJ", "cluj napoca": "CLJ",
    "timisoara": "TSR", "timișoara": "TSR", "sibiu": "SBZ", "craiova": "CRA",
    "bacau": "BCM", "bacău": "BCM", "satu mare": "SUJ", "suceava": "SCV",
    "debrecen": "DEB", "thessaloniki": "SKG", "athens": "ATH", "corfu": "CFU",
    "crete heraklion": "HER", "heraklion": "HER", "rhodes": "RHO", "santorini": "JTR",
    "larnaca": "LCA", "paphos": "PFO", "malta": "MLA", "rome fiumicino": "FCO",
    "rome": "FCO", "milan malpensa": "MXP", "milan": "MXP", "venice": "VCE",
    "bologna": "BLQ", "naples": "NAP", "bari": "BRI", "catania": "CTA",
    "palermo": "PMO", "pisa": "PSA", "turin": "TRN", "verona": "VRN",
    "barcelona": "BCN", "madrid": "MAD", "malaga": "AGP", "málaga": "AGP",
    "valencia": "VLC", "alicante": "ALC", "seville": "SVQ", "tenerife south": "TFS",
    "tenerife": "TFS", "gran canaria": "LPA", "lisbon": "LIS", "porto": "OPO",
    "faro": "FAO", "paris beauvais": "BVA", "paris": "BVA", "lyon": "LYS",
    "nice": "NCE", "basel": "BSL", "geneva": "GVA", "brussels charleroi": "CRL",
    "brussels": "CRL", "eindhoven": "EIN", "amsterdam": "AMS", "cologne": "CGN",
    "cologne/bonn": "CGN", "dortmund": "DTM", "hamburg": "HAM", "berlin": "BER",
    "memmingen": "FMM", "nuremberg": "NUE", "frankfurt hahn": "HHN", "frankfurt": "HHN",
    "munich": "MUC", "stockholm arlanda": "ARN", "stockholm": "ARN", "gothenburg": "GOT",
    "malmo": "MMX", "malmö": "MMX", "copenhagen": "CPH", "oslo": "OSL",
    "oslo torp": "TRF", "helsinki": "HEL", "reykjavik keflavik": "KEF", "reykjavik": "KEF",
    "riga": "RIX", "vilnius": "VNO", "kaunas": "KUN", "tallinn": "TLL",
    "chisinau": "RMO", "chișinău": "RMO", "tbilisi": "TBS", "batumi": "BUS",
    "baku": "GYD", "istanbul": "IST", "istanbul sabiha gokcen": "SAW", "antalya": "AYT",
    "dalaman": "DLM", "bodrum": "BJV", "izmir": "ADB", "cairo": "CAI",
    "alexandria": "HBE", "marrakesh": "RAK", "marrakech": "RAK", "agadir": "AGA",
    "tenerife": "TFS", "funchal": "FNC", "madeira": "FNC",
}


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
        normalized = raw.casefold()
        sid = self.station_ids.get(normalized)
        if sid:
            return sid
        alias = _STATION_ALIASES.get(normalized)
        if alias:
            return alias
        # Wizz/PDF labels sometimes append an airport name in parentheses. Try
        # the city prefix before failing, while avoiding fuzzy/ambiguous guesses.
        prefix = re.split(r"\s*\(|\s+-\s+", raw, maxsplit=1)[0].strip().casefold()
        if prefix and prefix != normalized:
            sid = self.station_ids.get(prefix) or _STATION_ALIASES.get(prefix)
            if sid:
                return sid
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
        flights = client.check(path[0], path[1], day)
        return [{"path": path, "legs": [f.to_dict()], "connection_minutes": None} for f in flights]

    first_legs = client.check(path[0], path[1], day)
    combos: List[Dict[str, Any]] = []
    min_transfer = timedelta(minutes=max(0, min_transfer_minutes))
    max_transfer = timedelta(hours=max(1, max_transfer_hours))
    second_cache: Dict[date, List[Flight]] = {}
    for first in first_legs:
        for second_day in (first.arrival.date(), first.arrival.date() + timedelta(days=1)):
            if second_day not in second_cache:
                second_cache[second_day] = client.check(path[1], path[2], second_day)
            for second in second_cache[second_day]:
                wait = second.departure - first.arrival
                if wait < min_transfer or wait > max_transfer:
                    continue
                combos.append({
                    "path": path,
                    "legs": [first.to_dict(), second.to_dict()],
                    "connection_minutes": int(wait.total_seconds() // 60),
                })
    combos.sort(key=lambda x: x["legs"][0]["departure"])
    return combos


def scan_itineraries(
    graph: CurrentRouteGraph,
    client: WizzAYCFClient,
    origin: str,
    destination: Optional[str],
    start: date,
    days: int = 4,
    max_stops: int = 1,
    min_transfer_minutes: int = 150,
    max_results: int = 100,
    max_paths_per_day: int = 250,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    days = max(1, min(4, int(days)))
    for offset in range(days):
        day = start + timedelta(days=offset)
        for path in graph.paths(origin, destination, day, max_stops=max_stops, max_paths=max_paths_per_day):
            for combo in combine_path(client, path, day, min_transfer_minutes=min_transfer_minutes):
                sig = tuple((leg.get("flight_code"), leg.get("departure"), leg.get("arrival")) for leg in combo["legs"])
                if sig in seen:
                    continue
                seen.add(sig)
                combo["search_day"] = day.isoformat()
                combo["source"] = "live"
                out.append(combo)
                if len(out) >= max_results:
                    return out, {"live_requests": client.live_requests, "truncated": True}
    return out, {"live_requests": client.live_requests, "truncated": False}
