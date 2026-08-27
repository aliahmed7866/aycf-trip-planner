"""Scheduled morning AYCF cache warmer.

Downloads Wizz's official AYCF PDF, reuses the verified request captured from
Android Chrome, checks the PDF route network across its advertised date window,
and persists positive and zero-flight results in SQLite.

The PDF is route-level, not a per-day timetable. Multipass may therefore return
HTTP 400/error.availability for a listed route on a particular day; that is a
normal zero-flight result. Authentication/interstitial HTML is never treated as
zero availability.
"""

import hashlib
import json
import os
import re
import time
from datetime import timedelta
from pathlib import Path

import requests

from cache_db import ScanCacheDB
from direct_pdf import refresh_direct_snapshot
from scanner import (
    CurrentRouteGraph,
    Flight,
    WizzAYCFClient,
    WizzIntegrationChanged,
    WizzSessionExpired,
    _parse_dt,
)
from session_vault import SessionVault


def _cache_dir() -> str:
    return os.environ.get("AYCF_CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))


def _runtime_path() -> Path:
    config_dir = Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf")))
    return config_dir / "wizz_runtime.json"


def _load_wizz_runtime() -> dict:
    path = _runtime_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _apply_wizz_runtime(client: WizzAYCFClient) -> bool:
    """Apply the verified Chrome endpoint/template and captured station aliases."""
    runtime = _load_wizz_runtime()
    endpoint = str(runtime.get("availability_url") or "").strip()
    if not endpoint.startswith("https://multipass.wizzair.com/"):
        return False

    client.dynamic_url = endpoint
    client.captured_request_method = str(runtime.get("request_method") or "POST").upper()
    client.captured_template_type = str(runtime.get("request_template_type") or "").lower()
    template = runtime.get("request_template")
    client.captured_request_template = template if isinstance(template, dict) else None

    station_ids = runtime.get("station_ids")
    if isinstance(station_ids, dict):
        for key, value in station_ids.items():
            if key and value:
                client.station_ids[str(key).casefold()] = str(value).upper()
    return True


def _replace_route_fields(value, origin_id: str, destination_id: str, day_text: str):
    """Clone a captured request body while changing only semantic route/date fields."""
    origin_keys = {
        "origin", "originid", "origincode", "originstation", "departurestation",
        "departurestationid", "from", "fromstation", "fromstationid",
    }
    destination_keys = {
        "destination", "destinationid", "destinationcode", "destinationstation",
        "destinationstationid", "arrivalstation", "arrivalstationid", "to",
        "tostation", "tostationid",
    }
    date_keys = {
        "departure", "departuredate", "departureday", "date", "flightdate",
        "outbounddate", "searchdate", "traveldate",
    }

    if isinstance(value, dict):
        out = {}
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).casefold())
            if normalized in origin_keys:
                out[key] = origin_id
            elif normalized in destination_keys:
                out[key] = destination_id
            elif normalized in date_keys:
                out[key] = day_text
            else:
                out[key] = _replace_route_fields(child, origin_id, destination_id, day_text)
        return out
    if isinstance(value, list):
        return [_replace_route_fields(item, origin_id, destination_id, day_text) for item in value]
    return value


def _safe_wizz_error(response: requests.Response) -> str:
    """Return a short application error without leaking auth/session values."""
    text = ""
    try:
        payload = response.json()
        if isinstance(payload, dict):
            for key in ("message", "error", "detail", "title", "errors"):
                if key in payload:
                    value = payload.get(key)
                    text = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
                    break
    except Exception:
        pass
    if not text:
        text = str(response.text or "")
    text = re.sub(
        r"(?i)(authorization|cookie|token|secret|session)[\s\"':=]+[^,;\s\"]+",
        r"\1=<redacted>",
        text,
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text[:320] or "no response body"


def _is_no_availability_400(response: requests.Response) -> bool:
    if response.status_code != 400:
        return False
    detail = _safe_wizz_error(response).strip().casefold().strip('"')
    return detail == "error.availability"


def _looks_like_login_html(response: requests.Response) -> bool:
    final_url = str(response.url or "").casefold()
    if "openid-connect/auth" in final_url or "/login" in final_url:
        return True
    body = str(response.text or "")[:8000].casefold()
    markers = (
        "openid-connect/auth",
        "keycloak",
        "name=\"password\"",
        "name='password'",
        "sign in to your account",
        "log in to your account",
    )
    return any(marker in body for marker in markers)


def _add_station_alias(client: WizzAYCFClient, value, iata: str) -> None:
    text = str(value or "").strip()
    if not text:
        return
    client.station_ids[text.casefold()] = iata
    if "(" in text:
        prefix = text.split("(", 1)[0].strip()
        if prefix:
            client.station_ids.setdefault(prefix.casefold(), iata)


def _populate_wizz_station_ids(client: WizzAYCFClient) -> int:
    """Best-effort fallback only when Chrome did not capture enough aliases."""
    before = len(client.station_ids)

    # A normal Android capture currently provides hundreds of aliases. Avoid an
    # unrelated public-map request on the critical morning path when those are
    # already present.
    if before >= int(os.environ.get("AYCF_CAPTURED_ALIAS_THRESHOLD", "100")):
        return 0

    session = requests.Session()
    session.headers.update({
        "User-Agent": os.environ.get(
            "WIZZ_USER_AGENT",
            "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        ),
        "Accept": "application/json,text/plain,*/*",
        "Origin": "https://www.wizzair.com",
        "Referer": "https://www.wizzair.com/",
    })

    try:
        home = session.get("https://www.wizzair.com/", timeout=20, allow_redirects=True)
        home.raise_for_status()
        versions = re.findall(r"be\.wizzair\.com/(\d+\.\d+\.\d+)", home.text)
    except Exception:
        versions = []

    candidates = []
    for version in versions[:3] + ["12.2.0"]:
        if version not in candidates:
            candidates.append(version)

    payload = None
    for version in candidates:
        for path in ("Api/asset/MapData", "Api/asset/map"):
            try:
                response = session.get(
                    f"https://be.wizzair.com/{version}/{path}?languageCode=en-gb",
                    timeout=20,
                )
                if response.status_code != 200:
                    continue
                data = response.json()
                if isinstance(data, dict) and isinstance(data.get("cities"), list):
                    payload = data
                    break
            except Exception:
                continue
        if payload:
            break

    if payload:
        for city in payload.get("cities") or []:
            if not isinstance(city, dict):
                continue
            iata = str(city.get("iata") or city.get("iataCode") or "").strip().upper()
            if len(iata) != 3 or not iata.isalpha():
                continue
            client.station_ids[iata.casefold()] = iata
            for key in (
                "name", "shortName", "city", "cityName", "airportName",
                "displayName", "nameWithCountry", "fullName",
            ):
                _add_station_alias(client, city.get(key), iata)

    fallback = {
        "alghero": "AHO", "london luton": "LTN", "london": "LTN", "liverpool": "LPL",
        "budapest": "BUD", "bucharest": "OTP", "warsaw": "WAW", "kutaisi": "KUT",
        "yerevan": "EVN", "abu dhabi": "AUH", "amman": "AMM", "hurghada": "HRG",
        "sharm el-sheikh": "SSH", "gdansk": "GDN", "krakow": "KRK", "katowice": "KTW",
        "birmingham": "BHX", "leeds/bradford": "LBA",
    }
    for name, iata in fallback.items():
        client.station_ids.setdefault(name.casefold(), iata)
    return max(0, len(client.station_ids) - before)


class CapturedRequestWizzClient(WizzAYCFClient):
    """Replay the verified Chrome request with conservative response handling."""

    captured_request_method = "POST"
    captured_template_type = ""
    captured_request_template = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.no_availability_responses = 0
        self.html_retries = 0

    def _request_kwargs(self, payload):
        headers = {"X-Requested-With": "XMLHttpRequest"}
        if str(self.captured_template_type or "").lower() == "form":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            return {"data": payload, "headers": headers}
        headers["Content-Type"] = "application/json"
        return {"json": payload, "headers": headers}

    def _send_and_decode(self, payload, context: str, allow_no_availability: bool = True):
        method = str(self.captured_request_method or "POST").upper()
        retries = max(0, min(3, int(os.environ.get("AYCF_HTML_RETRIES", "2"))))

        for attempt in range(retries + 1):
            try:
                response = self._request(method, self.dynamic_url, **self._request_kwargs(payload))
            except requests.HTTPError as exc:
                response = exc.response
                if response is not None and allow_no_availability and _is_no_availability_400(response):
                    self.no_availability_responses += 1
                    return None
                if response is not None and response.status_code == 400:
                    raise WizzIntegrationChanged(
                        f"Wizz rejected {context} with HTTP 400: {_safe_wizz_error(response)}"
                    ) from exc
                raise

            try:
                return response.json()
            except ValueError as exc:
                content_type = response.headers.get("Content-Type", "unknown")
                if _looks_like_login_html(response):
                    raise WizzSessionExpired(
                        "Wizz returned its login/auth page during AYCF polling. Reconnect Wizz; already completed route checks remain cached."
                    ) from exc

                if attempt < retries:
                    self.html_retries += 1
                    time.sleep(min(8.0, 2.0 ** (attempt + 1)))
                    continue

                final_url = str(response.url or self.dynamic_url)
                raise WizzIntegrationChanged(
                    f"Wizz returned persistent non-JSON for {context} after {attempt + 1} attempts "
                    f"(HTTP {response.status_code}, {content_type}, final URL {final_url}). "
                    "The saved session/availability endpoint may need recapture; completed checks remain cached."
                ) from exc

        raise AssertionError("unreachable")

    def preflight(self) -> dict:
        """Replay the untouched captured request before starting a long scan."""
        template = self.captured_request_template
        if not self.dynamic_url or not isinstance(template, dict):
            return {"ok": False, "reason": "no captured request template"}

        data = self._send_and_decode(template, "captured AYCF preflight", allow_no_availability=True)
        # error.availability is also proof that the endpoint understood the
        # request and authenticated the session, so None is a successful preflight.
        return {"ok": True, "response": "no-availability" if data is None else "json"}

    def check(self, origin, destination, day):
        key = f"{origin.casefold()}|{destination.casefold()}|{day.isoformat()}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        if not self.dynamic_url:
            self.bootstrap()

        origin_id = self.resolve_station(origin)
        destination_id = self.resolve_station(destination)
        template = self.captured_request_template

        if isinstance(template, dict):
            payload = _replace_route_fields(template, origin_id, destination_id, day.isoformat())
        else:
            payload = {
                "flightType": "OW",
                "origin": origin_id,
                "destination": destination_id,
                "departure": day.isoformat(),
                "arrival": "",
                "intervalSubtype": None,
            }
            self.captured_template_type = "json"

        context = f"AYCF search {origin} ({origin_id}) -> {destination} ({destination_id}) on {day.isoformat()}"
        data = self._send_and_decode(payload, context, allow_no_availability=True)
        if data is None:
            flights = []
            self.cache.set(key, flights, self.cache_ttl)
            return flights

        flights = []
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


def _mirror_for_web(cache_root: str, df, generated) -> None:
    web_data = Path(cache_root) / "data"
    web_data.mkdir(parents=True, exist_ok=True)
    target = web_data / f"official-{generated.isoformat().replace(':', '_')}.csv"
    if not target.exists():
        df.to_csv(target, index=False)
    (Path(cache_root) / "last_update.txt").write_text(str(int(time.time())), encoding="utf-8")


def _scan_days(start, end):
    day = start.date()
    end_day = end.date()
    while day <= end_day:
        yield day
        day += timedelta(days=1)


def run(force: bool = False) -> dict:
    cache_root = _cache_dir()
    data_dir, df, generated, departure_start, departure_end = refresh_direct_snapshot(
        cache_root,
        os.environ.get("AYCF_PDF_URL", "https://multipass.wizzair.com/aycf-availability.pdf"),
    )
    _mirror_for_web(cache_root, df, generated)
    graph = CurrentRouteGraph(data_dir)
    route_pairs = sorted(set(zip(df["departure_from"], df["departure_to"])))
    run_id = hashlib.sha256(
        (generated.isoformat() + "\n" + "\n".join(f"{a}>{b}" for a, b in route_pairs)).encode()
    ).hexdigest()[:20]

    db = ScanCacheDB()
    db.upsert_pdf_run(
        run_id,
        generated.isoformat(),
        departure_start.isoformat(),
        departure_end.isoformat(),
        len(route_pairs),
    )
    current = db.latest_pdf_run()
    if current and current.get("run_id") == run_id and current.get("scanned_at") and not force:
        return {"ok": True, "skipped": True, "reason": "PDF publication already scanned", "pdf_run_id": run_id}
    if db.scan_in_progress(run_id) and not force:
        return {"ok": True, "skipped": True, "reason": "A scan for this PDF is already running", "pdf_run_id": run_id}

    state = SessionVault().load()
    if not state:
        raise RuntimeError("No saved Wizz session. Import a Wizz session before the scheduled scan.")

    client = CapturedRequestWizzClient(
        state,
        cache_ttl=int(os.environ.get("AYCF_LIVE_CACHE_SECONDS", "300")),
        min_delay=float(os.environ.get("AYCF_MIN_REQUEST_DELAY", "1.0")),
    )
    if not _apply_wizz_runtime(client):
        client.bootstrap()
    added_aliases = _populate_wizz_station_ids(client)

    print(
        f"[AYCF] PDF {generated.isoformat()} | {len(route_pairs)} routes | "
        f"{departure_start.date()}..{departure_end.date()} | station aliases {len(client.station_ids)} "
        f"(+{added_aliases} fallback)",
        flush=True,
    )

    preflight = client.preflight()
    if preflight.get("ok"):
        print(f"[AYCF] Captured-request preflight OK ({preflight.get('response')}).", flush=True)
    else:
        print(f"[AYCF] Preflight skipped: {preflight.get('reason')}", flush=True)

    days = list(_scan_days(departure_start, departure_end))
    edges_by_day = {day: sorted(graph.edges_for_day(day)) for day in days}
    total_checks = sum(len(edges) for edges in edges_by_day.values())
    progress_every = max(1, int(os.environ.get("AYCF_PROGRESS_EVERY", "10")))

    scan_id = db.start_scan(run_id)
    route_day_checks = 0
    flights_found = 0
    resumed_checks = 0
    processed = 0
    started = time.time()

    try:
        for day in days:
            for origin, destination in edges_by_day[day]:
                processed += 1
                if db.route_checked(run_id, origin, destination, day) and not force:
                    resumed_checks += 1
                    if processed == 1 or processed % progress_every == 0 or processed == total_checks:
                        print(
                            f"[AYCF] {processed}/{total_checks} | resumed {resumed_checks} | "
                            f"live {route_day_checks} | flights {flights_found}",
                            flush=True,
                        )
                    continue

                flights = client.check(origin, destination, day)
                db.replace_route_check(run_id, origin, destination, day, flights)
                route_day_checks += 1
                flights_found += len(flights)

                if processed == 1 or processed % progress_every == 0 or processed == total_checks:
                    elapsed = max(1.0, time.time() - started)
                    rate = route_day_checks / elapsed if route_day_checks else 0.0
                    print(
                        f"[AYCF] {processed}/{total_checks} | {origin}->{destination} {day} | "
                        f"live {route_day_checks} | resumed {resumed_checks} | flights {flights_found} | "
                        f"no-availability {client.no_availability_responses} | {rate:.2f} checks/s",
                        flush=True,
                    )

        db.mark_pdf_scanned(run_id)
        db.finish_scan(scan_id, "completed", route_day_checks, client.live_requests, flights_found)
        return {
            "ok": True,
            "skipped": False,
            "pdf_run_id": run_id,
            "generated_at": generated.isoformat(),
            "routes": len(route_pairs),
            "total_route_day_checks": total_checks,
            "route_day_checks": route_day_checks,
            "resumed_checks": resumed_checks,
            "live_requests": client.live_requests,
            "flights_found": flights_found,
            "no_availability_responses": client.no_availability_responses,
            "html_retries": client.html_retries,
        }
    except Exception as exc:
        db.finish_scan(scan_id, "failed", route_day_checks, client.live_requests, flights_found, str(exc))
        print(
            f"[AYCF] Scan stopped after {processed}/{total_checks}; "
            f"{route_day_checks} new checks and {resumed_checks} resumed checks are preserved in SQLite.",
            flush=True,
        )
        raise


if __name__ == "__main__":
    print(
        json.dumps(
            run(force=os.environ.get("AYCF_FORCE_MORNING_SCAN", "false").lower() == "true"),
            indent=2,
        )
    )
