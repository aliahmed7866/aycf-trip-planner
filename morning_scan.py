"""Scheduled AYCF cache warmer using a persisted user-selected route scope."""

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
from scan_scope import airport_variants, load_scope, scan_plan, scope_fingerprint, scope_summary
from scanner import Flight, WizzAYCFClient, WizzIntegrationChanged, WizzSessionExpired, _parse_dt
from session_vault import SessionVault
from station_resolver import prepare_required_stations


def _cache_dir() -> str:
    return os.environ.get("AYCF_CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))


def _runtime_path() -> Path:
    config_dir = Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf")))
    return config_dir / "wizz_runtime.json"


def _load_wizz_runtime() -> dict:
    try:
        data = json.loads(_runtime_path().read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _apply_wizz_runtime(client: WizzAYCFClient) -> bool:
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
    origin_keys = {"origin", "originid", "origincode", "originstation", "departurestation", "departurestationid", "from", "fromstation", "fromstationid"}
    destination_keys = {"destination", "destinationid", "destinationcode", "destinationstation", "destinationstationid", "arrivalstation", "arrivalstationid", "to", "tostation", "tostationid"}
    date_keys = {"departure", "departuredate", "departureday", "date", "flightdate", "outbounddate", "searchdate", "traveldate"}
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
    text = re.sub(r"(?i)(authorization|cookie|token|secret|session)[\s\"':=]+[^,;\s\"]+", r"\1=<redacted>", text)
    return re.sub(r"\s+", " ", text).strip()[:320] or "no response body"


def _is_no_availability_400(response: requests.Response) -> bool:
    return response.status_code == 400 and _safe_wizz_error(response).strip().casefold().strip('"') == "error.availability"


def _is_auth_location(location: str) -> bool:
    low = str(location or "").casefold()
    return "openid-connect/auth" in low or "keycloak" in low or "/login" in low


def _is_wallet_location(location: str) -> bool:
    low = str(location or "").casefold()
    return "multipass.wizzair.com" in low and "/subscriptions/spa/private-page/wallets" in low


def _looks_like_login_html(response: requests.Response) -> bool:
    if _is_auth_location(str(response.url or "")):
        return True
    body = str(response.text or "")[:8000].casefold()
    return any(marker in body for marker in ("openid-connect/auth", "keycloak", 'name="password"', "name='password'", "sign in to your account", "log in to your account"))


class CapturedRequestWizzClient(WizzAYCFClient):
    captured_request_method = "POST"
    captured_template_type = ""
    captured_request_template = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.no_availability_responses = 0
        self.wallet_redirects = 0
        self.html_retries = 0

    def _request_kwargs(self, payload):
        headers = {"X-Requested-With": "XMLHttpRequest"}
        kwargs = {"headers": headers, "allow_redirects": False}
        if str(self.captured_template_type or "").lower() == "form":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            kwargs["data"] = payload
        else:
            headers["Content-Type"] = "application/json"
            kwargs["json"] = payload
        return kwargs

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
                    raise WizzIntegrationChanged(f"Wizz rejected {context} with HTTP 400: {_safe_wizz_error(response)}") from exc
                raise
            if 300 <= response.status_code < 400:
                location = str(response.headers.get("Location") or "")
                if _is_auth_location(location):
                    raise WizzSessionExpired("Wizz redirected AYCF polling to authentication. Reconnect Wizz; completed route checks remain cached.")
                if allow_no_availability and _is_wallet_location(location):
                    self.no_availability_responses += 1
                    self.wallet_redirects += 1
                    return None
                raise WizzIntegrationChanged(f"Wizz redirected {context} with HTTP {response.status_code} to an unexpected location: {location or '<missing>'}")
            try:
                return response.json()
            except ValueError as exc:
                content_type = response.headers.get("Content-Type", "unknown")
                if _looks_like_login_html(response):
                    raise WizzSessionExpired("Wizz returned its login/auth page during AYCF polling. Reconnect Wizz; completed checks remain cached.") from exc
                if attempt < retries:
                    self.html_retries += 1
                    time.sleep(min(8.0, 2.0 ** (attempt + 1)))
                    continue
                raise WizzIntegrationChanged(f"Wizz returned persistent non-JSON for {context} after {attempt + 1} attempts (HTTP {response.status_code}, {content_type}, final URL {response.url or self.dynamic_url}). Completed checks remain cached.") from exc
        raise AssertionError("unreachable")

    def preflight(self) -> dict:
        template = self.captured_request_template
        if not self.dynamic_url or not isinstance(template, dict):
            return {"ok": False, "reason": "no captured request template"}
        data = self._send_and_decode(template, "captured AYCF preflight", allow_no_availability=True)
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
            payload = {"flightType": "OW", "origin": origin_id, "destination": destination_id, "departure": day.isoformat(), "arrival": "", "intervalSubtype": None}
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
            flights.append(Flight(origin=origin, destination=destination, flight_code=str(row.get("flightCode") or row.get("flightNumber") or ""), departure=dep, arrival=arr, departure_text=str(dep_raw), arrival_text=str(arr_raw), duration=str(row.get("duration") or "")))
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
    while day <= end.date():
        yield day
        day += timedelta(days=1)


def run(force: bool = False) -> dict:
    cache_root = _cache_dir()
    _, df, generated, departure_start, departure_end = refresh_direct_snapshot(cache_root, os.environ.get("AYCF_PDF_URL", "https://multipass.wizzair.com/aycf-availability.pdf"))
    _mirror_for_web(cache_root, df, generated)

    all_route_pairs = sorted(set(zip(df["departure_from"], df["departure_to"])))
    scope = load_scope()
    plan = scan_plan(all_route_pairs, scope, days=max(1, (departure_end.date() - departure_start.date()).days + 1))
    primary_pairs = plan["primary_routes"]
    hub_pairs = plan["hub_routes"]
    route_pairs = plan["routes"]
    if not route_pairs:
        raise RuntimeError("Your scan scope matches no routes in the current AYCF PDF. Adjust Morning scan scope in the app.")
    scope_id = scope_fingerprint(scope)
    run_id = hashlib.sha256((generated.isoformat() + "\n" + scope_id + "\n" + "\n".join(f"{a}>{b}" for a, b in route_pairs)).encode()).hexdigest()[:20]

    station_names = sorted({station for origin, destination in route_pairs for endpoint in (origin, destination) for station in airport_variants(endpoint, scope)})

    db = ScanCacheDB()
    db.upsert_pdf_run(run_id, generated.isoformat(), departure_start.isoformat(), departure_end.isoformat(), len(route_pairs), scope_id=scope_id, scope=scope)
    current = db.get_pdf_run(run_id)
    if current and current.get("scanned_at") and not force:
        return {"ok": True, "skipped": True, "reason": "Current PDF and scan scope already scanned", "pdf_run_id": run_id, "scope_id": scope_id}
    if db.scan_in_progress(run_id) and not force:
        return {"ok": True, "skipped": True, "reason": "A scan for this PDF and scope is already running", "pdf_run_id": run_id, "scope_id": scope_id}

    state = SessionVault().load()
    if not state:
        raise RuntimeError("No saved Wizz session. Import a Wizz session before the scheduled scan.")
    client = CapturedRequestWizzClient(state, cache_ttl=int(os.environ.get("AYCF_LIVE_CACHE_SECONDS", "300")), min_delay=float(os.environ.get("AYCF_MIN_REQUEST_DELAY", "1.0")))
    if not _apply_wizz_runtime(client):
        client.bootstrap()

    station_report = prepare_required_stations(client, station_names)
    print(f"[AYCF] PDF {generated.isoformat()} | scope {scope_id} | priority {len(primary_pairs)} routes + hubs {len(hub_pairs)} routes | {len(route_pairs)} total | {departure_start.date()}..{departure_end.date()} | stations {station_report['resolved']}/{station_report['required']} resolved | aliases {station_report['aliases']}", flush=True)
    print(f"[AYCF] Scope: {scope_summary(scope)}", flush=True)
    if station_report["unresolved"]:
        raise RuntimeError("Station preflight failed before live scanning. Unresolved scoped stations: " + ", ".join(station_report["unresolved"]))
    print("[AYCF] Station preflight OK for selected scope.", flush=True)

    preflight = client.preflight()
    print(f"[AYCF] Captured-request preflight OK ({preflight.get('response')})." if preflight.get("ok") else f"[AYCF] Preflight skipped: {preflight.get('reason')}", flush=True)

    days = list(_scan_days(departure_start, departure_end))
    total_checks = len(route_pairs) * len(days)
    progress_every = max(1, int(os.environ.get("AYCF_PROGRESS_EVERY", "10")))
    scan_id = db.start_scan(run_id)
    route_day_checks = flights_found = resumed_checks = processed = 0
    started = time.time()
    try:
        for day in days:
            for origin, destination in route_pairs:
                processed += 1
                if db.route_checked(run_id, origin, destination, day) and not force:
                    resumed_checks += 1
                    if processed == 1 or processed % progress_every == 0 or processed == total_checks:
                        print(f"[AYCF] {processed}/{total_checks} | resumed {resumed_checks} | live {route_day_checks} | flights {flights_found}", flush=True)
                    continue

                merged_flights = []
                origin_variants = airport_variants(origin, scope)
                destination_variants = airport_variants(destination, scope)
                for concrete_origin in origin_variants:
                    for concrete_destination in destination_variants:
                        if concrete_origin == concrete_destination:
                            continue
                        merged_flights.extend(client.check(concrete_origin, concrete_destination, day))
                merged_flights.sort(key=lambda f: f.departure)
                db.replace_route_check(run_id, origin, destination, day, merged_flights)
                route_day_checks += 1
                flights_found += len(merged_flights)

                if processed == 1 or processed % progress_every == 0 or processed == total_checks:
                    elapsed = max(1.0, time.time() - started)
                    rate = route_day_checks / elapsed if route_day_checks else 0.0
                    variant_text = f"{'/'.join(origin_variants)}->{'/' .join(destination_variants)}"
                    print(f"[AYCF] {processed}/{total_checks} | {variant_text} {day} | live {route_day_checks} | resumed {resumed_checks} | flights {flights_found} | no-availability {client.no_availability_responses} | wallet-redirects {client.wallet_redirects} | {rate:.2f} checks/s", flush=True)

        db.mark_pdf_scanned(run_id)
        db.finish_scan(scan_id, "completed", route_day_checks, client.live_requests, flights_found)
        return {"ok": True, "skipped": False, "pdf_run_id": run_id, "scope_id": scope_id, "scope": scope, "generated_at": generated.isoformat(), "routes": len(route_pairs), "priority_routes": len(primary_pairs), "hub_routes": len(hub_pairs), "pdf_routes": len(all_route_pairs), "stations": station_report["required"], "total_route_day_checks": total_checks, "route_day_checks": route_day_checks, "resumed_checks": resumed_checks, "live_requests": client.live_requests, "flights_found": flights_found, "no_availability_responses": client.no_availability_responses, "wallet_redirects": client.wallet_redirects, "html_retries": client.html_retries}
    except Exception as exc:
        db.finish_scan(scan_id, "failed", route_day_checks, client.live_requests, flights_found, str(exc))
        print(f"[AYCF] Scan stopped after {processed}/{total_checks}; {route_day_checks} new checks and {resumed_checks} resumed checks are preserved in SQLite.", flush=True)
        raise


if __name__ == "__main__":
    print(json.dumps(run(force=os.environ.get("AYCF_FORCE_MORNING_SCAN", "false").lower() == "true"), indent=2))
