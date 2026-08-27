"""Scheduled morning AYCF cache warmer.

Run this repeatedly around Wizz's morning publication window. It downloads the
official PDF directly, skips work if that publication timestamp was already
scanned, then checks every advertised route for each day in the PDF's 4-day
window and persists positive and zero-flight results in SQLite.
"""

import hashlib
import json
import os
import re
from datetime import timedelta
from pathlib import Path

import requests

from cache_db import ScanCacheDB
from direct_pdf import refresh_direct_snapshot
from scanner import CurrentRouteGraph, WizzAYCFClient
from session_vault import SessionVault


def _cache_dir() -> str:
    return os.environ.get("AYCF_CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))


def _runtime_path() -> Path:
    config_dir = Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf")))
    return config_dir / "wizz_runtime.json"


def _load_wizz_runtime() -> dict:
    """Load the non-secret endpoint/station metadata captured from Android Chrome."""
    path = _runtime_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _apply_wizz_runtime(client: WizzAYCFClient) -> bool:
    """Apply the captured Wizz endpoint. Return True when discovery can be skipped."""
    runtime = _load_wizz_runtime()
    endpoint = str(runtime.get("availability_url") or "").strip()
    if not endpoint.startswith("https://multipass.wizzair.com/"):
        return False

    client.dynamic_url = endpoint
    station_ids = runtime.get("station_ids")
    if isinstance(station_ids, dict):
        for key, value in station_ids.items():
            if key and value:
                client.station_ids[str(key).casefold()] = str(value).upper()
    return True


def _add_station_alias(client: WizzAYCFClient, value, iata: str) -> None:
    text = str(value or "").strip()
    if not text:
        return
    client.station_ids[text.casefold()] = iata
    # Wizz labels often look like "Rome (Fiumicino)". The AYCF PDF may use just
    # the city name, so also keep the prefix as an alias.
    if "(" in text:
        prefix = text.split("(", 1)[0].strip()
        if prefix:
            client.station_ids.setdefault(prefix.casefold(), iata)


def _populate_wizz_station_ids(client: WizzAYCFClient) -> int:
    """Populate city/airport aliases from Wizz's public airport map.

    The authenticated Multipass page no longer exposes window.CVO.routes on
    Android, so Chrome capture can legitimately report zero station aliases.
    Wizz's public website map still carries the canonical IATA/name mapping.
    This lookup is best-effort; captured aliases and static fallbacks remain
    available if the public map is temporarily unavailable.
    """
    before = len(client.station_ids)
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

    # Keep a recent known version as a last-resort candidate; normal operation
    # discovers the current version from the Wizz frontend above.
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

    # Small safety net for names known to appear in AYCF PDFs. Runtime/public
    # mappings win; these only fill gaps when Wizz's public map is unavailable.
    fallback = {
        "alghero": "AHO",
        "london luton": "LTN",
        "london": "LTN",
        "liverpool": "LPL",
        "budapest": "BUD",
        "bucharest": "OTP",
        "warsaw": "WAW",
        "kutaisi": "KUT",
        "yerevan": "EVN",
        "abu dhabi": "AUH",
        "amman": "AMM",
        "hurghada": "HRG",
        "sharm el-sheikh": "SSH",
        "gdansk": "GDN",
        "krakow": "KRK",
        "katowice": "KTW",
        "birmingham": "BHX",
        "leeds/bradford": "LBA",
    }
    for name, iata in fallback.items():
        client.station_ids.setdefault(name.casefold(), iata)

    return max(0, len(client.station_ids) - before)


def _mirror_for_web(cache_root: str, df, generated) -> None:
    """Make the official snapshot visible to the already-running web graph."""
    web_data = Path(cache_root) / "data"
    web_data.mkdir(parents=True, exist_ok=True)
    target = web_data / f"official-{generated.isoformat().replace(':', '_')}.csv"
    if not target.exists():
        df.to_csv(target, index=False)
    # Keep the third-party updater from immediately replacing today's official
    # snapshot after the morning worker has published it locally.
    (Path(cache_root) / "last_update.txt").write_text(str(int(__import__('time').time())), encoding="utf-8")


def run(force: bool = False) -> dict:
    cache_root = _cache_dir()
    data_dir, df, generated, departure_start, departure_end = refresh_direct_snapshot(
        cache_root,
        os.environ.get("AYCF_PDF_URL", "https://multipass.wizzair.com/aycf-availability.pdf"),
    )
    _mirror_for_web(cache_root, df, generated)
    graph = CurrentRouteGraph(data_dir)
    route_pairs = sorted(set(zip(df["departure_from"], df["departure_to"])))
    run_id = hashlib.sha256((generated.isoformat() + "\n" + "\n".join(f"{a}>{b}" for a, b in route_pairs)).encode()).hexdigest()[:20]

    db = ScanCacheDB()
    db.upsert_pdf_run(run_id, generated.isoformat(), departure_start.isoformat(), departure_end.isoformat(), len(route_pairs))
    current = db.latest_pdf_run()
    if current and current.get("run_id") == run_id and current.get("scanned_at") and not force:
        return {"ok": True, "skipped": True, "reason": "PDF publication already scanned", "pdf_run_id": run_id}
    if db.scan_in_progress(run_id) and not force:
        return {"ok": True, "skipped": True, "reason": "A scan for this PDF is already running", "pdf_run_id": run_id}

    state = SessionVault().load()
    if not state:
        raise RuntimeError("No saved Wizz session. Import a Wizz session before the scheduled scan.")

    client = WizzAYCFClient(
        state,
        cache_ttl=int(os.environ.get("AYCF_LIVE_CACHE_SECONDS", "300")),
        min_delay=float(os.environ.get("AYCF_MIN_REQUEST_DELAY", "1.0")),
    )
    # Android imports the real availability URL from the authenticated Chrome
    # network session. Reuse it directly; HTML bootstrap is only a fallback for
    # older/non-Android setups where no runtime capture exists.
    if not _apply_wizz_runtime(client):
        client.bootstrap()
    _populate_wizz_station_ids(client)

    scan_id = db.start_scan(run_id)
    route_day_checks = 0
    flights_found = 0
    resumed_checks = 0
    try:
        day = departure_start.date()
        end_day = departure_end.date()
        while day <= end_day:
            for origin, destination in sorted(graph.edges_for_day(day)):
                if db.route_checked(run_id, origin, destination, day) and not force:
                    resumed_checks += 1
                    continue
                flights = client.check(origin, destination, day)
                db.replace_route_check(run_id, origin, destination, day, flights)
                route_day_checks += 1
                flights_found += len(flights)
            day += timedelta(days=1)

        db.mark_pdf_scanned(run_id)
        db.finish_scan(scan_id, "completed", route_day_checks, client.live_requests, flights_found)
        return {
            "ok": True,
            "skipped": False,
            "pdf_run_id": run_id,
            "generated_at": generated.isoformat(),
            "routes": len(route_pairs),
            "route_day_checks": route_day_checks,
            "resumed_checks": resumed_checks,
            "live_requests": client.live_requests,
            "flights_found": flights_found,
        }
    except Exception as exc:
        db.finish_scan(scan_id, "failed", route_day_checks, client.live_requests, flights_found, str(exc))
        raise


if __name__ == "__main__":
    print(json.dumps(run(force=os.environ.get("AYCF_FORCE_MORNING_SCAN", "false").lower() == "true"), indent=2))
