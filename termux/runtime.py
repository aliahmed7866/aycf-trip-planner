"""Run AYCF web/morning processes with Termux-specific reliability patches."""

import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

STATE_DIR = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf")))
STATE_DIR.mkdir(parents=True, exist_ok=True)
os.environ["AYCF_DB_PATH"] = os.environ.get("AYCF_TERMUX_DB_PATH", str(STATE_DIR / "aycf.sqlite3"))
os.environ["AYCF_ALLOW_LIVE_FALLBACK"] = os.environ.get("AYCF_TERMUX_ALLOW_LIVE_FALLBACK", "false")

import scanner  # noqa: E402

CONFIG_PATH = Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf"))) / "wizz_runtime.json"

PILOT_ALIASES = {
    "aalesund": "AES", "alesund": "AES", "barcelona": "BCN",
    "basel/mulhouse": "BSL", "basel mulhouse": "BSL", "cluj": "CLJ",
    "giza": "SPX", "kefallinia": "EFL", "kefalonia": "EFL", "kerkyra": "CFU",
    "klaipeda/palanga": "PLQ", "klaipeda palanga": "PLQ", "madeira": "FNC",
    "sevilla": "SVQ", "szczytno": "SZY", "zakinthos island": "ZTH",
    "zakynthos island": "ZTH", "varna": "VAR", "burgas": "BOJ",
    "liverpool": "LPL", "leeds/bradford": "LBA", "leeds bradford": "LBA",
    "birmingham": "BHX", "london gatwick": "LGW", "gatwick": "LGW",
    "london luton": "LTN", "luton": "LTN", "london stansted": "STN", "stansted": "STN",
}


def _normalize(value) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _load_runtime():
    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _install_aliases(target):
    for key, value in PILOT_ALIASES.items():
        target.setdefault(str(key).casefold(), str(value).upper())
        normalized = _normalize(key)
        if normalized:
            target.setdefault(normalized, str(value).upper())


def _patch_scanner(runtime):
    initial_runtime = runtime if isinstance(runtime, dict) else {}
    original_init = scanner.WizzAYCFClient.__init__

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        current = _load_runtime() or initial_runtime
        endpoint = str(current.get("availability_url") or "").strip()
        station_ids = current.get("station_ids") if isinstance(current.get("station_ids"), dict) else {}
        if endpoint:
            self.dynamic_url = endpoint
        for key, value in station_ids.items():
            if key and value:
                self.station_ids[str(key).casefold()] = str(value).upper()
        _install_aliases(self.station_ids)

    scanner.WizzAYCFClient.__init__ = patched_init

    original_parse_dt = scanner._parse_dt

    def anchored_parse_dt(day_text, value):
        parsed = original_parse_dt(day_text, value)
        try:
            travel_day = datetime.fromisoformat(str(day_text)[:10])
            return parsed.replace(year=travel_day.year, month=travel_day.month, day=travel_day.day)
        except (TypeError, ValueError):
            return parsed

    scanner._parse_dt = anchored_parse_dt


def _repair_cached_flight_dates() -> int:
    path = Path(os.environ["AYCF_DB_PATH"])
    if not path.exists():
        return 0
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """SELECT rowid, travel_date, departure, arrival
               FROM route_flights
               WHERE substr(departure,1,10) <> travel_date"""
        ).fetchall()
        repaired = 0
        for row in rows:
            try:
                travel_day = datetime.fromisoformat(row["travel_date"])
                old_dep = datetime.fromisoformat(row["departure"])
                old_arr = datetime.fromisoformat(row["arrival"])
                dep = travel_day.replace(hour=old_dep.hour, minute=old_dep.minute, second=old_dep.second, microsecond=old_dep.microsecond)
                arr = travel_day.replace(hour=old_arr.hour, minute=old_arr.minute, second=old_arr.second, microsecond=old_arr.microsecond)
                if arr <= dep:
                    arr += timedelta(days=1)
                conn.execute("UPDATE route_flights SET departure=?, arrival=? WHERE rowid=?", (dep.isoformat(), arr.isoformat(), row["rowid"]))
                repaired += 1
            except (TypeError, ValueError):
                continue
        if repaired:
            conn.commit()
            print(f"[AYCF] Repaired travel dates for {repaired} cached flights locally; no Wizz rescan required.", flush=True)
        return repaired
    finally:
        conn.close()


def _patch_transport():
    original_request = scanner.WizzAYCFClient._request
    attempts = max(1, min(5, int(os.environ.get("AYCF_NETWORK_ATTEMPTS", "3"))))

    def resilient_request(self, method, url, **kwargs):
        last_exc = None
        for attempt in range(attempts):
            try:
                return original_request(self, method, url, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                if attempt + 1 >= attempts:
                    break
                delay = min(10.0, 1.5 * (2 ** attempt))
                print(f"[AYCF] transient network error; retrying in {delay:.1f}s ({attempt + 2}/{attempts})", flush=True)
                try:
                    self.http.close()
                except Exception:
                    pass
                time.sleep(delay)
        raise last_exc

    scanner.WizzAYCFClient._request = resilient_request


def _ensure_pdf_catalogue():
    cache_root = os.environ.get("AYCF_CACHE_DIR", str(ROOT / "cache"))
    direct_dir = Path(cache_root) / "direct-data"
    if direct_dir.exists() and any(direct_dir.glob("*.csv")):
        return
    from direct_pdf import refresh_direct_snapshot
    print("[AYCF] No cached official PDF catalogue; fetching one before starting the web UI.", flush=True)
    refresh_direct_snapshot(cache_root, os.environ.get("AYCF_PDF_URL", "https://multipass.wizzair.com/aycf-availability.pdf"))


def _json_file(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _status():
    from termux.run_state import read_status
    print(json.dumps({"scan": read_status(), "wizz": _json_file(STATE_DIR / "wizz-session-status.json"), "supervisor": _json_file(STATE_DIR / "supervisor-status.json")}, indent=2))


def _repair():
    result = subprocess.run(["bash", str(ROOT / "termux" / "auto-refresh-wizz.sh")], cwd=str(ROOT), env=os.environ.copy(), check=False)
    raise SystemExit(result.returncode)


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in {"web", "morning", "status", "repair"}:
        raise SystemExit("Usage: python termux/runtime.py web|morning|status|repair")
    command = sys.argv[1]
    if command == "status":
        _status()
        return
    if command == "repair":
        _repair()
        return

    _patch_scanner(_load_runtime())
    _patch_transport()
    _repair_cached_flight_dates()
    print(f"[AYCF] Local DB: {os.environ['AYCF_DB_PATH']}", flush=True)
    if command == "web":
        _ensure_pdf_catalogue()
        os.environ["AYCF_WEB_PROCESS"] = "true"
        from app import create_app
        from termux.health_ui import bp as system_health_bp
        from termux.multi_search import bp as multi_search_bp
        app = create_app()
        app.register_blueprint(system_health_bp)
        app.register_blueprint(multi_search_bp)
        app.jinja_env.globals["system_health_enabled"] = True
        app.jinja_env.globals["multi_search_enabled"] = True
        app.run(host=os.environ.get("AYCF_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8080")))
    else:
        from termux import automated_morning
        force = os.environ.get("AYCF_FORCE_MORNING_SCAN", "false").lower() == "true" or os.environ.get("AYCF_WEB_PROCESS", "false").lower() == "true"
        result = automated_morning.run(force=force)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
