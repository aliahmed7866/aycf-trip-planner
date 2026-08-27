"""Run AYCF web/morning processes with Wizz runtime metadata captured from Chrome.

Termux defaults to a deliberately small UK pilot scope while the live AYCF flow
is being validated end-to-end. The scope is configurable with
AYCF_SCAN_ORIGINS and uses a separate SQLite database so completing the pilot
cannot mark a future full-network scan as complete.
"""

import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Keep the pilot cache isolated from any future full-network cache. Both the web
# process and morning process launched through this runtime use the same DB.
os.environ.setdefault("AYCF_DB_PATH", str(ROOT / "cache" / "aycf-uk-pilot.sqlite3"))

import scanner  # noqa: E402

CONFIG_PATH = Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf"))) / "wizz_runtime.json"

DEFAULT_UK_ORIGINS = (
    "Liverpool,Leeds/Bradford,Birmingham,London Gatwick,London Luton,London Stansted"
)

# Extra deterministic aliases seen in the Wizz PDF vocabulary. These are only
# fallbacks; authenticated aliases captured from Chrome remain authoritative.
PILOT_ALIASES = {
    "aalesund": "AES",
    "alesund": "AES",
    "barcelona": "BCN",
    "basel/mulhouse": "BSL",
    "basel mulhouse": "BSL",
    "cluj": "CLJ",
    "giza": "SPX",
    "kefallinia": "EFL",
    "kefalonia": "EFL",
    "kerkyra": "CFU",
    "klaipeda/palanga": "PLQ",
    "klaipeda palanga": "PLQ",
    "madeira": "FNC",
    "sevilla": "SVQ",
    "szczytno": "SZY",
    "zakinthos island": "ZTH",
    "zakynthos island": "ZTH",
    "varna": "VAR",
    "burgas": "BOJ",
    "liverpool": "LPL",
    "leeds/bradford": "LBA",
    "leeds bradford": "LBA",
    "birmingham": "BHX",
    "london gatwick": "LGW",
    "gatwick": "LGW",
    "london luton": "LTN",
    "luton": "LTN",
    "london stansted": "STN",
    "stansted": "STN",
}


def _normalize(value) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _configured_origins():
    raw = os.environ.get("AYCF_SCAN_ORIGINS", DEFAULT_UK_ORIGINS)
    return {
        _normalize(item)
        for item in raw.split(",")
        if _normalize(item)
    }


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
    endpoint = str(runtime.get("availability_url") or "").strip()
    station_ids = runtime.get("station_ids") if isinstance(runtime.get("station_ids"), dict) else {}

    original_init = scanner.WizzAYCFClient.__init__

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        if endpoint:
            self.dynamic_url = endpoint
        for key, value in station_ids.items():
            if key and value:
                self.station_ids[str(key).casefold()] = str(value).upper()
        _install_aliases(self.station_ids)

    scanner.WizzAYCFClient.__init__ = patched_init


def _patch_transport():
    """Retry network failures that happen before Wizz returns an HTTP response."""
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
                print(
                    f"[AYCF] transient network error; retrying in {delay:.1f}s "
                    f"({attempt + 2}/{attempts})",
                    flush=True,
                )
                # Close pooled sockets so a reset connection is not reused.
                try:
                    self.http.close()
                except Exception:
                    pass
                time.sleep(delay)
        raise last_exc

    scanner.WizzAYCFClient._request = resilient_request


def _patch_route_scope():
    """Filter graph edges to selected departure airports only."""
    selected = _configured_origins()
    original_edges_for_day = scanner.CurrentRouteGraph.edges_for_day

    def scoped_edges_for_day(self, day):
        edges = original_edges_for_day(self, day)
        return {
            (origin, destination)
            for origin, destination in edges
            if _normalize(origin) in selected
        }

    scanner.CurrentRouteGraph.edges_for_day = scoped_edges_for_day
    return selected


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in {"web", "morning"}:
        raise SystemExit("Usage: python termux/runtime.py web|morning")

    _patch_scanner(_load_runtime())
    _patch_transport()
    selected = _patch_route_scope()
    print(
        "[AYCF] Termux UK pilot origins: " + ", ".join(sorted(selected)),
        flush=True,
    )
    print(f"[AYCF] Pilot DB: {os.environ['AYCF_DB_PATH']}", flush=True)

    if sys.argv[1] == "web":
        from app import create_app

        app = create_app()
        host = os.environ.get("AYCF_BIND_HOST", "127.0.0.1")
        port = int(os.environ.get("PORT", "8080"))
        app.run(host=host, port=port)
    else:
        import morning_scan

        result = morning_scan.run(
            force=os.environ.get("AYCF_FORCE_MORNING_SCAN", "false").lower() == "true"
        )
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
