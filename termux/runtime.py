"""Run AYCF web/morning processes with Wizz runtime metadata captured from Chrome."""

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scanner  # noqa: E402

CONFIG_PATH = Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf"))) / "wizz_runtime.json"


def _load_runtime():
    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _patch_scanner(runtime):
    endpoint = str(runtime.get("availability_url") or "").strip()
    station_ids = runtime.get("station_ids") if isinstance(runtime.get("station_ids"), dict) else {}
    if not endpoint:
        return

    original_init = scanner.WizzAYCFClient.__init__

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.dynamic_url = endpoint
        for key, value in station_ids.items():
            if key and value:
                self.station_ids[str(key).casefold()] = str(value).upper()

    scanner.WizzAYCFClient.__init__ = patched_init


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in {"web", "morning"}:
        raise SystemExit("Usage: python termux/runtime.py web|morning")

    _patch_scanner(_load_runtime())

    if sys.argv[1] == "web":
        from app import create_app

        app = create_app()
        host = os.environ.get("AYCF_BIND_HOST", "127.0.0.1")
        port = int(os.environ.get("PORT", "8080"))
        app.run(host=host, port=port)
    else:
        import morning_scan

        result = morning_scan.run(force=os.environ.get("AYCF_FORCE_MORNING_SCAN", "false").lower() == "true")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
