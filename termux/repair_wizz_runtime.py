"""Repair legacy/incorrect Wizz runtime captures before auth validation.

The Multipass availability endpoint is a POST JSON API. Older Chrome captures
could accidentally persist the endpoint-discovery GET request, leaving
request_method=GET and no request template. That makes auth validation fail
with 'no captured request template' even when the browser cookies are valid.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

CONFIG_DIR = Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf")))
RUNTIME_FILE = CONFIG_DIR / "wizz_runtime.json"

DEFAULT_TEMPLATE = {
    "flightType": "OW",
    "origin": "",
    "destination": "",
    "departure": "",
    "arrival": "",
    "intervalSubtype": None,
}


def repair_runtime(path: Path = RUNTIME_FILE) -> bool:
    try:
        runtime = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return False
    if not isinstance(runtime, dict):
        return False

    endpoint = str(runtime.get("availability_url") or "").strip()
    if not (
        endpoint.startswith("https://multipass.wizzair.com/")
        and "/subscriptions/json/availability/" in endpoint
    ):
        return False

    template = runtime.get("request_template")
    method = str(runtime.get("request_method") or "").upper()
    template_type = str(runtime.get("request_template_type") or "").lower()
    if isinstance(template, dict) and method == "POST" and template_type in {"json", "form"}:
        return False

    runtime["request_method"] = "POST"
    runtime["request_template_type"] = "json"
    runtime["request_template"] = dict(DEFAULT_TEMPLATE)
    runtime["template_repaired_at"] = int(time.time())
    runtime["template_repair_reason"] = "normalized availability endpoint to POST JSON"

    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(runtime, indent=2) + "\n", encoding="utf-8")
    os.chmod(temp, 0o600)
    temp.replace(path)
    os.chmod(path, 0o600)
    return True


def main() -> int:
    if repair_runtime():
        print("[AYCF] Repaired captured Wizz runtime: availability endpoint normalized to POST JSON template.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
