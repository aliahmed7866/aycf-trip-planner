"""Repair legacy/incorrect Wizz runtime captures before auth validation.

The Multipass availability endpoint is a POST JSON API. Older Chrome captures
could accidentally persist the endpoint-discovery GET request, leaving
request_method=GET and no request template. That makes auth validation fail
with 'no captured request template' even when the browser cookies are valid.

The Termux env file can set AYCF_CONFIG_DIR after some modules have already
computed their default config path. Repair every plausible runtime path so auth
refresh and the scanner cannot disagree about which runtime metadata is valid.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

DEFAULT_CONFIG_DIR = Path.home() / ".config/aycf"
DEFAULT_ENV_FILE = DEFAULT_CONFIG_DIR / "env"

DEFAULT_TEMPLATE = {
    "flightType": "OW",
    "origin": "",
    "destination": "",
    "departure": "",
    "arrival": "",
    "intervalSubtype": None,
}


def _env_config_dir() -> Path | None:
    """Read AYCF_CONFIG_DIR from the normal Termux env file without sourcing it."""
    try:
        lines = DEFAULT_ENV_FILE.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for raw in lines:
        line = raw.strip()
        if not line.startswith("export AYCF_CONFIG_DIR="):
            continue
        value = line.split("=", 1)[1].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        value = os.path.expandvars(os.path.expanduser(value.strip()))
        if value:
            return Path(value)
    return None


def runtime_paths() -> list[Path]:
    dirs = [DEFAULT_CONFIG_DIR]
    process_dir = os.environ.get("AYCF_CONFIG_DIR")
    if process_dir:
        dirs.append(Path(os.path.expandvars(os.path.expanduser(process_dir))))
    env_dir = _env_config_dir()
    if env_dir is not None:
        dirs.append(env_dir)

    seen: set[str] = set()
    paths: list[Path] = []
    for directory in dirs:
        path = directory / "wizz_runtime.json"
        key = str(path)
        if key not in seen:
            seen.add(key)
            paths.append(path)
    return paths


def repair_runtime(path: Path) -> bool:
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
    repaired = []
    for path in runtime_paths():
        if repair_runtime(path):
            repaired.append(path)
    if repaired:
        print(
            "[AYCF] Repaired captured Wizz runtime: availability endpoint normalized "
            f"to POST JSON template ({len(repaired)} runtime file(s))."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
