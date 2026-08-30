"""Repair legacy/incorrect Wizz runtime captures before auth validation.

This migration helper repairs any already-canonical Multipass availability URL
that was saved as GET/no-template. The refresh process also performs the same
normalization in-process and can rebuild a template after endpoint rediscovery,
so this helper is now a belt-and-suspenders migration rather than a correctness
dependency.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from termux.wizz_runtime import normalize_runtime, write_runtime  # noqa: E402

DEFAULT_CONFIG_DIR = Path.home() / ".config/aycf"
DEFAULT_ENV_FILE = DEFAULT_CONFIG_DIR / "env"


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

    normalized, repaired = normalize_runtime(runtime)
    if not repaired:
        return False
    write_runtime(path, normalized)
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
