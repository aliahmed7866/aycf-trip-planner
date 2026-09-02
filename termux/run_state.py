"""Shared persistent state/locking for Termux scan and web status."""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

STATE_DIR = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf")))
STATUS_FILE = STATE_DIR / "scan-status.json"
LOCK_FILE = STATE_DIR / "scan.lock"


def write_status(state: str, message: str = "", **extra) -> dict:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    previous = read_status()
    payload = {
        "state": state,
        "message": str(message or ""),
        "updated_at": int(time.time()),
        "pid": os.getpid(),
    }
    if state == "running" and previous.get("state") != "running":
        payload["started_at"] = int(time.time())
    elif previous.get("started_at"):
        payload["started_at"] = previous["started_at"]
    payload.update(extra)
    fd, temp_name = tempfile.mkstemp(prefix=".scan-status-", suffix=".tmp", dir=str(STATE_DIR))
    tmp = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), 0o600)
        tmp.replace(STATUS_FILE)
        try:
            os.chmod(STATUS_FILE, 0o600)
        except OSError:
            pass
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
    return payload


def read_status() -> dict:
    try:
        value = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


@contextmanager
def single_scan_lock():
    """Yield True only to the one process allowed to run a scan."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    handle = LOCK_FILE.open("a+")
    acquired = False
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError:
            acquired = False
        yield acquired
    finally:
        if acquired:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()
