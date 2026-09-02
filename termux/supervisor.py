"""Lightweight Termux supervisor for unattended AYCF operation.

The supervisor is safe to wake frequently. It rate-limits network health work,
proactively repairs Wizz authentication only when needed, and launches the
idempotent morning scan only inside the configured publication window.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from termux.run_state import read_status, single_scan_lock, write_status

STATE_DIR = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf")))
SUPERVISOR_FILE = STATE_DIR / "supervisor-status.json"
WIZZ_STATUS_FILE = STATE_DIR / "wizz-session-status.json"
REFRESH = ROOT / "termux" / "auto-refresh-wizz.sh"


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _load(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _save(payload: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["updated_at"] = int(time.time())
    fd, temp_name = tempfile.mkstemp(prefix=".supervisor-status-", suffix=".tmp", dir=str(STATE_DIR))
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), 0o600)
        temp_path.replace(SUPERVISOR_FILE)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _hours() -> set[int]:
    raw = os.environ.get("AYCF_SCAN_WINDOW_UTC", "6,7,8,9,10")
    result = set()
    for value in raw.split(","):
        try:
            hour = int(value.strip())
        except ValueError:
            continue
        if 0 <= hour <= 23:
            result.add(hour)
    return result or {6, 7, 8, 9, 10}


def _run(command: list[str], timeout: int) -> int:
    try:
        return subprocess.run(
            command,
            cwd=str(ROOT),
            env=os.environ.copy(),
            timeout=timeout,
            check=False,
        ).returncode
    except subprocess.TimeoutExpired:
        return 124
    except Exception:
        return 125


def _saved_session_health() -> bool:
    """Validate/repair the encrypted saved session without browser login."""
    try:
        from termux.refresh_wizz_from_chrome import RUNTIME_FILE, _try_saved_session
        if not RUNTIME_FILE.exists():
            return False
        runtime = json.loads(RUNTIME_FILE.read_text(encoding="utf-8"))
        return bool(_try_saved_session(runtime))
    except Exception as exc:
        print(f"[AYCF] Supervisor session health check failed: {exc}", flush=True)
        return False


def main() -> int:
    now = int(time.time())
    sup = _load(SUPERVISOR_FILE)
    health_every = _env_int("AYCF_AUTH_HEALTH_SECONDS", 21600, 1800, 172800)
    repair_cooldown = _env_int("AYCF_AUTH_REPAIR_COOLDOWN_SECONDS", 10800, 900, 86400)
    scan_retry = _env_int("AYCF_SCAN_RETRY_SECONDS", 900, 300, 21600)

    scan_status = read_status()
    if scan_status.get("state") in {"running", "renewing_auth"}:
        # Status can survive an abruptly killed process. Confirm the authoritative
        # flock before suppressing every future scheduled scan.
        with single_scan_lock() as lock_available:
            if not lock_available:
                _save({**sup, "state": "scan_busy", "message": "Existing AYCF work is active."})
                return 0
        write_status(
            "interrupted",
            "Recovered stale scan status after finding no active scan lock.",
            previous_pid=scan_status.get("pid"),
        )
        scan_status = read_status()

    last_health = int(sup.get("last_health_at") or 0)
    health_ok = sup.get("health_ok") is True
    if now - last_health >= health_every:
        health_ok = _saved_session_health()
        sup["last_health_at"] = now
        sup["health_ok"] = health_ok
        if health_ok:
            sup["last_health_success_at"] = now
            sup["state"] = "healthy"
            sup["message"] = "Encrypted Wizz session validated."
        else:
            sup["state"] = "auth_degraded"
            sup["message"] = "Saved Wizz session needs renewal."
        _save(sup)

    if not health_ok:
        last_repair = int(sup.get("last_repair_attempt_at") or 0)
        if now - last_repair >= repair_cooldown:
            sup["last_repair_attempt_at"] = now
            sup["state"] = "repairing_auth"
            sup["message"] = "Attempting automatic Wizz authentication repair."
            _save(sup)
            rc = _run(["bash", str(REFRESH)], timeout=_env_int("AYCF_WIZZ_REFRESH_TIMEOUT", 120, 30, 300))
            sup["last_repair_rc"] = rc
            if rc == 0:
                sup["health_ok"] = True
                sup["last_health_success_at"] = int(time.time())
                sup["state"] = "healthy"
                sup["message"] = "Wizz authentication repaired automatically."
                health_ok = True
            else:
                sup["state"] = "attention_required"
                sup["message"] = f"Automatic Wizz renewal needs attention (exit {rc})."
            _save(sup)

    hour = datetime.now(timezone.utc).hour
    if hour not in _hours():
        _save({**sup, "state": sup.get("state") or "idle", "last_wake_at": now})
        return 0

    if not health_ok:
        _save({**sup, "state": "attention_required", "message": "Morning scan deferred until Wizz authentication is healthy."})
        return 0

    last_scan_attempt = int(sup.get("last_scan_attempt_at") or 0)
    if now - last_scan_attempt < scan_retry:
        return 0

    sup["last_scan_attempt_at"] = now
    sup["state"] = "launching_scan"
    sup["message"] = "Running morning AYCF scan."
    _save(sup)

    # runtime.py/automated_morning owns the actual process lock; the scan itself
    # is PDF+scope idempotent, so repeated scheduler wakes remain cheap.
    rc = _run([sys.executable, str(ROOT / "termux" / "runtime.py"), "morning"], timeout=_env_int("AYCF_SUPERVISOR_SCAN_TIMEOUT", 14400, 300, 21600))
    sup["last_scan_rc"] = rc
    sup["last_scan_finished_at"] = int(time.time())
    if rc == 0:
        sup["state"] = "idle"
        sup["message"] = "Morning scan cycle completed or was already current."
    else:
        sup["state"] = "scan_retry_pending"
        sup["message"] = f"Morning scan exited {rc}; supervisor will retry after cooldown."
    _save(sup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
