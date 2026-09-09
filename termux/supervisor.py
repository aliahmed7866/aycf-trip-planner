"""Lightweight Termux supervisor for unattended AYCF operation.

The supervisor is safe to wake frequently. It rate-limits network health work,
proactively repairs Wizz authentication only when needed, and launches the
idempotent morning scan in the publication window, with unfinished work retried
outside that window until a scan actually completes.
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

from termux.env_loader import load_termux_env
load_termux_env()

from termux.run_state import read_status, single_scan_lock, write_status, process_lock
from termux.auth_recovery import refresh_timeout

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
    # Scheduler and manual health checks can overlap; keep repair/status work single-owner.
    with process_lock(STATE_DIR / "supervisor.lock") as acquired:
        if not acquired:
            return 0
        return _run_cycle()


def _run_cycle() -> int:
    now = int(time.time())
    sup = _load(SUPERVISOR_FILE)
    sup["last_wake_at"] = now
    _save(sup)
    health_every = _env_int("AYCF_AUTH_HEALTH_SECONDS", 21600, 1800, 172800)
    repair_cooldown = _env_int("AYCF_AUTH_REPAIR_COOLDOWN_SECONDS", 900, 900, 86400)
    scan_retry = _env_int("AYCF_SCAN_RETRY_SECONDS", 900, 300, 21600)

    scan_status = read_status()
    with single_scan_lock() as lock_available:
        if not lock_available:
            _save({**sup, "state": "scan_busy", "message": "Existing AYCF work is active."})
            return 0
    if scan_status.get("state") in {"running", "renewing_auth"}:
        # Status can survive an abruptly killed process. Confirm the authoritative
        # flock before suppressing every future scheduled scan.
        write_status(
            "interrupted",
            "Recovered stale scan status after finding no active scan lock.",
            previous_pid=scan_status.get("pid"),
        )
        scan_status = read_status()

    # Adopt failures left by scheduled or manual runs, including older versions
    # that did not persist a pending flag. A later manual completion satisfies it.
    scan_at = int(scan_status.get("updated_at") or 0)
    if scan_status.get("state") in {"failed", "auth_failed", "attention_required", "service_unavailable", "interrupted", "already_running"}:
        sup.setdefault("pending_since", scan_at or now)
        sup["scan_pending"] = True
        sup["last_scan_failure"] = scan_status
    elif scan_status.get("state") == "complete" and (
        not sup.get("scan_pending") or scan_at >= int(sup.get("pending_since") or now)
    ):
        sup["scan_pending"] = False
        sup.pop("pending_since", None)
        sup["state"] = "idle"
        sup["message"] = scan_status.get("message", "Scan completed.")

    in_window = datetime.now(timezone.utc).hour in _hours()
    if in_window:
        sup["scan_pending"] = True
        sup.setdefault("pending_since", now)
    _save(sup)

    last_health = int(sup.get("last_health_at") or 0)
    health_ok = sup.get("health_ok") is True
    # A confirmed scan auth failure supersedes a cached healthy result. A newer
    # successful repair (including the manual button) supersedes that failure.
    auth_failed_at = scan_at if scan_status.get("state") in {"auth_failed", "attention_required"} else 0
    if auth_failed_at >= int(sup.get("last_health_success_at") or 0) and auth_failed_at:
        health_ok = False
    wizz = _load(WIZZ_STATUS_FILE)
    wizz_at = int(wizz.get("updated_at") or 0)
    if wizz.get("ok") is True and wizz_at > max(last_health, auth_failed_at):
        health_ok = True
        last_health = wizz_at
        sup["last_health_at"] = wizz_at
        sup["last_health_success_at"] = wizz_at
    sup["health_ok"] = health_ok
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
            rc = _run(["bash", str(REFRESH)], timeout=refresh_timeout())
            sup["last_repair_rc"] = rc
            if rc == 0:
                sup["health_ok"] = True
                sup["last_health_success_at"] = int(time.time())
                sup["last_health_at"] = int(time.time())
                sup["state"] = "healthy"
                sup["message"] = "Wizz authentication repaired automatically."
                health_ok = True
            else:
                sup["state"] = "attention_required"
                sup["message"] = f"Automatic Wizz renewal needs attention (exit {rc})."
            _save(sup)

    if not sup.get("scan_pending"):
        _save({**sup, "state": sup.get("state") or "idle", "last_wake_at": now})
        return 0

    if not health_ok:
        _save({**sup, "state": "scan_retry_pending", "message": "Scan pending; authentication repair will retry after cooldown."})
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
    outcome = read_status()
    sup["last_scan_outcome"] = outcome
    # Duplicate launches and a stale successful status are not completion proof.
    if rc == 0 and outcome.get("state") == "complete" and int(outcome.get("updated_at") or 0) >= now:
        sup["scan_pending"] = False
        sup.pop("pending_since", None)
        sup["state"] = "idle"
        sup["message"] = outcome.get("message", "Scan completed.")
    else:
        sup["scan_pending"] = True
        sup["last_scan_failure"] = outcome
        if outcome.get("state") in {"auth_failed", "attention_required"}:
            sup["health_ok"] = False
        sup["state"] = "scan_retry_pending"
        sup["message"] = f"Scan unfinished (exit {rc}); will retry after cooldown, including outside the morning window."
    _save(sup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
