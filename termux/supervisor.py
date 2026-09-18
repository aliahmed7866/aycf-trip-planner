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

from termux.run_state import (read_status, single_scan_lock, write_status, process_lock,
                             automatic_scan_completed_today)
from termux.auth_recovery import refresh_timeout
from scanner import WizzRequestRejected
from wizz_rate_limit import WizzRateLimited, rate_limit_status, rate_limit_message, record_rate_limit

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
        return bool(_try_saved_session(runtime, refresh_directory=True))
    except WizzRateLimited:
        raise
    except WizzRequestRejected as exc:
        write_status('request_rejected', str(exc), scan_performed=False, http_status=418)
        return False
    except Exception as exc:
        print(f"[AYCF] Supervisor session health check failed: {exc}", flush=True)
        return False


def main() -> int:
    # Scheduler and manual health checks can overlap; keep repair/status work single-owner.
    with process_lock(STATE_DIR / "supervisor.lock") as acquired:
        if not acquired:
            return 0
        return _run_cycle()


def _defer_rate_limit(sup, *, exc=None):
    """Keep the provider deadline authoritative, including after process exit."""
    limit = rate_limit_status()
    if exc is not None and not limit['blocked']:
        limit = record_rate_limit(0)
    if not limit['blocked']:
        return False
    message = rate_limit_message(limit)
    details = {key: limit[key] for key in ('cooldown_until', 'retry_at', 'effective_request_interval')}
    # Preserve the existing health result: throttling is not proof of expiry.
    write_status('rate_limited', message, scan_performed=False, http_status=429,
                 resume_scan=bool(sup.get('scan_pending')), **details)
    _save({**sup, 'state': 'rate_limited', 'message': message, **details})
    return True


def _run_cycle() -> int:
    now = int(time.time())
    sup = _load(SUPERVISOR_FILE)
    sup["last_wake_at"] = now
    _save(sup)
    health_every = _env_int("AYCF_AUTH_HEALTH_SECONDS", 21600, 1800, 172800)
    repair_cooldown = _env_int("AYCF_AUTH_REPAIR_COOLDOWN_SECONDS", 900, 900, 86400)
    scan_retry = _env_int("AYCF_SCAN_RETRY_SECONDS", 900, 300, 21600)

    scan_status = read_status()
    if scan_status.get("state") in {"request_rejected", "request_repair_required"}:
        _save({**sup, "state": scan_status["state"], "scan_pending": False,
               "message": scan_status.get("message", "Wizz requests paused; manual review required.")})
        return 0
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
    if (scan_status.get("state") in {"failed", "partial", "auth_failed", "attention_required", "service_unavailable", "interrupted", "already_running"}
            or (scan_status.get('state') == 'rate_limited' and scan_status.get('resume_scan', True))):
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

    completed_today = automatic_scan_completed_today(scan_status, now=now)
    explicit_fresh = scan_status.get('fresh_reset_pending') or scan_status.get('fresh_pending')
    in_window = datetime.fromtimestamp(now, timezone.utc).hour in _hours()
    if completed_today and not explicit_fresh:
        sup['scan_pending'] = False
        sup.pop('pending_since', None)
        sup['state'] = 'idle'
        sup['message'] = 'A scan already completed today (UTC); next automatic scan is tomorrow. Manual reruns remain available.'
    elif in_window:
        sup["scan_pending"] = True
        sup.setdefault("pending_since", now)
    if _defer_rate_limit(sup):
        return 0
    if scan_status.get('state') == 'service_unavailable' and now < int(scan_status.get('retry_at_epoch') or 0):
        _save({**sup, 'state': 'scan_retry_pending', 'message': scan_status.get('message', ''),
               'retry_at': scan_status.get('retry_at')})
        return 0
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
        try:
            health_ok = _saved_session_health()
        except WizzRateLimited as exc:
            _defer_rate_limit(sup, exc=exc)
            return 0
        if _defer_rate_limit(sup):
            return 0
        if read_status().get('state') == 'request_rejected':
            _save({**sup, 'state': 'request_rejected', 'scan_pending': False,
                   'message': read_status().get('message', 'Wizz rejected the health probe.')})
            return 0
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
            if _defer_rate_limit(sup):
                return 0
            if read_status().get('state') == 'request_rejected':
                _save({**sup, 'state': 'request_rejected', 'scan_pending': False,
                       'message': read_status().get('message', 'Wizz rejected the repair probe.')})
                return 0
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
    # Rate-limited work becomes eligible at its persisted provider deadline.
    # Other failures retain the general scan retry interval.
    if scan_status.get('state') != 'rate_limited' and now - last_scan_attempt < scan_retry:
        return 0

    sup["last_scan_attempt_at"] = now
    sup["state"] = "launching_scan"
    sup["message"] = "Running morning AYCF scan."
    _save(sup)

    # The child rechecks daily completion under the scan lock, covering a manual
    # completion between this scheduling decision and the worker acquiring it.
    rc = _run([sys.executable, str(ROOT / "termux" / "runtime.py"), "morning"], timeout=_env_int("AYCF_SUPERVISOR_SCAN_TIMEOUT", 14400, 300, 21600))
    sup["last_scan_rc"] = rc
    sup["last_scan_finished_at"] = int(time.time())
    outcome = read_status()
    sup["last_scan_outcome"] = outcome
    if outcome.get('state') == 'rate_limited':
        sup['scan_pending'] = True
        sup['last_scan_failure'] = outcome
        _defer_rate_limit(sup)
        return 0
    # Duplicate launches and a stale successful status are not completion proof.
    if outcome.get("state") in {"request_rejected", "request_repair_required"}:
        sup["scan_pending"] = False
        sup.pop("pending_since", None)
        sup["state"] = outcome["state"]
        sup["message"] = outcome.get("message", "Wizz requests paused; manual review required.")
    elif rc == 0 and outcome.get("state") == "complete" and int(outcome.get("updated_at") or 0) >= now:
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
