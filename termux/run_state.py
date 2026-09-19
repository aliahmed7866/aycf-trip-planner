"""Shared persistent state/locking for Termux scan and web status."""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

STATE_DIR = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf")))
STATUS_FILE = STATE_DIR / "scan-status.json"
LOCK_FILE = STATE_DIR / "scan.lock"


def _schedule_state() -> dict:
    try:
        value = json.loads((STATE_DIR / 'scan-schedule.json').read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_schedule(value: dict) -> None:
    """Keep completion and explicit rerun intent independent of UI status."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.scan-schedule-', suffix='.tmp', dir=STATE_DIR)
    tmp = Path(name)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(value, handle)
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), 0o600)
        tmp.replace(STATE_DIR / 'scan-schedule.json')
    finally:
        tmp.unlink(missing_ok=True)


def request_manual_scan() -> None:
    """An explicit rerun may continue retrying until it succeeds."""
    _save_schedule({**_schedule_state(), 'manual_pending': True})


def automatic_scan_completed_today(status=None, *, now=None) -> bool:
    """Use the same UTC calendar day as the automatic publication window."""
    schedule = _schedule_state()
    if schedule.get('manual_pending'):
        return False
    status = read_status() if status is None else status
    completed_at = schedule.get('completed_at') or 0
    # Adopt a successful scan from an installation predating the receipt file.
    if status.get('state') == 'complete' and status.get('scan_performed') is not False:
        completed_at = max(completed_at, status.get('updated_at') or 0)
    now = time.time() if now is None else now
    return bool(completed_at and completed_at <= now and
                datetime.fromtimestamp(completed_at, timezone.utc).date() ==
                datetime.fromtimestamp(now, timezone.utc).date())


def write_status(state: str, message: str = "", **extra) -> dict:
    from scan_observability import context
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    previous = read_status()
    # Preserve pre-upgrade completion evidence before another status replaces it.
    schedule = _schedule_state()
    if (not schedule.get('completed_at') and previous.get('state') == 'complete'
            and previous.get('scan_performed') is not False and previous.get('updated_at')):
        _save_schedule({**schedule, 'completed_at': previous['updated_at']})
    active = context()
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
    if active:
        if previous.get('run_id') == active['run_id']:
            for key in ('progress', 'pdf_run_id', 'scope_id', 'refresh_policy', 'directory'):
                if key in previous:
                    payload[key] = previous[key]
        payload.update(active)
        if state not in {'running', 'renewing_auth'}:
            payload.update(ended_at=int(time.time()), end_reason=state,
                           elapsed_seconds=max(0, int(time.time()) - active['started_at']))
    payload.update(extra)
    if state == 'complete' and extra.get('scan_performed') is True:
        _save_schedule({'completed_at': payload['updated_at'], 'manual_pending': False})
    elif state == 'complete' and _schedule_state().get('manual_pending'):
        _save_schedule({**_schedule_state(), 'manual_pending': False})
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
def process_lock(path: Path):
    """Nonblocking process lock automatically released when its owner exits."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
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


@contextmanager
def single_scan_lock():
    """Yield True only to the one process allowed to run a scan."""
    with process_lock(LOCK_FILE) as acquired:
        yield acquired
