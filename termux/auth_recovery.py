"""Shared repair budget for scheduled, in-scan and manual authentication repair."""

import os
import json
import time

from termux import run_state


def refresh_timeout() -> int:
    try:
        seconds = int(os.environ.get("AYCF_WIZZ_REFRESH_TIMEOUT", "300"))
    except ValueError:
        seconds = 300
    return max(30, min(900, seconds))


def server_session_recovery_due(*, success=False) -> bool:
    """Persist a bounded HTTP-500 recovery budget across supervisor processes.

    A single failed scan remains an outage. Two failed scans within six hours
    permit a direct-login diagnostic, at most once an hour. Successful scans
    clear the streak but never erase the last-attempt budget.
    """
    path = run_state.STATE_DIR / 'server-session-recovery.json'
    with run_state.process_lock(path.with_suffix('.lock')) as acquired:
        if not acquired:
            return False
        try:
            state = json.loads(path.read_text(encoding='utf-8'))
            failures = int(state.get('failures', 0))
            last_failure = float(state.get('last_failure', 0))
            last_attempt = float(state.get('last_attempt', 0))
        except (OSError, ValueError, TypeError, AttributeError):
            failures, last_failure, last_attempt = 0, 0, 0
        now = time.time()
        if success:
            failures = 0
        else:
            failures = failures + 1 if 0 <= now - last_failure < 21600 else 1
            last_failure = now
        due = (not success and failures >= 2 and
               (not last_attempt or now - last_attempt >= 3600) and
               os.environ.get('AYCF_AUTO_REFRESH_WIZZ_SESSION', 'true').lower() == 'true')
        if due:
            last_attempt = now
        # The lock protects the fixed temporary name; replace is atomic for readers.
        tmp = path.with_suffix('.tmp')
        tmp.write_text(json.dumps({'failures': failures, 'last_failure': last_failure,
                                   'last_attempt': last_attempt}), encoding='utf-8')
        tmp.chmod(0o600)
        tmp.replace(path)
        return due
