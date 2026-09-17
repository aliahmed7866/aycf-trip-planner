"""Durable, process-shared Wizz cooldowns and request start spacing.

Only an actual 429 advances the backoff. Checking a cooldown, creating a new
client, restarting AYCF, or completing a request never clears or extends it.
SQLite transactions allocate only a request start that is currently due.
"""

from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import datetime, timezone
import math
import os
from pathlib import Path
import sqlite3
import time


BASE_INTERVAL = 1.0
QUIET_SECONDS = 24 * 60 * 60
BASE_COOLDOWN = 15 * 60
MAX_LOCAL_COOLDOWN = 6 * 60 * 60


def _path():
    explicit = os.environ.get('AYCF_WIZZ_RATE_LIMIT_PATH')
    if explicit:
        return Path(explicit).expanduser().resolve()
    state = os.environ.get('AYCF_STATE_DIR')
    root = Path(state).expanduser() if state else Path.home() / '.local' / 'share' / 'aycf'
    return (root / 'wizz-rate-limit.sqlite3').resolve()


def _number(value, default=0.0):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _iso(timestamp):
    try:
        return datetime.fromtimestamp(timestamp, timezone.utc).isoformat() if timestamp > 0 else None
    except (OverflowError, OSError, ValueError):
        return None


def _status(row=None, now=None):
    now = time.time() if now is None else now
    row = row or {}
    deadline = _number(row.get('cooldown_until'))
    last = _number(row.get('last_rate_limit_at'))
    blocked = now < deadline
    level = int(_number(row.get('level')))
    if not blocked and now - last >= QUIET_SECONDS:
        level = 0
    interval = BASE_INTERVAL if level == 0 else (5.0, 10.0, 20.0, 30.0)[min(level - 1, 3)]
    return {'blocked': blocked, 'cooldown_until': deadline, 'retry_at': _iso(deadline),
            'effective_request_interval': interval, 'last_rate_limit_at': last, 'level': level}


def rate_limit_message(status=None):
    """Return safe UI text without trusting a provider/exception message."""
    status = status if isinstance(status, dict) else {}
    deadline = _iso(_number(status.get('cooldown_until')))
    if status.get('blocked'):
        if deadline:
            return (f'Wizz rate limit reached. Requests are paused until {deadline}. '
                    'Saved flights remain available; requests will resume at a slower pace.')
        return 'Wizz rate limit reached. Requests are paused; saved flights remain available.'
    if not status:
        return 'Wizz rate limit reached. Requests are paused; saved flights remain available.'
    interval = max(BASE_INTERVAL, _number(status.get('effective_request_interval'), BASE_INTERVAL))
    return f'Wizz requests are available, with at least {interval:g} seconds between requests.'


class WizzRateLimited(RuntimeError):
    def __init__(self, message=None, *, status=None):
        # Preserve old message-only call sites without retaining arbitrary text.
        self.status = dict(status) if isinstance(status, dict) else {}
        super().__init__(rate_limit_message(self.status))


def rate_limit_status():
    path = _path()
    if not path.exists():
        return _status()
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as conn:
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute('SELECT * FROM rate_limit WHERE id = 1').fetchone()
        except sqlite3.OperationalError as exc:
            # A second process may have opened the newly created database just
            # before the first writer committed its schema. The send transaction
            # below still serializes with that writer before allowing a request.
            if 'no such table' not in str(exc):
                raise
            row = None
    return _status(dict(row) if row else None)


@contextmanager
def _write_state():
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('''CREATE TABLE IF NOT EXISTS rate_limit (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cooldown_until REAL NOT NULL DEFAULT 0,
            last_rate_limit_at REAL NOT NULL DEFAULT 0,
            level INTEGER NOT NULL DEFAULT 0,
            last_request_at REAL NOT NULL DEFAULT 0
        )''')
        conn.execute('INSERT OR IGNORE INTO rate_limit (id) VALUES (1)')
        yield conn, dict(conn.execute('SELECT * FROM rate_limit WHERE id = 1').fetchone())
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def check_cooldown():
    status = rate_limit_status()
    if status['blocked']:
        raise WizzRateLimited(status=status)
    return status


def record_rate_limit(retry_after=0):
    """Record one 429 episode, preserving any longer server-supplied deadline."""
    retry_after = _number(retry_after)
    with _write_state() as (conn, row):
        now = time.time()
        previous = _status(row, now)
        if previous['blocked']:
            level = max(1, previous['level'])
            deadline = max(previous['cooldown_until'], now + retry_after)
        else:
            level = previous['level'] + 1
            local_delay = min(MAX_LOCAL_COOLDOWN, BASE_COOLDOWN * (2 ** min(level - 1, 5)))
            deadline = now + max(local_delay, retry_after)
        conn.execute('''UPDATE rate_limit SET cooldown_until = ?, last_rate_limit_at = ?, level = ?
                        WHERE id = 1''', (deadline, now, level))
        row.update(cooldown_until=deadline, last_rate_limit_at=now, level=level)
        return _status(row, now)


def wait_for_request(min_interval=BASE_INTERVAL):
    """Allocate one due request start globally; abort promptly on any cooldown.

    Waiters never reserve future slots, so a newly recorded cooldown cannot be
    bypassed by previously queued requests. Connections close before each wait.
    """
    minimum = max(0.0, _number(min_interval, BASE_INTERVAL))
    while True:
        with _write_state() as (conn, row):
            now = time.time()
            status = _status(row, now)
            if status['blocked']:
                raise WizzRateLimited(status=status)
            interval = max(minimum, status['effective_request_interval'])
            previous_start = _number(row.get('last_request_at'))
            wait = previous_start + interval - now if previous_start else 0
            if wait <= 0:
                conn.execute('UPDATE rate_limit SET last_request_at = ? WHERE id = 1', (now,))
                return status
        time.sleep(min(1.0, wait))
