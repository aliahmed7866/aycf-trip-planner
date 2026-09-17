"""Durable, process-shared Wizz cooldowns and request start spacing.

Only an actual 429 advances the backoff. Checking a cooldown, creating a new
client, restarting AYCF, or completing a request never clears or extends it.
SQLite transactions allocate only a request start that is currently due.
"""

from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import datetime, timezone
import math
from email.utils import parsedate_to_datetime
import os
from pathlib import Path
import sqlite3
import time


BASE_INTERVAL = 1.0
# Recovery starts gently above baseline; repeated 429 episodes still slow down.
RECOVERY_INTERVALS = (2.0, 3.0, 5.0, 10.0, 20.0, 30.0)
QUIET_SECONDS = 24 * 60 * 60
BASE_COOLDOWN = 15 * 60
MAX_LOCAL_COOLDOWN = 6 * 60 * 60
# Local precautionary budgets, not published Wizz quotas.
NORMAL_REQUESTS_PER_MINUTE = 40
RECOVERY_REQUESTS_PER_MINUTE = 20
REQUEST_HISTORY_SECONDS = 24 * 60 * 60
OPERATIONS = {'availability', 'session', 'stations', 'authentication', 'other'}


def retry_after_details(raw, now=None):
    """Parse Retry-After without retaining its raw, potentially unsafe value."""
    if raw is None or raw == '':
        return {'seconds': None, 'kind': 'missing'}
    if not isinstance(raw, str) or len(raw) > 128:
        return {'seconds': None, 'kind': 'invalid'}
    raw = raw.strip()
    if not raw:
        return {'seconds': None, 'kind': 'missing'}
    try:
        seconds = float(raw)
        if math.isfinite(seconds):
            return {'seconds': max(0.0, seconds), 'kind': 'seconds'}
    except ValueError:
        try:
            when = parsedate_to_datetime(raw)
            when = when.replace(tzinfo=timezone.utc) if when.tzinfo is None else when
            seconds = when.timestamp() - (time.time() if now is None else now)
            if math.isfinite(seconds):
                return {'seconds': max(0.0, seconds), 'kind': 'date'}
        except (ValueError, TypeError, OverflowError):
            pass
    return {'seconds': None, 'kind': 'invalid'}


def _counts(conn, now):
    row = conn.execute('''SELECT
        COALESCE(SUM(started_at > ?), 0), COALESCE(SUM(started_at > ?), 0), COUNT(*)
        FROM request_starts WHERE started_at > ?''',
        (now - 60, now - 900, now - REQUEST_HISTORY_SECONDS)).fetchone()
    return dict(zip(('requests_60s', 'requests_15m', 'requests_24h'), row))


def _budget(conn, status, now):
    limit = RECOVERY_REQUESTS_PER_MINUTE if status['level'] else NORMAL_REQUESTS_PER_MINUTE
    row = conn.execute('SELECT started_at FROM request_starts WHERE started_at > ? '
                       'ORDER BY started_at DESC LIMIT 1 OFFSET ?', (now - 60, limit - 1)).fetchone()
    return {'limit_per_minute': limit, 'wait_seconds': max(0.0, row[0] + 60 - now) if row else 0.0}


def _latest_limit(conn):
    try:
        row = conn.execute('SELECT * FROM rate_limit_events ORDER BY id DESC LIMIT 1').fetchone()
        return dict(row) if row else None
    except sqlite3.OperationalError as exc:
        if 'no such table' not in str(exc):
            raise
        return None


def request_budget_status():
    """Read-only diagnostics, including before the first request or on old DBs."""
    empty = {'requests_60s': 0, 'requests_15m': 0, 'requests_24h': 0,
             'limit_per_minute': NORMAL_REQUESTS_PER_MINUTE, 'wait_seconds': 0.0}
    path = _path()
    if not path.exists():
        return empty
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('BEGIN')
        try:
            row = conn.execute('SELECT * FROM rate_limit WHERE id = 1').fetchone()
            now = time.time()
            status = _status(dict(row) if row else None, now)
            empty['limit_per_minute'] = RECOVERY_REQUESTS_PER_MINUTE if status['level'] else NORMAL_REQUESTS_PER_MINUTE
            return {**_counts(conn, now), **_budget(conn, status, now)}
        except sqlite3.OperationalError as exc:
            if 'no such table' not in str(exc):
                raise
            return empty


def diagnostic_message(event):
    """Only allowlisted labels and finite numbers reach logs or UI."""
    if not isinstance(event, dict):
        return ''
    kind = event.get('retry_after_kind')
    if kind in {'seconds', 'date'}:
        supplied = f"Wizz Retry-After: {_number(event.get('retry_after_seconds')):g}s ({kind})."
    else:
        supplied = {'missing': 'Wizz supplied no Retry-After.', 'invalid': 'Wizz Retry-After was invalid.'}.get(kind, 'Wizz Retry-After was not recorded.')
    source = {'policy': 'AYCF backoff', 'server': 'Wizz Retry-After',
              'both': 'Wizz Retry-After and AYCF backoff', 'existing': 'previously saved cooldown'}.get(event.get('deadline_source'), 'unknown')
    operation = event.get('operation') if event.get('operation') in OPERATIONS else 'other'
    counts = [int(min(1000000, _number(event.get(key)))) for key in ('requests_60s', 'requests_15m', 'requests_24h')]
    return (f"{supplied} Cooldown set by: {source}. Operation: {operation}. "
            f"Managed attempts at the 429: {counts[0]}/minute, {counts[1]}/15 minutes, {counts[2]}/24 hours.")


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
    interval = BASE_INTERVAL if level == 0 else RECOVERY_INTERVALS[min(level - 1, len(RECOVERY_INTERVALS) - 1)]
    return {'blocked': blocked, 'cooldown_until': deadline, 'retry_at': _iso(deadline),
            'effective_request_interval': interval, 'last_rate_limit_at': last, 'level': level}


def rate_limit_message(status=None):
    """Return safe UI text without trusting a provider/exception message."""
    status = status if isinstance(status, dict) else {}
    deadline = _iso(_number(status.get('cooldown_until')))
    if status.get('blocked'):
        if deadline:
            return (f'Wizz rate limit reached. Requests are paused until {deadline}. '
                    'Saved flights remain available; requests will resume at a slower pace. ' +
                    diagnostic_message(status.get('last_limit'))).strip()
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
        conn.execute('BEGIN')
        try:
            row = conn.execute('SELECT * FROM rate_limit WHERE id = 1').fetchone()
        except sqlite3.OperationalError as exc:
            # A second process may have opened the newly created database just
            # before the first writer committed its schema. The send transaction
            # below still serializes with that writer before allowing a request.
            if 'no such table' not in str(exc):
                raise
            row = None
        event = _latest_limit(conn)
    status = _status(dict(row) if row else None)
    if event:
        status['last_limit'] = event
    return status


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
        conn.execute('''CREATE TABLE IF NOT EXISTS request_starts (
            id INTEGER PRIMARY KEY, started_at REAL NOT NULL)''')
        conn.execute('CREATE INDEX IF NOT EXISTS request_starts_time ON request_starts(started_at)')
        conn.execute('''CREATE TABLE IF NOT EXISTS rate_limit_events (
            id INTEGER PRIMARY KEY, observed_at REAL NOT NULL, operation TEXT NOT NULL,
            retry_after_kind TEXT NOT NULL, retry_after_seconds REAL, policy_wait_seconds REAL NOT NULL,
            effective_wait_seconds REAL NOT NULL, deadline_source TEXT NOT NULL,
            requests_60s INTEGER NOT NULL, requests_15m INTEGER NOT NULL, requests_24h INTEGER NOT NULL)''')
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


def record_rate_limit(retry_after=0, *, operation='other', retry_after_kind='unknown'):
    """Record one 429 episode, preserving any longer server-supplied deadline."""
    retry_after = _number(retry_after)
    operation = operation if operation in OPERATIONS else 'other'
    kind = retry_after_kind if retry_after_kind in {'seconds', 'date', 'missing', 'invalid'} else 'unknown'
    with _write_state() as (conn, row):
        now = time.time()
        previous = _status(row, now)
        if previous['blocked']:
            level = max(1, previous['level'])
            deadline = max(previous['cooldown_until'], now + retry_after)
            local_delay = 0
            source = 'server' if now + retry_after > previous['cooldown_until'] else 'existing'
        else:
            level = previous['level'] + 1
            local_delay = min(MAX_LOCAL_COOLDOWN, BASE_COOLDOWN * (2 ** min(level - 1, 5)))
            deadline = now + max(local_delay, retry_after)
            source = 'server' if retry_after > local_delay else ('both' if retry_after == local_delay else 'policy')
        conn.execute('''UPDATE rate_limit SET cooldown_until = ?, last_rate_limit_at = ?, level = ?
                        WHERE id = 1''', (deadline, now, level))
        row.update(cooldown_until=deadline, last_rate_limit_at=now, level=level)
        counts = _counts(conn, now)
        conn.execute('''INSERT INTO rate_limit_events
            (observed_at, operation, retry_after_kind, retry_after_seconds, policy_wait_seconds,
             effective_wait_seconds, deadline_source, requests_60s, requests_15m, requests_24h)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (now, operation, kind, retry_after if kind in {'seconds', 'date'} else None,
             local_delay, deadline - now, source, *counts.values()))
        conn.execute('DELETE FROM rate_limit_events WHERE id NOT IN '
                     '(SELECT id FROM rate_limit_events ORDER BY id DESC LIMIT 20)')
        return {**_status(row, now), 'last_limit': _latest_limit(conn)}


def wait_for_request(min_interval=BASE_INTERVAL):
    """Allocate one due request start globally; abort promptly on any cooldown.

    Waiters never reserve future slots, so a newly recorded cooldown cannot be
    bypassed by previously queued requests. Connections close before each wait.
    """
    minimum = max(0.0, _number(min_interval, BASE_INTERVAL))
    reported_budget_pause = False
    while True:
        with _write_state() as (conn, row):
            now = time.time()
            status = _status(row, now)
            if status['blocked']:
                event = _latest_limit(conn)
                if event:
                    status['last_limit'] = event
                raise WizzRateLimited(status=status)
            budget = _budget(conn, status, now)
            interval = max(minimum, status['effective_request_interval'])
            previous_start = _number(row.get('last_request_at'))
            wait = previous_start + interval - now if previous_start else 0
            wait = max(wait, budget['wait_seconds'])
            if wait <= 0:
                conn.execute('DELETE FROM request_starts WHERE started_at <= ?', (now - REQUEST_HISTORY_SECONDS,))
                conn.execute('INSERT INTO request_starts (started_at) VALUES (?)', (now,))
                conn.execute('UPDATE rate_limit SET last_request_at = ? WHERE id = 1', (now,))
                return status
        if budget['wait_seconds'] >= 2 and not reported_budget_pause:
            print(f"[AYCF] Local request-budget pause: {math.ceil(budget['wait_seconds'])}s; "
                  f"{budget['limit_per_minute']} attempts/minute shared across workers. No new Wizz 429.", flush=True)
            reported_budget_pause = True
        time.sleep(min(1.0, wait))
