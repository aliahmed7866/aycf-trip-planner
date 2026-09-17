"""Exercise the real shared admission path using controlled clocks; no live Wizz calls."""
from datetime import datetime, timezone
from email.utils import format_datetime
import json
from pathlib import Path
import sqlite3
from unittest.mock import Mock

import pytest
import requests

import wizz_rate_limit as limits
from scanner import WizzAYCFClient
from termux import health_ui


@pytest.fixture
def clock(monkeypatch):
    value = [100000.0]
    monkeypatch.setattr(limits.time, 'time', lambda: value[0])
    monkeypatch.setattr(limits.time, 'sleep', lambda seconds: value.__setitem__(0, value[0] + seconds))
    return value


@pytest.mark.parametrize('recovery,limit,interval', [(False, 40, 1), (True, 20, 2)])
def test_rolling_budget_waits_and_resumes_without_fabricating_429(clock, recovery, limit, interval):
    if recovery:
        state = limits.record_rate_limit()
        clock[0] = state['cooldown_until']
    start = clock[0]
    for _ in range(limit):
        limits.wait_for_request()
    assert clock[0] == start + (limit - 1) * interval
    snapshot = limits.rate_limit_status()
    budget = limits.request_budget_status()
    assert budget['requests_60s'] == limit
    assert budget['limit_per_minute'] == limit
    assert budget['wait_seconds'] == 60 - (limit - 1) * interval
    limits.wait_for_request()
    assert clock[0] == start + 60
    assert limits.request_budget_status()['requests_60s'] == limit
    assert limits.rate_limit_status()['level'] == snapshot['level']
    assert limits.rate_limit_status()['cooldown_until'] == snapshot['cooldown_until']


def test_a_new_429_interrupts_budget_wait_before_any_new_admission(clock, monkeypatch):
    for _ in range(40):
        limits.wait_for_request()
    before = limits.request_budget_status()['requests_24h']
    def sleep(seconds):
        clock[0] += seconds
        limits.record_rate_limit(3600, retry_after_kind='seconds')
    monkeypatch.setattr(limits.time, 'sleep', sleep)
    with pytest.raises(limits.WizzRateLimited):
        limits.wait_for_request()
    assert limits.request_budget_status()['requests_24h'] == before
    assert limits.rate_limit_status()['blocked']


def test_old_database_is_readable_then_migrates_without_losing_deadline(clock):
    path = limits._path()
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE rate_limit (id INTEGER PRIMARY KEY, cooldown_until REAL, '
                     'last_rate_limit_at REAL, level INTEGER, last_request_at REAL)')
        conn.execute('INSERT INTO rate_limit VALUES (1, ?, ?, 1, ?)', (clock[0]+900, clock[0], clock[0]))
    before = path.read_bytes()
    assert limits.request_budget_status()['limit_per_minute'] == 20
    assert limits.rate_limit_status()['blocked']
    assert path.read_bytes() == before
    with pytest.raises(limits.WizzRateLimited):
        limits.wait_for_request()
    assert limits.rate_limit_status()['cooldown_until'] == clock[0] + 900
    clock[0] += 900
    limits.wait_for_request()
    assert limits.request_budget_status()['requests_24h'] == 1


def test_retention_counts_boundaries_and_read_only_health(clock, monkeypatch):
    with limits._write_state() as (conn, _):
        for age in (1, 59, 60, 899, 900, 86399, 86400, 90000):
            conn.execute('INSERT INTO request_starts(started_at) VALUES (?)', (clock[0]-age,))
    before = limits._path().read_bytes()
    monkeypatch.setattr(health_ui, '_json_file', lambda _: {})
    snapshot = health_ui.rate_limit_summary({}, {})
    assert snapshot['request_budget']['requests_60s'] == 2
    assert snapshot['request_budget']['requests_15m'] == 4
    assert snapshot['request_budget']['requests_24h'] == 6
    assert not snapshot['blocked']
    assert limits._path().read_bytes() == before
    limits.wait_for_request()
    with sqlite3.connect(limits._path()) as conn:
        assert conn.execute('SELECT COUNT(*) FROM request_starts').fetchone()[0] == 7


@pytest.mark.parametrize('raw,kind,seconds', [(None, 'missing', None), ('', 'missing', None),
    ('Bearer secret-key', 'invalid', None), ('nan', 'invalid', None), ('inf', 'invalid', None),
    ('120', 'seconds', 120), ('-1', 'seconds', 0)])
def test_retry_after_parser_retains_only_safe_values(clock, raw, kind, seconds):
    assert limits.retry_after_details(raw) == {'kind': kind, 'seconds': seconds}


def test_http_date_and_long_server_delay_have_correct_provenance(clock):
    header = format_datetime(datetime.fromtimestamp(clock[0] + 90000, timezone.utc), usegmt=True)
    parsed = limits.retry_after_details(header)
    assert parsed == {'kind': 'date', 'seconds': 90000}
    status = limits.record_rate_limit(parsed['seconds'], operation='availability', retry_after_kind=parsed['kind'])
    event = status['last_limit']
    assert event['deadline_source'] == 'server'
    assert event['policy_wait_seconds'] == 900
    assert event['effective_wait_seconds'] == 90000
    assert 'Wizz Retry-After: 90000s (date)' in limits.rate_limit_message(status)
    assert limits.rate_limit_status() == status


def test_transport_counts_request_and_logs_policy_not_fake_server_wait(clock):
    client = WizzAYCFClient({})
    client._throttle = lambda: None
    client.dynamic_url = 'https://example.invalid/private?key=secret'
    response = requests.Response()
    response.status_code = 429
    response.headers['Retry-After'] = 'secret-token'
    client.http.request = Mock(return_value=response)
    with pytest.raises(limits.WizzRateLimited) as error:
        client._request('POST', client.dynamic_url, json={'password': 'secret-password'})
    event = error.value.status['last_limit']
    assert event['operation'] == 'availability'
    assert event['retry_after_kind'] == 'invalid'
    assert event['retry_after_seconds'] is None
    assert event['requests_60s'] == event['requests_15m'] == event['requests_24h'] == 1
    assert event['deadline_source'] == 'policy'
    assert 'AYCF backoff' in str(error.value)
    assert 'secret' not in str(error.value) and 'secret' not in json.dumps(event)
    assert b'secret' not in limits._path().read_bytes()
    client.http.request.assert_called_once()


def test_authentication_shares_budget_and_records_its_operation(clock, monkeypatch):
    from termux import refresh_wizz_from_chrome as chrome
    monkeypatch.setattr(chrome, 'wait_for_request', limits.wait_for_request)
    for _ in range(40):
        limits.wait_for_request()
    response = requests.Response()
    response.status_code = 429
    response.headers['Retry-After'] = '120'
    send = Mock(return_value=response)
    with pytest.raises(limits.WizzRateLimited):
        chrome._auth_request(send, 'https://example.invalid/secret')
    assert clock[0] == 100060
    event = limits.rate_limit_status()['last_limit']
    assert event['operation'] == 'authentication'
    assert event['retry_after_seconds'] == 120
    assert event['requests_24h'] == 41
    send.assert_called_once()


def test_unknown_operation_and_event_retention(clock):
    for _ in range(25):
        limits.record_rate_limit(operation='https://secret.example/token', retry_after_kind='secret')
    with sqlite3.connect(limits._path()) as conn:
        assert conn.execute('SELECT COUNT(*) FROM rate_limit_events').fetchone()[0] == 20
    event = limits.rate_limit_status()['last_limit']
    assert event['operation'] == 'other' and event['retry_after_kind'] == 'unknown'
    assert b'secret' not in limits._path().read_bytes()


def test_fresh_processes_share_the_existing_budget():
    import time
    from tests.test_persistent_rate_limits import _processes
    seeded = time.time() - 58
    with limits._write_state() as (conn, _):
        conn.executemany('INSERT INTO request_starts(started_at) VALUES (?)', [(seeded,)] * 40)
        conn.execute('UPDATE rate_limit SET last_request_at=? WHERE id=1', (seeded,))
    code = '''import contextlib, io, json, sys
import wizz_rate_limit as limits
sys.stdin.read(1)
with contextlib.redirect_stdout(io.StringIO()):
    limits.wait_for_request()
print(json.dumps(True))
'''
    assert _processes(code, [0, 1, 2]) == [True, True, True]
    with sqlite3.connect(limits._path()) as conn:
        admissions = [r[0] for r in conn.execute('SELECT started_at FROM request_starts WHERE started_at > ? ORDER BY started_at', (seeded,))]
    assert len(admissions) == 3
    assert admissions[0] >= seeded + 60
    assert all(b - a >= 1 for a, b in zip(admissions, admissions[1:]))


def test_plan_estimate_accounts_for_budget_and_recovery_without_changing_scope(clock):
    from scan_scope import scan_plan, default_scope, scope_fingerprint
    scope = default_scope()
    before = scope_fingerprint(scope)
    normal = scan_plan([('Liverpool', 'Budapest')], scope)
    assert normal['request_units'] > 0
    assert normal['estimated_seconds'] >= round(normal['request_units'] * 1.5)
    limits.record_rate_limit()
    recovery = scan_plan([('Liverpool', 'Budapest')], scope)
    assert recovery['estimated_seconds'] >= recovery['request_units'] * 3
    assert recovery['request_units'] == normal['request_units']
    assert scope_fingerprint(scope) == before
