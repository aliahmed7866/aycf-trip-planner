"""Regressions from the 17 September scan; all provider traffic is mocked."""
from datetime import date, datetime, timedelta
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

from cache_db import ScanCacheDB
from scanner import Flight
from scan_reuse import reuse_check
from termux import automated_morning, run_state, supervisor
from tests.test_rate_limit_scan_preservation import scan_fixture
import tiered_morning


@pytest.fixture
def reuse_db(tmp_path):
    db = ScanCacheDB(str(tmp_path / 'reuse.sqlite3'))
    day = date.today() + timedelta(days=1)
    stamp = datetime.now().isoformat()
    for run in ('old', 'new'):
        db.upsert_pdf_run(run, stamp, day.isoformat(), day.isoformat(), 1)
    pairs = [('London Gatwick', 'Budapest'), ('London Luton', 'Budapest')]
    start = datetime.combine(day, datetime.min.time()) + timedelta(hours=12)
    flights = [Flight(a, b, 'W' + str(i), start, start + timedelta(hours=2), '', '')
               for i, (a, b) in enumerate(pairs)]
    db.replace_route_check('old', 'London', 'Budapest', day, flights, checked_pairs=pairs)
    return db, day, pairs


def test_scope_change_reuses_only_retained_airports_without_rejuvenating_timestamps(reuse_db):
    db, day, pairs = reuse_db
    before = db.route_check_info('old', 'London', 'Budapest', day)['fetched_at']
    assert reuse_check(db, 'new', 'London', 'Budapest', day, pairs[1:], lambda _: 1800) == 1
    assert db.route_check_info('new', 'London', 'Budapest', day)['fetched_at'] == before
    flights = db.get_flights('London', 'Budapest', day, 'new')
    assert [flight.origin for flight in flights] == ['London Luton']
    with db.connect() as conn:
        assert conn.execute("SELECT fetched_at FROM route_flights WHERE pdf_run_id='new'").fetchone()[0] == before


@pytest.mark.parametrize('invalid', ['stale', 'partial', 'legacy', 'new_airport', 'other_pdf', 'missing_physical', 'full_refresh'])
def test_unproven_or_stale_coverage_is_never_reused(reuse_db, invalid):
    db, day, pairs = reuse_db
    with db.connect() as conn:
        if invalid == 'stale':
            conn.execute('UPDATE route_checks SET fetched_at=?', ((datetime.utcnow() - timedelta(days=1)).isoformat(),))
        if invalid == 'partial':
            conn.execute('UPDATE route_checks SET complete=0')
        if invalid == 'legacy':
            conn.execute('UPDATE route_checks SET request_pairs_json=NULL')
        if invalid == 'other_pdf':
            conn.execute("UPDATE pdf_runs SET generated_at='other' WHERE run_id='new'")
        if invalid == 'missing_physical':
            conn.execute("UPDATE route_flights SET physical_origin=NULL")
    if invalid == 'new_airport':
        pairs = [('Liverpool', 'Budapest')]
    assert reuse_check(db, 'new', 'London', 'Budapest', day, pairs,
                       lambda _: 0 if invalid == 'full_refresh' else 1800) is None
    assert not db.route_checked('new', 'London', 'Budapest', day)


def test_verified_empty_coverage_can_be_reused(reuse_db):
    db, day, pairs = reuse_db
    db.replace_route_check('old', 'London', 'Budapest', day, [], checked_pairs=pairs)
    assert reuse_check(db, 'new', 'London', 'Budapest', day, pairs, lambda _: 1800) == 0
    assert db.route_checked('new', 'London', 'Budapest', day)


def test_other_scope_cannot_replace_current_partial_observations(reuse_db):
    db, day, pairs = reuse_db
    db.replace_route_check('new', 'London', 'Budapest', day, [], complete=False)
    assert reuse_check(db, 'new', 'London', 'Budapest', day, pairs, lambda _: 1800) is None
    assert not db.route_checked('new', 'London', 'Budapest', day)


def test_worker_keeps_successful_checks_after_old_ttl_expires(scan_fixture):
    state = scan_fixture()
    state.failing = False
    result = tiered_morning._run_locked(state.db)
    assert result['ok']
    state.requests.clear()
    assert tiered_morning._run_locked(state.db)['state'] == 'already_current'
    assert not state.requests
    with state.db.connect() as conn:
        conn.execute('UPDATE route_checks SET fetched_at=?', (datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),))
    result = tiered_morning._run_locked(state.db)
    assert result['state'] == 'already_current'
    assert not state.requests


@pytest.mark.parametrize('force', [False, True])
def test_resume_keeps_earlier_checks_even_when_ttl_elapsed(scan_fixture, monkeypatch, force):
    state = scan_fixture()
    state.failing = False
    with state.db.connect() as conn:
        conn.execute('UPDATE route_checks SET fetched_at=?', (datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),))
    monkeypatch.setenv('AYCF_MANUAL_REFRESH_TTL_SECONDS', '0')
    result = tiered_morning._run_locked(state.db, force=force)
    assert result['resumed_checks'] == state.completed
    assert state.requests == [state.fail_pair]


def test_interrupted_refresh_does_not_claim_stale_unfetched_checks_complete(scan_fixture):
    import wizz_rate_limit
    state = scan_fixture()
    state.failing = False
    assert tiered_morning._run_locked(state.db)['ok']
    with state.db.connect() as conn:
        conn.execute('UPDATE route_checks SET fetched_at=?', (datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),))
    with state.db.connect() as conn:
        yesterday = (datetime.utcnow() - timedelta(days=1)).isoformat()
        conn.execute('UPDATE route_checks SET fetched_at=?', (yesterday,))
        conn.execute('UPDATE daily_airport_checks SET checked_at=?', (yesterday,))
    state.failing = True
    with pytest.raises(wizz_rate_limit.WizzRateLimited) as stopped:
        tiered_morning._run_locked(state.db)
    assert not state.db.route_checked(state.run_id, 'Liverpool', 'Budapest', state.day)
    state.clock[0] = stopped.value.status['cooldown_until']
    state.failing = False
    state.requests.clear()
    result = tiered_morning._run_locked(state.db)
    assert result['ok']
    assert state.requests == [state.fail_pair]


def test_server_failure_preserves_verified_part_of_multi_airport_group(scan_fixture, monkeypatch):
    state = scan_fixture('London')
    state.failing = False
    original_factory = tiered_morning.CapturedRequestWizzClient
    response = requests.Response()
    response.status_code = 503
    class Client(original_factory):
        def check(self, a, b, day):
            if (a, b) == state.fail_pair:
                raise requests.HTTPError('Fixture outage', response=response)
            return super().check(a, b, day)
    monkeypatch.setattr(tiered_morning, 'CapturedRequestWizzClient', Client)
    result = tiered_morning._run_locked(state.db)
    assert result['state'] == 'partial' and result['unknown_checks'] == 1
    flights = state.db.get_flights('London', 'Budapest', state.day, state.run_id)
    assert flights and len(flights) == len(state.pending[6]) - 1
    assert not state.db.route_checked(state.run_id, 'London', 'Budapest', state.day)
    assert not state.db.get_pdf_run(state.run_id)['scanned_at']
    monkeypatch.setattr(tiered_morning, 'CapturedRequestWizzClient', original_factory)
    result = tiered_morning._run_locked(state.db)
    assert result['ok'] and result['resumed_checks'] == state.completed
    assert len(state.db.get_flights('London', 'Budapest', state.day, state.run_id)) == len(state.pending[6])


def test_fresh_reset_callback_runs_only_after_verified_preflight(scan_fixture, monkeypatch):
    state = scan_fixture()
    reset = Mock()
    response = requests.Response()
    response.status_code = 500
    monkeypatch.setattr(tiered_morning, 'verify_scan_requests', Mock(side_effect=requests.HTTPError(response=response)))
    with pytest.raises(requests.HTTPError):
        tiered_morning._run_locked(state.db, before_scan=reset)
    reset.assert_not_called()


@pytest.mark.parametrize('state', ['service_unavailable', 'partial'])
def test_supervisor_waits_for_service_retry_deadline_without_auth_or_scan(monkeypatch, tmp_path, state):
    monkeypatch.setattr(supervisor, 'STATE_DIR', tmp_path)
    monkeypatch.setattr(supervisor, 'SUPERVISOR_FILE', tmp_path / 'supervisor.json')
    monkeypatch.setattr(supervisor, '_hours', lambda: set())
    blocked = Mock(side_effect=AssertionError('No provider work before retry time'))
    monkeypatch.setattr(supervisor, '_saved_session_health', blocked)
    monkeypatch.setattr(supervisor, '_run', blocked)
    result = automated_morning._service_unavailable('Fixture outage')
    assert result['retry_at'] and result['retry_at_epoch']
    if state == 'partial':
        run_state.write_status('partial', 'Pending service failures',
                               retry_at=result['retry_at'], retry_at_epoch=result['retry_at_epoch'])
    assert supervisor.main() == 0
    blocked.assert_not_called()


def test_run_identity_and_progress_survive_status_transitions(monkeypatch):
    from scan_observability import observed_scan
    @observed_scan
    def scan():
        start = run_state.write_status('running')
        run_state.write_status('running', progress={'live_requests': 5})
        run_state.write_status('renewing_auth')
        finish = run_state.write_status('complete')
        assert finish['run_id'] == start['run_id']
        assert finish['worker_revision'] == start['worker_revision']
        assert finish['progress']['live_requests'] == 5
        assert finish['ended_at'] >= finish['started_at']
        return finish
    first = scan()
    second = scan()
    assert first['run_id'] != second['run_id']
