"""Real scan persistence survives a throttled HTTP request and later resumes."""
from datetime import date, datetime, time as clock_time, timedelta, timezone
from concurrent.futures import wait
from types import SimpleNamespace

import pandas as pd
import pytest

from cache_db import ScanCacheDB
from parallel_fetch import GlobalStartLimiter, ParallelFetcher
import parallel_fetch
from scanner import Flight, WizzAYCFClient
from scan_scope import default_scope, scan_jobs, scan_plan, scan_run_id, scope_fingerprint
import tiered_morning
import morning_scan
import wizz_rate_limit


def _flight(origin, destination, day):
    number = 'W6' + str(sum(map(ord, origin)))
    return Flight(origin, destination, number, datetime.combine(day, clock_time(10)),
                  datetime.combine(day, clock_time(13)), '10:00', '13:00')


@pytest.fixture
def scan_fixture(tmp_path, monkeypatch):
    # Keep time advancement local to the durable limiter. No test sleeps for
    # real cooldowns, and every HTTP request below terminates in this fixture.
    clock = [datetime.now(timezone.utc).timestamp()]
    monkeypatch.setattr(wizz_rate_limit, 'time', SimpleNamespace(time=lambda: clock[0]))
    monkeypatch.setattr(GlobalStartLimiter, 'wait', lambda self: wizz_rate_limit.check_cooldown())
    monkeypatch.setenv('AYCF_STATE_DIR', str(tmp_path / 'state'))
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path / 'config'))
    monkeypatch.setenv('AYCF_SCAN_WORKERS', '1')
    monkeypatch.setenv('AYCF_MANUAL_REFRESH_TTL_SECONDS', '21600')

    def prepare(origin='Liverpool', worker=tiered_morning):
        day = date.today() + timedelta(days=1)
        generated = datetime.combine(day, clock_time())
        origins = ['London Gatwick', 'London Luton'] if origin == 'London' else [origin]
        scope = dict(default_scope(), origins=origins, connection_hubs=[],
                     preferred_destinations=[], watch_routes=[])
        frame = pd.DataFrame([(origin, 'Budapest')], columns=['departure_from', 'departure_to'])
        plan = scan_plan([(origin, 'Budapest')], scope, days=1)
        jobs = scan_jobs(plan, scope, [day])
        route_pairs = plan['primary_routes'] + plan['hub_routes']
        run_id = scan_run_id(generated, scope, route_pairs)
        db = ScanCacheDB(str(tmp_path / (origin + '.sqlite3')))
        db.upsert_pdf_run(run_id, generated.isoformat(), generated.isoformat(), generated.isoformat(),
                          len(route_pairs), scope_id=scope_fingerprint(scope), scope=scope)
        pending = next(job for job in jobs if (job[1], job[2]) == (origin, 'Budapest'))
        for job in jobs:
            if job == pending:
                continue
            a, b = job[6][0]
            db.replace_route_check(run_id, job[1], job[2], day, [_flight(a, b, day)],
                                   checked_pairs=job[6])

        state = SimpleNamespace(fail_pair=pending[6][-1], failing=True, requests=[],
                                clock=clock, db=db, run_id=run_id, day=day, pending=pending,
                                completed=len(jobs) - 1, jobs=jobs)

        class FixtureHTTP:
            def __init__(self):
                self.cookies = {}

            def request(self, method, url, **kwargs):
                assert method == 'POST' and url == 'https://multipass.wizzair.com/fixture-only'
                pair = tuple(kwargs['json']['route'])
                state.requests.append(pair)
                limited = state.failing and pair == state.fail_pair
                return SimpleNamespace(status_code=429 if limited else 200,
                                       headers={'Retry-After': '1800'} if limited else {},
                                       raise_for_status=lambda: None)

        class FixtureClient(WizzAYCFClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.no_availability_responses = self.wallet_redirects = self.html_retries = 0
                self.http = FixtureHTTP()
                self.dynamic_url = 'https://multipass.wizzair.com/fixture-only'
                self.captured_request_method = 'POST'
                self.captured_template_type = 'json'
                self.captured_request_template = {}

            def preflight(self, *args):
                wizz_rate_limit.check_cooldown()
                return {'ok': True, 'availability_verified': True, 'response': 'fixture'}

            def _throttle(self):
                wizz_rate_limit.check_cooldown()

            def check(self, a, b, travel_day):
                self._request('POST', self.dynamic_url, json={'route': [a, b]})
                return [_flight(a, b, travel_day)]

        monkeypatch.setattr(worker, 'refresh_direct_snapshot',
                            lambda *args: ('pdf', frame, generated, generated, generated))
        monkeypatch.setattr(worker, '_cache_dir', lambda: str(tmp_path / 'cache'))
        monkeypatch.setattr(worker, '_mirror_for_web', lambda *args: None)
        monkeypatch.setattr(worker, 'load_scope', lambda: scope)
        monkeypatch.setattr(worker, 'scan_scope_with_preferences', lambda value: value)
        monkeypatch.setattr(worker, 'SessionVault',
                            lambda: SimpleNamespace(load=lambda: {'cookies': []}))
        monkeypatch.setattr(worker, 'CapturedRequestWizzClient', FixtureClient)
        monkeypatch.setattr(worker, '_apply_wizz_runtime', lambda client: True)
        monkeypatch.setattr(worker, 'prepare_required_stations',
                            lambda *args: {'resolved': 3, 'required': 3, 'unresolved': [], 'aliases': 3})
        state.client_factory = lambda: FixtureClient({'cookies': []})
        return state

    return prepare


def _completed_rows(db, run_id):
    with db.connect() as connection:
        return [tuple(row) for row in connection.execute(
            'SELECT origin, destination, travel_date, fetched_at, flight_count FROM route_checks '
            'WHERE pdf_run_id=? AND complete=1 ORDER BY origin, destination, travel_date', (run_id,))]


@pytest.mark.parametrize('worker,force', [(tiered_morning, False), (tiered_morning, True), (morning_scan, False)])
def test_429_preserves_completed_sqlite_checks_and_resume_reuses_them(scan_fixture, monkeypatch, worker, force):
    state = scan_fixture(worker=worker)
    before = _completed_rows(state.db, state.run_id)
    known = state.db.get_flights('Budapest', 'Liverpool', state.day, state.run_id)
    with pytest.raises(wizz_rate_limit.WizzRateLimited) as stopped:
        worker._run_locked(state.db, force=force)
    assert state.requests == [state.fail_pair]  # No retry after the first 429.
    assert stopped.value.status['cooldown_until'] == state.clock[0] + 1800
    assert _completed_rows(state.db, state.run_id) == before
    assert state.db.get_flights('Budapest', 'Liverpool', state.day, state.run_id) == known
    assert not state.db.route_checked(state.run_id, 'Liverpool', 'Budapest', state.day)
    assert not state.db.get_pdf_run(state.run_id)['scanned_at']
    with state.db.connect() as connection:
        assert connection.execute('SELECT status FROM scan_runs ORDER BY id DESC LIMIT 1').fetchone()[0] != 'completed'

    # A new DB/client and even an explicit full refresh cannot bypass cooldown.
    reopened = ScanCacheDB(str(state.db.path))
    monkeypatch.setenv('AYCF_MANUAL_REFRESH_TTL_SECONDS', '0')
    state.clock[0] = stopped.value.status['cooldown_until'] - 1
    monkeypatch.setattr(worker, 'ScanCacheDB', lambda: reopened)
    with pytest.raises(wizz_rate_limit.WizzRateLimited):
        worker.run(force=True)
    assert state.requests == [state.fail_pair]
    assert _completed_rows(reopened, state.run_id) == before

    # Once due, regular resume and smart manual refresh both retain eligible
    # completed checks instead of rebuilding the full scan.
    monkeypatch.setenv('AYCF_MANUAL_REFRESH_TTL_SECONDS', '21600')
    state.clock[0] += 1
    state.failing = False
    result = worker._run_locked(reopened, force=force)
    assert result['ok'] and result['resumed_checks'] == state.completed
    assert result['route_day_checks'] == 1
    assert state.requests == [state.fail_pair, state.fail_pair]
    assert all(row in _completed_rows(reopened, state.run_id) for row in before)
    assert reopened.get_pdf_run(state.run_id)['scanned_at']
    assert reopened.get_flights('Budapest', 'Liverpool', state.day, state.run_id) == known


@pytest.mark.parametrize('worker', [tiered_morning, morning_scan])
def test_429_partway_through_city_group_preserves_verified_airport_flights(scan_fixture, worker):
    state = scan_fixture('London', worker=worker)
    pairs = state.pending[6]
    assert len(pairs) >= 2
    before = _completed_rows(state.db, state.run_id)
    with pytest.raises(wizz_rate_limit.WizzRateLimited) as stopped:
        worker._run_locked(state.db)
    assert state.requests == pairs
    saved = state.db.get_flights('London', 'Budapest', state.day, state.run_id)
    assert saved and {flight.origin for flight in saved} == {pair[0] for pair in pairs[:-1]}
    assert not state.db.route_checked(state.run_id, 'London', 'Budapest', state.day)
    assert not state.db.get_pdf_run(state.run_id)['scanned_at']
    assert _completed_rows(state.db, state.run_id) == before

    state.clock[0] = stopped.value.status['cooldown_until']
    state.failing = False
    resumed = worker._run_locked(ScanCacheDB(str(state.db.path)))
    assert resumed['ok'] and resumed['resumed_checks'] == state.completed
    assert state.requests == pairs + pairs
    saved = state.db.get_flights('London', 'Budapest', state.day, state.run_id)
    assert len(saved) == len(pairs)  # Earlier observed flights are not duplicated.
    assert state.db.route_checked(state.run_id, 'London', 'Budapest', state.day)


def test_completed_future_is_drained_when_429_is_reported_first(scan_fixture, monkeypatch):
    state = scan_fixture()
    assert state.jobs[-1] == state.pending

    def failure_first(futures):
        # A coordinator may observe the failed future before another already
        # completed result. Force that legal completion order deterministically.
        ordered = list(futures)
        _, outstanding = wait(ordered, timeout=5)
        assert not outstanding
        return iter(reversed(ordered))

    monkeypatch.setattr(parallel_fetch, 'as_completed', failure_first)
    captured = []
    fetcher = ParallelFetcher(state.client_factory, workers=1)
    with pytest.raises(wizz_rate_limit.WizzRateLimited):
        fetcher.run(state.jobs, captured.append)
    assert state.requests == [('Budapest', 'Liverpool'), ('Liverpool', 'Budapest')]
    assert len(captured) == 1
    assert captured[0]['origin'] == 'Budapest' and captured[0]['destination'] == 'Liverpool'
    assert captured[0]['unknown'] == [] and len(captured[0]['flights']) == 1
