from datetime import date, datetime, timedelta, timezone
import json
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
import requests

from cache_db import ScanCacheDB
from daily_verification import DailyVerification, verified_today
from scanner import Flight, WizzAvailabilityUnknown, WizzIntegrationChanged
from parallel_fetch import fetch_group
from morning_scan import CapturedRequestWizzClient
from scan_service_errors import is_service_error, ServiceFailureTracker
from termux import fresh_scan, automated_morning, run_state, health_ui
from tests.test_fresh_scan import fresh
from tests.test_rate_limit_scan_preservation import scan_fixture
import tiered_morning


@pytest.fixture
def cache(tmp_path):
    return DailyVerification(ScanCacheDB(str(tmp_path / 'daily.sqlite3')))


def flight(day):
    start = datetime.combine(day, datetime.min.time()) + timedelta(hours=12)
    return Flight('LTN', 'BUD', 'W1', start, start+timedelta(hours=2), '', '')


@pytest.mark.parametrize('empty', [False, True])
def test_same_day_survives_process_restart_and_preserves_timestamp(cache, empty):
    day = date.today() + timedelta(days=1)
    client = Mock()
    client.check.return_value = [] if empty else [flight(day)]
    first = cache.check(client, 'London Luton', 'Budapest', day)
    observed = cache.get('LTN','BUD',day)[0]
    second = DailyVerification(cache.db).check(client, 'LTN', 'BUD', day)
    assert first == second
    client.check.assert_called_once()
    assert cache.get('LTN','BUD',day)[0] == observed


def test_overlapping_groups_share_one_request(cache):
    client = Mock()
    client.check.return_value = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(lambda _: cache.check(client,'LTN','BUD',date.today()), range(6)))
    assert results == [[]] * 6
    client.check.assert_called_once()


def test_unknown_is_never_cached_as_empty(cache):
    client = Mock()
    client.check.side_effect = [WizzAvailabilityUnknown('unknown'), []]
    with pytest.raises(WizzAvailabilityUnknown):
        cache.check(client,'LTN','BUD',date.today())
    assert cache.get('LTN','BUD',date.today()) is None
    assert cache.check(client,'LTN','BUD',date.today()) == []
    assert client.check.call_count == 2


def test_partial_city_retries_only_unverified_airport_including_empty(cache):
    day = date.today()
    client = Mock()
    client.check.side_effect = [[], WizzAvailabilityUnknown('unknown')]
    first = fetch_group(client,[('LTN','BUD'),('LGW','BUD')],day,daily_cache=cache)
    assert first[1] == [('LTN','BUD')] and len(first[2]) == 1
    client.check.side_effect = None
    client.check.return_value = []
    client.check.reset_mock()
    second = fetch_group(client,[('LTN','BUD'),('LGW','BUD')],day,daily_cache=DailyVerification(cache.db))
    assert len(second[1]) == 2 and not second[2]
    client.check.assert_called_once_with('LGW','BUD',day)


@pytest.mark.parametrize('offset', [-1, 1])
def test_yesterday_and_future_observations_do_not_suppress_requests(cache, offset):
    day = date.today()
    stamp = (datetime.now(timezone.utc)+timedelta(days=offset)).isoformat()
    cache.save('LTN','BUD',day,[],stamp)
    client = Mock()
    client.check.return_value = []
    cache.check(client,'LTN','BUD',day)
    client.check.assert_called_once()
    assert verified_today(cache.get('LTN','BUD',day)[0])


def test_new_pdf_reuses_only_current_plan_airports_and_dates(cache):
    db = cache.db
    day = date.today()
    db.upsert_pdf_run('old','old',day.isoformat(),day.isoformat(),1)
    db.replace_route_check('old','London','Budapest',day,[flight(day)],checked_pairs=[('LTN','BUD'),('LGW','BUD')])
    retained = DailyVerification(db)
    assert retained.group([('LGW','BUD')],day)[1] == []
    assert len(retained.group([('LTN','BUD')],day)[1]) == 1
    assert retained.group([('LPL','BUD')],day) is None
    assert retained.group([('LTN','BUD')],day+timedelta(days=1)) is None
    assert retained.group([('BUD','LTN')],day) is None


def test_preflight_result_is_not_requested_again_by_worker(cache, monkeypatch):
    client = CapturedRequestWizzClient({'cookies': []})
    client.daily_verification = cache
    client.dynamic_url = 'https://multipass.wizzair.com/fixture'
    client.captured_request_template = {'origin':'LTN','destination':'BUD','departure':date.today().isoformat()}
    monkeypatch.setattr(client,'resolve_station',lambda value:value)
    send = Mock(return_value=None)
    monkeypatch.setattr(client,'_send_and_decode',send)
    assert client.preflight('LTN','BUD',date.today())['ok']
    assert cache.check(client,'LTN','BUD',date.today()) == []
    assert client.preflight('LTN','BUD',date.today())['ok']
    send.assert_called_once()


def test_safe_clear_keeps_positive_empty_and_partial_observations(fresh, monkeypatch):
    db,day,_,_ = fresh
    db.replace_route_check('current','BUD','LTN',day,[],checked_pairs=[('BUD','LTN')])
    cache = DailyVerification(db)
    cache.save('LTN','BUD',day,[flight(day)])
    before = cache.get('LTN','BUD',day)
    def scan(**kwargs):
        assert db.route_checked('current','Manchester','Budapest',day)
        assert db.route_checked('current','BUD','LTN',day)
        assert DailyVerification(db).get('LTN','BUD',day) == before
        return {'ok':True}
    monkeypatch.setattr(automated_morning,'_run_with_lock',scan)
    result = fresh_scan.run(preserve_completed=True)
    assert result['pending_reset']['checks_reset'] == 0
    assert result['pending_reset']['catalogues_reset'] == 0
    assert result['pending_reset']['failed_records_cleared'] == 1


def test_safe_clear_during_cooldown_retains_checks_and_deadline(fresh, monkeypatch):
    import wizz_rate_limit as limits
    db,day,_,_ = fresh
    deadline = limits.record_rate_limit(7200)['cooldown_until']
    monkeypatch.setattr(automated_morning.tiered_morning,'_run_locked',Mock(side_effect=AssertionError('network')))
    result = fresh_scan.run(preserve_completed=True)
    assert result['queued'] and result['cooldown_until'] == deadline
    assert db.route_checked('current','Manchester','Budapest',day)


def test_explicit_full_reset_invalidates_ledger_but_keeps_flights(fresh):
    db,day,_,_ = fresh
    cache = DailyVerification(db)
    cache.save('LTN','BUD',day,[flight(day)])
    fresh_scan._reset_pending(db)
    assert cache.get('LTN','BUD',day) is None
    assert not db.route_checked('current','Manchester','Budapest',day)
    assert db.get_flights('Manchester','Budapest',day,'current')


def test_full_rescan_requires_both_csrf_and_explicit_confirmation(monkeypatch):
    from tests.test_rate_limit_status_ui import health_app
    spawn = Mock()
    monkeypatch.setattr(health_ui,'_spawn',spawn)
    client = health_app().test_client()
    with client.session_transaction() as session:
        session['csrf_token'] = 'token'
    client.post('/system/full-rescan',data={'confirm_full_rescan':'yes'})
    client.post('/system/full-rescan',data={'csrf_token':'token'})
    spawn.assert_not_called()
    client.post('/system/full-rescan',data={'csrf_token':'token','confirm_full_rescan':'yes'})
    assert spawn.call_args.args[1][-1] == 'fresh'


def backend_error(**overrides):
    response = requests.Response()
    response.status_code = 400
    response._content = json.dumps(dict(code='PASS-0000',key='wallet.error.generic',message='cyf.flights.FlightSearchException: backend failed',**overrides)).encode()
    return requests.HTTPError('bad request', response=response)


def test_specific_backend_400_is_pending_and_widespread_errors_pause(cache):
    exc = backend_error()
    client = Mock()
    client.check.side_effect = exc
    tracker = ServiceFailureTracker()
    first = fetch_group(client,[('LTN','BUD')],date.today(),service_failures=tracker,daily_cache=cache)
    assert first[1] == [] and len(first[2]) == 1
    assert cache.get('LTN','BUD',date.today()) is None
    fetch_group(client,[('LGW','BUD')],date.today(),service_failures=tracker)
    with pytest.raises(requests.HTTPError,match='Pausing scan'):
        fetch_group(client,[('LPL','BUD')],date.today(),service_failures=tracker)


def test_other_400_still_requires_request_repair(monkeypatch):
    response = requests.Response()
    response.status_code = 400
    response._content = b'{"code":"BAD_REQUEST","message":"missing origin"}'
    exc = requests.HTTPError('bad request',response=response)
    assert not is_service_error(exc)
    client = CapturedRequestWizzClient({'cookies': []})
    monkeypatch.setattr(client,'_request',Mock(side_effect=exc))
    with pytest.raises(WizzIntegrationChanged):
        client._send_and_decode({},'test')


def test_backend_400_does_not_trigger_auth_repair(monkeypatch):
    monkeypatch.setattr(automated_morning.tiered_morning,'run',Mock(side_effect=backend_error()))
    renew = Mock(side_effect=AssertionError('Not an auth failure'))
    monkeypatch.setattr(automated_morning,'_refresh',renew)
    result = automated_morning._run_once(False)
    assert result['state'] == 'wizz_service_unavailable'
    renew.assert_not_called()
