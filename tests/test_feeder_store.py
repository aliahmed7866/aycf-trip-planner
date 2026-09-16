"""Fare checks share a persistent allowance without changing Wizz scan data."""
from datetime import datetime, timedelta, timezone
import multiprocessing
import sqlite3
from unittest.mock import Mock

import pytest

from feeder_provider import ProviderError
from feeder_store import AUTOMATIC_LIMIT_24H, FeederStore, LIMIT_24H, LIMIT_31D


NOW = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
DAY = '2026-01-20'


def quote(**changes):
    return {
        'origin': 'MAN', 'destination': 'WAW', 'airline': 'Ryanair',
        'flight_number': 'FR123', 'departure': DAY + 'T09:00:00+00:00',
        'arrival': DAY + 'T12:20:00+01:00', 'price_gbp': '29.99',
        'observed_at': NOW.isoformat(), 'source': 'Manually checked',
        'booking_url': 'https://www.ryanair.com/', **changes,
    }


def test_manual_quotes_are_validated_isolated_and_deletable(tmp_path):
    wizz_db = tmp_path / 'aycf.sqlite3'
    wizz_db.write_bytes(b'Existing AYCF data must not change')
    store = FeederStore(tmp_path / 'feeder_quotes.sqlite3')
    saved = store.save_offer(quote(observed_at='2025-01-01T00:00:00+00:00'))
    assert saved['id']
    assert store.list_offers() == [saved]
    assert store.usage(now=NOW)['usage_31d'] == 0
    assert wizz_db.read_bytes() == b'Existing AYCF data must not change'
    assert store.delete_offer(saved['id'])
    assert not store.delete_offer(saved['id'])
    with pytest.raises(ValueError):
        store.save_offer(quote(price_gbp='not a price'))


def test_reads_and_unconfigured_refresh_do_not_call_provider(tmp_path, monkeypatch):
    fetch = Mock(side_effect=AssertionError('Unexpected network request'))
    monkeypatch.setattr('feeder_provider.fetch_serpapi_offers', fetch)
    store = FeederStore(tmp_path / 'fares.sqlite3')
    assert store.list_offers() == []
    assert store.search_status('WAW', DAY)['state'] == 'unsearched'
    assert store.usage(now=NOW)['usage_31d'] == 0
    assert store.refresh('WAW', DAY, '', now=NOW)['state'] == 'unconfigured'
    assert store.usage(now=NOW)['usage_31d'] == 0
    fetch.assert_not_called()


def test_success_is_cached_one_hour_without_refreshing_observation_time(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    observed = (NOW - timedelta(hours=5)).isoformat()
    fetch = Mock(return_value=[quote(observed_at=observed)])
    result = store.refresh('WAW', DAY, 'secret', fetcher=fetch, now=NOW)
    assert result == {'state': 'complete', 'message': 'Found 1 matching feeder fare.', 'offers': 1, 'cached': False}
    assert store.list_offers()[0]['observed_at'] == observed
    second = FeederStore(store.path)
    assert second.refresh('WAW', DAY, 'secret', fetcher=fetch, now=NOW + timedelta(minutes=59))['cached']
    assert second.usage(now=NOW)['usage_31d'] == 1
    fetch.assert_called_once_with('WAW', DAY, 'secret', now=NOW)
    second.refresh('WAW', DAY, 'secret', fetcher=fetch, now=NOW + timedelta(hours=1))
    assert fetch.call_count == 2
    assert second.list_offers()[0]['observed_at'] == observed


def test_failed_check_preserves_quotes_counts_attempt_and_has_short_cooldown(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    store.refresh('WAW', DAY, 'secret', fetcher=Mock(return_value=[quote()]), now=NOW)
    saved = store.list_offers()
    failed_at = NOW + timedelta(hours=1)
    fetch = Mock(side_effect=RuntimeError('https://example.test?api_key=secret'))
    result = store.refresh('WAW', DAY, 'secret', fetcher=fetch, now=failed_at)
    assert result['state'] == 'error'
    assert result['offers'] == 1
    assert store.list_offers() == saved
    assert store.search_status('WAW', DAY)['state'] == 'error'
    assert store.usage(now=failed_at)['usage_31d'] == 2
    assert store.refresh('WAW', DAY, 'secret', fetcher=fetch, now=failed_at + timedelta(minutes=4))['cached']
    assert fetch.call_count == 1
    store.refresh('WAW', DAY, 'secret', fetcher=fetch, now=failed_at + timedelta(minutes=5))
    assert fetch.call_count == 2
    with sqlite3.connect(store.path) as connection:
        persisted = '\n'.join(connection.iterdump())
    assert 'secret' not in persisted
    assert 'example.test' not in persisted


def test_successful_zero_only_replaces_the_requested_provider_quotes(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    manual = store.save_offer(quote(observed_at='2025-01-01T00:00:00+00:00'))
    store.refresh('WAW', DAY, 'key', fetcher=Mock(return_value=[quote()]), now=NOW)
    store.refresh('BUD', DAY, 'key', fetcher=Mock(return_value=[quote(destination='BUD')]), now=NOW)
    assert len(store.list_offers()) == 3
    result = store.refresh('WAW', DAY, 'key', fetcher=Mock(return_value=[]), now=NOW + timedelta(hours=1))
    assert result['state'] == 'complete' and result['offers'] == 0
    assert result['message'] == 'No matching fares were returned for this search.'
    assert store.search_status('WAW', DAY)['state'] == 'complete'
    assert {offer['id'] for offer in store.list_offers()} >= {manual['id']}
    assert len(store.list_offers()) == 2
    assert any(offer['destination'] == 'BUD' for offer in store.list_offers())


@pytest.mark.parametrize('code', ['invalid_key', 'quota', 'timeout', 'network', 'invalid_request'])
def test_known_provider_failure_preserves_safe_cause_quotes_and_retry_limits(tmp_path, code):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    store.refresh('WAW', DAY, 'private-key', fetcher=Mock(return_value=[quote()]), now=NOW)
    saved = store.list_offers()
    failed_at = NOW + timedelta(hours=1)
    error = ProviderError(code)
    fetch = Mock(side_effect=error)
    result = store.refresh('WAW', DAY, 'private-key', fetcher=fetch, now=failed_at)
    assert result['state'] == 'error'
    assert result['offers'] == 1
    assert result['message'].startswith(error.safe_message)
    assert 'Saved quotes are unchanged.' in result['message']
    assert 'five minutes' in result['message']
    assert store.list_offers() == saved
    assert store.search_status('WAW', DAY)['message'] == result['message']
    cached = FeederStore(store.path).refresh('WAW', DAY, 'private-key', fetcher=fetch,
                                           now=failed_at + timedelta(minutes=4))
    assert cached['cached'] and cached['message'] == result['message']
    assert store.usage(now=failed_at)['usage_24h'] == 2
    fetch.assert_called_once()
    store.refresh('WAW', DAY, 'private-key', fetcher=fetch, now=failed_at + timedelta(minutes=5))
    assert fetch.call_count == 2
    assert store.usage(now=failed_at)['usage_24h'] == 3


def test_provider_error_with_untrusted_argument_is_never_persisted(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    error = ProviderError('https://provider.test/?api_key=private-key')
    result = store.refresh('WAW', DAY, 'private-key', fetcher=Mock(side_effect=error), now=NOW)
    assert result['state'] == 'error'
    assert result['message'].startswith(ProviderError('unavailable').safe_message)
    with sqlite3.connect(store.path) as connection:
        persisted = '\n'.join(connection.iterdump())
    for text in (str(result), persisted):
        assert 'private-key' not in text
        assert 'provider.test' not in text


@pytest.mark.parametrize('replacement', [None, {}, [quote(destination='BUD')], [quote(observed_at=None)]])
def test_invalid_provider_response_preserves_existing_quotes(tmp_path, replacement):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    store.refresh('WAW', DAY, 'key', fetcher=Mock(return_value=[quote()]), now=NOW)
    saved = store.list_offers()
    result = store.refresh('WAW', DAY, 'key', fetcher=Mock(return_value=replacement), now=NOW + timedelta(hours=1))
    assert result['state'] == 'error'
    assert store.list_offers() == saved


def test_query_date_is_manchester_local_date_at_summer_midnight(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    summer = datetime(2026, 6, 1, tzinfo=timezone.utc)
    fare = quote(departure='2026-06-20T00:30:00+01:00', arrival='2026-06-20T03:30:00+02:00',
                 observed_at=summer.isoformat())
    result = store.refresh('WAW', '2026-06-20', 'key', fetcher=Mock(return_value=[fare]), now=summer)
    assert result['state'] == 'complete'
    assert store.list_offers()[0]['departure'] == '2026-06-19T23:30:00+00:00'


def test_duplicate_provider_flight_keeps_the_lowest_returned_fare(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    result = store.refresh('WAW', DAY, 'key', now=NOW,
                           fetcher=Mock(return_value=[quote(price_gbp='19.99'), quote(price_gbp='29.99')]))
    assert result['offers'] == 1
    assert result['message'] == 'Found 1 matching feeder fare.'
    assert store.list_offers()[0]['price_gbp'] == '19.99'


def test_reservation_is_visible_before_fetch_and_counts_failed_calls(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')

    def fetch(hub, travel_date, key, *, now):
        observer = FeederStore(store.path)
        assert observer.usage(now=now)['usage_24h'] == 1
        assert observer.search_status(hub, travel_date)['state'] == 'running'
        raise RuntimeError('Provider unavailable')

    assert store.refresh('WAW', DAY, 'key', fetcher=fetch, now=NOW)['state'] == 'error'
    assert store.usage(now=NOW)['usage_24h'] == 1


def _reservation_worker(path, start, results, day, automatic=False):
    store = FeederStore(path)
    if not start.wait(10):
        results.put('worker timed out')
        return
    result = store.refresh('WAW', day, 'key', fetcher=lambda *args, **kwargs: [], now=NOW, automatic=automatic)
    results.put(result['state'])


def test_separate_processes_cannot_exceed_daily_allowance(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    context = multiprocessing.get_context('fork')
    start, results = context.Event(), context.Queue()
    workers = [context.Process(target=_reservation_worker, args=(str(store.path), start, results, f'2026-02-{day:02d}'))
               for day in range(1, 13)]
    try:
        for worker in workers:
            worker.start()
        start.set()
        states = [results.get(timeout=15) for _ in workers]
        for worker in workers:
            worker.join(5)
            assert worker.exitcode == 0
        assert states.count('complete') == LIMIT_24H
        assert states.count('blocked') == 12 - LIMIT_24H
        assert store.usage(now=NOW)['usage_24h'] == LIMIT_24H
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)


def test_running_query_lease_prevents_duplicate_reservation(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    other = FeederStore(store.path)
    nested_fetch = Mock(side_effect=AssertionError('Duplicate provider call'))

    def fetch(*args, **kwargs):
        result = other.refresh('WAW', DAY, 'key', fetcher=nested_fetch, now=NOW)
        assert result['state'] == 'running'
        assert result['cached']
        assert other.usage(now=NOW)['usage_31d'] == 1
        return []

    assert store.refresh('WAW', DAY, 'key', fetcher=fetch, now=NOW)['state'] == 'complete'
    nested_fetch.assert_not_called()


def test_expired_lease_allows_retry_and_discards_old_worker_result(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')

    def late_fetch(*args, **kwargs):
        result = FeederStore(store.path).refresh('WAW', DAY, 'key', fetcher=Mock(return_value=[]),
                                               now=NOW + timedelta(minutes=3))
        assert result['state'] == 'complete'
        return [quote()]

    result = store.refresh('WAW', DAY, 'key', fetcher=late_fetch, now=NOW)
    assert result['state'] == 'running'
    assert store.list_offers() == []
    assert store.search_status('WAW', DAY)['state'] == 'complete'
    assert store.usage(now=NOW)['usage_31d'] == 2


def test_monthly_allowance_and_rolling_window_boundary(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    reserved_at = (NOW - timedelta(days=2)).timestamp()
    with sqlite3.connect(store.path) as connection:
        connection.executemany('INSERT INTO requests(query_key, reserved_at) VALUES (?, ?)',
                               [(str(index), reserved_at) for index in range(LIMIT_31D)])
    fetch = Mock(return_value=[])
    assert store.refresh('WAW', DAY, 'key', fetcher=fetch, now=NOW)['state'] == 'blocked'
    assert store.search_status('WAW', DAY)['state'] == 'blocked'
    assert store.usage(now=NOW)['usage_24h'] == 0
    fetch.assert_not_called()
    boundary = NOW + timedelta(days=29)
    assert store.usage(now=boundary)['usage_31d'] == 0
    assert store.refresh('WAW', DAY, 'key', fetcher=fetch, now=boundary)['state'] == 'complete'


def test_daily_limit_releases_at_24_hours(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    fetch = Mock(return_value=[])
    for day in range(1, 9):
        assert store.refresh('WAW', f'2026-02-{day:02d}', 'key', fetcher=fetch, now=NOW)['state'] == 'complete'
    assert store.refresh('BUD', DAY, 'key', fetcher=fetch, now=NOW + timedelta(hours=23))['state'] == 'blocked'
    assert store.refresh('BUD', DAY, 'key', fetcher=fetch, now=NOW + timedelta(hours=24))['state'] == 'complete'
    assert store.usage(now=NOW + timedelta(hours=24))['usage_31d'] == 9


@pytest.mark.parametrize('hub,day', [('MAN', DAY), ('Warsaw', DAY), ('WAW', '2026-99-01')])
def test_invalid_query_never_uses_quota(tmp_path, hub, day):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    with pytest.raises(ValueError):
        store.refresh(hub, day, 'key', fetcher=Mock(), now=NOW)
    assert store.usage(now=NOW)['usage_31d'] == 0


def test_automatic_reservations_are_atomic_and_leave_two_manual_slots(tmp_path):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    context = multiprocessing.get_context('fork')
    start, results = context.Event(), context.Queue()
    workers = [context.Process(target=_reservation_worker,
                               args=(str(store.path), start, results, f'2026-02-{day:02d}', True))
               for day in range(1, 13)]
    try:
        for worker in workers:
            worker.start()
        start.set()
        states = [results.get(timeout=15) for _ in workers]
        for worker in workers:
            worker.join(5)
            assert worker.exitcode == 0
        assert states.count('complete') == AUTOMATIC_LIMIT_24H
        assert states.count('blocked') == 12 - AUTOMATIC_LIMIT_24H
        assert store.usage(now=NOW)['automatic_24h'] == AUTOMATIC_LIMIT_24H
        for hub in ('BUD', 'MXP'):
            assert store.refresh(hub, DAY, 'key', now=NOW, fetcher=Mock(return_value=[]))['state'] == 'complete'
        assert store.usage(now=NOW)['usage_24h'] == LIMIT_24H
        assert store.refresh('FCO', DAY, 'key', now=NOW, fetcher=Mock())['state'] == 'blocked'
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)


def test_existing_request_schema_migrates_without_resetting_allowance(tmp_path):
    path = tmp_path / 'fares.sqlite3'
    with sqlite3.connect(path) as connection:
        connection.execute('CREATE TABLE requests(id INTEGER PRIMARY KEY, query_key TEXT NOT NULL, reserved_at REAL NOT NULL)')
        connection.execute('INSERT INTO requests(query_key, reserved_at) VALUES (?, ?)', ('old-manual', NOW.timestamp()))
    store = FeederStore(path)
    assert store.usage(now=NOW)['usage_24h'] == 1
    assert store.usage(now=NOW)['automatic_24h'] == 0
    assert store.refresh('WAW', DAY, 'key', now=NOW, fetcher=Mock(return_value=[]), automatic=True)['state'] == 'complete'
    assert FeederStore(path).usage(now=NOW)['usage_24h'] == 2
    assert store.usage(now=NOW)['automatic_24h'] == 1


@pytest.mark.parametrize('positive,hours', [(True, 6), (False, 24)])
def test_store_enforces_longer_automatic_cache_even_without_controller(tmp_path, positive, hours):
    store = FeederStore(tmp_path / 'fares.sqlite3')
    fetch = Mock(return_value=[quote()] if positive else [])
    store.refresh('WAW', DAY, 'key', now=NOW, fetcher=fetch)
    assert store.refresh('WAW', DAY, 'key', now=NOW + timedelta(hours=1), fetcher=fetch, automatic=True)['cached']
    assert store.usage(now=NOW)['automatic_24h'] == 0
    assert not store.refresh('WAW', DAY, 'key', now=NOW + timedelta(hours=hours), fetcher=fetch, automatic=True)['cached']
    assert fetch.call_count == 2
