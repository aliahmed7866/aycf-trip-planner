"""Automatic fare checks spend quota only on eligible current connections."""
from datetime import datetime, timedelta, timezone
import multiprocessing
import sqlite3
from unittest.mock import Mock

import pytest

import feeder_automation as automation
from feeder_provider import ProviderError
from feeder_store import FeederStore

NOW = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
DAY = '2026-01-20'
REPORT = {'opportunities': [{'hub': 'WAW'}], 'incomplete': False}


def target(hub='WAW', day=DAY):
    return {'hub': hub, 'date': day, 'reason': 'Repeated onward availability to Georgia.'}


def offer(hub='WAW', day=DAY):
    return {'origin': 'MAN', 'destination': hub, 'airline': 'Ryanair',
            'flight_number': 'FR123', 'departure': day + 'T09:00:00+00:00',
            'arrival': day + 'T12:20:00+01:00', 'price_gbp': '29.99',
            'observed_at': NOW.isoformat(), 'source': 'Manually checked',
            'booking_url': 'https://www.ryanair.com/'}


@pytest.fixture
def store(tmp_path):
    return FeederStore(tmp_path / 'fares.sqlite3')


@pytest.fixture
def ranking(monkeypatch):
    rank = Mock(return_value=[target(), target('BUD')])
    monkeypatch.setattr(automation, 'rank_feeder_targets', rank)
    return rank


def test_batch_is_small_prefers_distinct_hubs_and_waits_six_hours(store, ranking):
    ranking.return_value = [target(), target(day='2026-01-21'), target('BUD'), target('MXP')]
    fetch = Mock(return_value=[])
    summary = automation.run_auto_checks(store, REPORT, 'key', now=NOW, fetcher=fetch)
    assert summary['checked_count'] == 2
    assert [(item['hub'], item['date']) for item in summary['selected']] == [('WAW', DAY), ('BUD', DAY)]
    assert summary['automatic_24h'] == summary['usage_24h'] == 2
    assert summary['next_due'] == (NOW + timedelta(hours=6)).isoformat()
    assert summary['last_checked'] == NOW.isoformat()
    automation.run_auto_checks(store, REPORT, 'key', now=NOW + timedelta(hours=5, minutes=59), fetcher=fetch)
    assert fetch.call_count == 2
    automation.run_auto_checks(store, REPORT, 'key', now=NOW + timedelta(hours=6), fetcher=fetch)
    assert fetch.call_count == 4
    assert [call.args[:2] for call in fetch.call_args_list[2:]] == [('WAW', '2026-01-21'), ('MXP', DAY)]


def test_one_hub_can_use_two_distinct_dates_but_not_duplicate_targets(store, ranking):
    ranking.return_value = [target(), target(), target(day='2026-01-21')]
    fetch = Mock(return_value=[])
    summary = automation.run_auto_checks(store, REPORT, 'key', now=NOW, fetcher=fetch)
    assert summary['checked_count'] == 2
    assert fetch.call_count == 2
    assert {item['date'] for item in summary['selected']} == {DAY, '2026-01-21'}


def test_positive_top_hubs_leave_a_slot_to_discover_unchecked_connections(store, ranking):
    ranking.return_value = [target(), target('BUD'), target('MXP'), target('FCO')]
    fetch = Mock(side_effect=lambda hub, day, key, **kwargs: [offer(hub, day)])
    batches = [automation.run_auto_checks(store, REPORT, 'key', now=NOW + timedelta(hours=hours), fetcher=fetch)
               for hours in (0, 6, 12)]
    assert [[item['hub'] for item in batch['selected']] for batch in batches] == [
        ['WAW', 'BUD'], ['WAW', 'MXP'], ['WAW', 'FCO'],
    ]
    assert batches[-1]['automatic_24h'] == 6


@pytest.mark.parametrize('result,cooldown', [('positive', 6), ('empty', 24), ('error', 6)])
def test_manual_observations_control_automatic_recheck_cooldown(store, ranking, result, cooldown):
    ranking.return_value = [target()]
    initial = Mock(return_value=[offer()] if result == 'positive' else [])
    if result == 'error':
        initial.side_effect = RuntimeError('Provider unavailable')
    store.refresh('WAW', DAY, 'key', now=NOW, fetcher=initial)
    fetch = Mock(return_value=[])
    waiting = automation.run_auto_checks(store, REPORT, 'key', now=NOW + timedelta(hours=1), fetcher=fetch)
    assert waiting['state'] == 'waiting'
    assert waiting['last_batch_at'] is None
    assert waiting['next_due'] == (NOW + timedelta(hours=cooldown)).isoformat()
    fetch.assert_not_called()
    due = automation.run_auto_checks(store, REPORT, 'key', now=NOW + timedelta(hours=cooldown), fetcher=fetch)
    assert due['checked_count'] == 1
    fetch.assert_called_once()


def test_provider_failure_stops_batch_and_never_persists_secret(store, ranking):
    fetch = Mock(side_effect=RuntimeError('https://provider.test?api_key=private-key'))
    summary = automation.run_auto_checks(store, REPORT, 'private-key', now=NOW, fetcher=fetch)
    assert summary['state'] == 'error'
    assert summary['checked_count'] == 1
    assert summary['automatic_24h'] == 1
    assert len(summary['selected']) == 1
    fetch.assert_called_once()
    with sqlite3.connect(store.path) as connection:
        persisted = '\n'.join(connection.iterdump())
    assert 'private-key' not in persisted
    assert 'provider.test' not in persisted
    automation.run_auto_checks(store, REPORT, 'private-key', now=NOW + timedelta(minutes=10), fetcher=fetch)
    assert fetch.call_count == 1


@pytest.mark.parametrize('code', ['invalid_key', 'quota', 'timeout'])
def test_automatic_failure_retains_actionable_cause_and_six_hour_cooldown(store, ranking, code):
    error = ProviderError(code)
    fetch = Mock(side_effect=error)
    summary = automation.run_auto_checks(store, REPORT, 'private-key', now=NOW, fetcher=fetch)
    assert summary['state'] == 'error'
    assert summary['message'].startswith(error.safe_message)
    assert 'Saved quotes are unchanged.' in summary['message']
    assert 'next scheduled batch' in summary['message']
    assert 'five minutes' not in summary['message']
    assert summary['message'] == store.search_status('WAW', DAY)['message']
    assert summary['checked_count'] == summary['automatic_24h'] == 1
    assert summary['next_due'] == (NOW + timedelta(hours=6)).isoformat()
    restored = automation.automation_status(FeederStore(store.path), now=NOW)
    assert restored['message'] == summary['message']
    waiting = automation.run_auto_checks(store, REPORT, 'private-key', now=NOW + timedelta(minutes=5), fetcher=fetch)
    assert waiting['message'] == summary['message']
    fetch.assert_called_once()
    with sqlite3.connect(store.path) as connection:
        assert 'private-key' not in '\n'.join(connection.iterdump())


def test_paused_no_key_and_empty_reports_do_not_burn_batch_or_requests(store, ranking):
    fetch = Mock(side_effect=AssertionError('No request should be made'))
    assert automation.run_auto_checks(store, REPORT, '', now=NOW, fetcher=fetch)['state'] == 'unconfigured'
    automation.set_automation_enabled(store, False)
    assert automation.run_auto_checks(store, REPORT, 'key', now=NOW, fetcher=fetch)['state'] == 'paused'
    assert not automation.automation_status(FeederStore(store.path), now=NOW)['enabled']
    automation.set_automation_enabled(store, True)
    empty = automation.run_auto_checks(store, {'opportunities': [], 'incomplete': True}, 'key', now=NOW, fetcher=fetch)
    assert empty['state'] == 'waiting'
    assert empty['last_batch_at'] is None
    assert empty['usage_31d'] == empty['automatic_24h'] == 0
    ranking.assert_not_called()
    fetch.assert_not_called()


def test_partial_scan_positive_opportunities_can_be_checked(store, ranking):
    summary = automation.run_auto_checks(store, {**REPORT, 'incomplete': True, 'scan_partial': True},
                                         'key', now=NOW, fetcher=Mock(return_value=[]))
    assert summary['checked_count'] == 2


def test_pause_during_batch_stops_next_request_and_resume_retains_cooldown(store, ranking):
    def fetch(*args, **kwargs):
        automation.set_automation_enabled(FeederStore(store.path), False)
        return []
    provider = Mock(side_effect=fetch)
    paused = automation.run_auto_checks(store, REPORT, 'key', now=NOW, fetcher=provider)
    assert paused['state'] == 'paused'
    assert paused['checked_count'] == 1
    assert paused['next_due'] is None
    automation.set_automation_enabled(store, True)
    resumed = automation.run_auto_checks(store, REPORT, 'key', now=NOW + timedelta(minutes=1), fetcher=provider)
    assert resumed['next_due'] == (NOW + timedelta(hours=6)).isoformat()
    assert resumed['automatic_24h'] == 1
    provider.assert_called_once()


def test_active_manual_query_is_skipped_but_expired_lease_can_recover(store, ranking):
    ranking.return_value = [target()]
    fetch = Mock(return_value=[])
    def during_manual(*args, **kwargs):
        summary = automation.run_auto_checks(store, REPORT, 'key', now=NOW, fetcher=fetch)
        assert summary['state'] == 'waiting'
        assert summary['checked_count'] == 0
        fetch.assert_not_called()
        return []
    store.refresh('WAW', DAY, 'key', now=NOW, fetcher=during_manual)
    with sqlite3.connect(store.path) as connection:
        connection.execute("UPDATE searches SET state='running', lease_until=?", (NOW.timestamp() - 1,))
    assert automation.run_auto_checks(store, REPORT, 'key', now=NOW, fetcher=fetch)['checked_count'] == 1


def test_second_controller_cannot_overlap_running_batch(store, ranking):
    nested_fetch = Mock(side_effect=AssertionError('Overlapping request'))
    def fetch(*args, **kwargs):
        summary = automation.run_auto_checks(FeederStore(store.path), REPORT, 'key', now=NOW, fetcher=nested_fetch)
        assert summary['state'] == 'running'
        # Both reservation and batch timestamp are durable before provider work.
        assert summary['last_batch_at'] == NOW.isoformat()
        return []
    result = automation.run_auto_checks(store, REPORT, 'key', now=NOW, fetcher=fetch)
    assert result['checked_count'] == 2
    assert result['usage_24h'] == 2
    nested_fetch.assert_not_called()


def _batch_worker(path, start, results):
    store = FeederStore(path)
    if not start.wait(10):
        results.put('timeout')
        return
    result = automation.run_auto_checks(store, REPORT, 'key', now=NOW, fetcher=lambda *args, **kwargs: [])
    results.put(result['state'])


def test_separate_process_triggers_share_one_batch(store, ranking):
    context = multiprocessing.get_context('fork')
    start, results = context.Event(), context.Queue()
    workers = [context.Process(target=_batch_worker, args=(str(store.path), start, results)) for _ in range(6)]
    try:
        for worker in workers:
            worker.start()
        start.set()
        states = [results.get(timeout=15) for _ in workers]
        for worker in workers:
            worker.join(5)
            assert worker.exitcode == 0
        assert set(states) <= {'running', 'complete'}
        assert store.usage(now=NOW)['automatic_24h'] == 2
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)


def test_automatic_allowance_stops_batch_but_manual_slots_remain(store, ranking):
    with sqlite3.connect(store.path) as connection:
        connection.executemany('INSERT INTO requests(query_key, reserved_at, automatic) VALUES (?, ?, 1)',
                               [(str(index), NOW.timestamp()) for index in range(5)])
    fetch = Mock(return_value=[])
    result = automation.run_auto_checks(store, REPORT, 'key', now=NOW, fetcher=fetch)
    assert result['state'] == 'blocked'
    assert result['checked_count'] == 1
    assert result['automatic_24h'] == 6
    assert result['next_due'] == (NOW + timedelta(hours=24)).isoformat()
    fetch.assert_called_once()
    for day in ('2026-01-21', '2026-01-22'):
        assert store.refresh('WAW', day, 'key', now=NOW, fetcher=fetch)['state'] == 'complete'
    assert store.usage(now=NOW)['usage_24h'] == 8


def test_exhausted_budget_does_not_start_batch_clock(store, ranking):
    with sqlite3.connect(store.path) as connection:
        connection.executemany('INSERT INTO requests(query_key, reserved_at, automatic) VALUES (?, ?, 1)',
                               [(str(index), NOW.timestamp()) for index in range(6)])
    fetch = Mock()
    result = automation.run_auto_checks(store, REPORT, 'key', now=NOW, fetcher=fetch)
    assert result['state'] == 'blocked'
    assert result['last_batch_at'] is None
    assert result['next_due'] == (NOW + timedelta(hours=24)).isoformat()
    ranking.assert_not_called()
    fetch.assert_not_called()


def test_status_only_reads_and_ranking_failure_does_not_spend_requests(store, ranking, monkeypatch):
    fetch = Mock(side_effect=AssertionError('Unexpected provider request'))
    monkeypatch.setattr('feeder_provider.fetch_serpapi_offers', fetch)
    assert automation.automation_status(store, now=NOW)['automatic_24h'] == 0
    ranking.assert_not_called()
    ranking.side_effect = RuntimeError('Private diagnostic details')
    summary = automation.run_auto_checks(store, REPORT, 'key', now=NOW)
    assert summary['state'] == 'error'
    assert summary['last_batch_at'] is None
    assert summary['usage_31d'] == 0
    assert 'Private' not in summary['message']
    fetch.assert_not_called()
