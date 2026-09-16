"""Regressions from the September planner/Hub review; no live Wizz requests."""
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cache_db import ScanCacheDB
from itinerary_search import cached_scan_itineraries
from termux import admin_hub as hub, automated_morning, health_ui, run_state


@pytest.fixture
def journey(tmp_path):
    db = ScanCacheDB(str(tmp_path / 'flights.sqlite'))
    db.upsert_pdf_run('current', '2026-09-16', '2026-09-16', '2026-09-19', 2)
    from scanner import Flight
    first = Flight('Liverpool', 'Warsaw', 'W1', datetime(2026, 9, 16, 6), datetime(2026, 9, 16, 9), '', '')
    onward = Flight('Warsaw', 'Kutaisi', 'W2', datetime(2026, 9, 18, 9), datetime(2026, 9, 18, 13), '', '')
    db.replace_route_check('current', 'Liverpool', 'Warsaw', date(2026, 9, 16), [first])
    db.replace_route_check('current', 'Warsaw', 'Kutaisi', date(2026, 9, 18), [onward], complete=False)
    # The known onward route can come from additional scan coverage, outside today's PDF edges.
    graph = SimpleNamespace(edges_for_day=lambda day: {('Liverpool', 'Warsaw')})
    return db, graph


def search(db, graph, **kw):
    return cached_scan_itineraries(graph, db, 'Liverpool', 'Kutaisi', date(2026, 9, 16),
                                  days=1, pdf_run_id='current', **kw)


def test_partial_onward_flight_is_searchable_at_exact_48_hour_boundary(journey):
    db, graph = journey
    rows, missing = search(db, graph)
    assert len(rows) == 1
    assert rows[0]['connection_minutes_list'] == [48 * 60]
    assert rows[0]['legs'][1]['flight_code'] == 'W2'
    assert missing == 2  # eligible 16th and 17th are unknown, not proven empty
    assert not db.route_checked('current', 'Warsaw', 'Kutaisi', date(2026, 9, 18))
    assert not db.get_pdf_run('current')['scanned_at']
    assert ('Warsaw', 'Kutaisi') not in db.checked_routes('current')


def test_missing_connection_dates_count_even_with_known_flights(journey):
    db, graph = journey
    # A completed empty check removes exactly one gap; no requests are made.
    db.replace_route_check('current', 'Warsaw', 'Kutaisi', date(2026, 9, 17), [])
    rows, missing = search(db, graph)
    assert len(rows) == 1 and missing == 1
    assert search(db, graph, max_transfer_minutes=48 * 60 - 1)[0] == []


def test_partial_routes_do_not_leak_from_other_runs(journey):
    db, graph = journey
    with db.connect() as conn:
        conn.execute("UPDATE route_checks SET pdf_run_id='other' WHERE origin='Warsaw'")
        conn.execute("UPDATE route_flights SET pdf_run_id='other' WHERE origin='Warsaw'")
    assert search(db, graph)[0] == []


@pytest.fixture
def managed(monkeypatch, tmp_path):
    monkeypatch.setattr(hub, 'STATE_DIR', tmp_path)
    monkeypatch.setattr(hub, '_service_available', lambda app: True)
    return {'id': 'mediahub', 'name': 'Media Hub', 'service': 'mediahub',
            'working_dir': str(tmp_path), 'process_match': 'obsolete',
            'health_url': 'http://127.0.0.1:8083/health', 'update_status': {}}


@pytest.mark.parametrize('failure', [subprocess.TimeoutExpired('sv', 4), OSError('supervisor lost')])
def test_service_probe_failure_keeps_hub_pages_available(managed, monkeypatch, failure):
    monkeypatch.setattr(hub.subprocess, 'run', Mock(side_effect=failure))
    monkeypatch.setattr(hub, '_load_registry', lambda: [managed])
    client = hub.create_app().test_client()
    for path in ('/', '/manage'):
        response = client.get(path)
        assert response.status_code == 200
        assert b'unavailable' in response.data.lower()
    assert hub.app_status(managed)['state'] == 'unavailable'


@pytest.mark.parametrize('action', ['start', 'stop', 'restart'])
@pytest.mark.parametrize('failure', [subprocess.TimeoutExpired('sv', 12), OSError('supervisor lost')])
def test_service_control_failures_never_use_process_fallback(managed, monkeypatch, action, failure):
    monkeypatch.setattr(hub.subprocess, 'run', Mock(side_effect=failure))
    monkeypatch.setattr(hub, '_pids_for', Mock(side_effect=AssertionError('unsafe fallback')))
    monkeypatch.setattr(hub, '_start_command', Mock(side_effect=AssertionError('duplicate launch')))
    with pytest.raises(RuntimeError, match='supervisor unavailable'):
        getattr(hub, action + '_app')(managed)


def test_supervised_start_ignores_old_process_pattern(managed, monkeypatch):
    monkeypatch.setattr(hub, '_pids_for', Mock(side_effect=AssertionError('obsolete pattern')))
    run = Mock(return_value=subprocess.CompletedProcess([], 0, 'ok', ''))
    monkeypatch.setattr(hub.subprocess, 'run', run)
    hub.start_app(managed)
    assert run.call_args.args[0][1] == 'up'


def test_runtime_lock_blocks_all_control_routes_and_updates(managed, monkeypatch):
    managed['actions'] = [{'id': 'scan', 'command': ['python', 'scan.py']}]
    managed['update_ready'] = True
    monkeypatch.setattr(hub, '_find_app', lambda _: managed)
    monkeypatch.setattr(hub, '_load_registry', lambda: [managed])
    actions = [Mock() for _ in range(4)]
    for name, action in zip(('start_app', 'stop_app', 'restart_app', '_start_command'), actions):
        monkeypatch.setattr(hub, name, action)
    client = hub.create_app().test_client()
    with client.session_transaction() as session:
        session['csrf_token'] = 'token'
    # No queued status JSON exists: the OS lock alone must enforce exclusion.
    with hub.runtime_action('mediahub'):
        for path in ('/apps/mediahub/start', '/apps/mediahub/stop', '/apps/mediahub/restart',
                     '/apps/mediahub/action/scan', '/restart-all'):
            assert client.post(path, data={'csrf_token': 'token'}).status_code == 302
        with pytest.raises(RuntimeError, match='already running'):
            hub.start_update(managed)
    for action in actions:
        action.assert_not_called()
    client.post('/apps/mediahub/start', data={'csrf_token': 'token'})
    actions[0].assert_called_once_with(managed)


def test_update_log_read_is_bounded(tmp_path, monkeypatch):
    path = tmp_path / 'update.log'
    path.write_bytes(b'x' * 1_000_000 + b'\nLatest update\n')
    monkeypatch.setattr(Path, 'read_text', Mock(side_effect=AssertionError('unbounded read')))
    text = hub.tail_update_log(path)
    assert len(text) == 16000 and text.endswith('Latest update\n')


@pytest.mark.parametrize('state', ['request_rejected', 'request_repair_required', 'partial', 'interrupted'])
def test_blocked_or_incomplete_scan_never_reports_operational(monkeypatch, state):
    monkeypatch.setattr(run_state, 'read_status', lambda: {'state': state})
    monkeypatch.setattr(health_ui, '_json_file', lambda _: {'ok': True, 'health_ok': True})
    snapshot = health_ui._snapshot()
    assert not snapshot['ok']
    assert snapshot['guidance']


def test_request_diagnosis_is_not_reclassified_as_expired_auth(monkeypatch):
    @contextmanager
    def unlocked():
        yield True
    monkeypatch.setattr(automated_morning, 'single_scan_lock', unlocked)
    monkeypatch.setattr(automated_morning, '_run_once', lambda **_: {
        'ok': False, 'state': 'request_repair_required', 'reason': 'Recapture availability request'})
    status = Mock()
    monkeypatch.setattr(automated_morning, 'write_status', status)
    automated_morning.run()
    assert status.call_args.args[0] == 'request_repair_required'
