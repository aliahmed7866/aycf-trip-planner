"""End-to-end regressions for shared search and unattended runtime refinements."""
from datetime import date, datetime
from unittest.mock import patch

import pandas as pd
import pytest
from flask import Flask

import app as base_app
from cache_db import ScanCacheDB
from scanner import Flight
from search_support import decorate_itineraries
from termux import runtime, automated_morning, multi_search, health_ui, supervisor


class Graph:
    def cities(self):
        return ['London', 'Budapest', 'Kutaisi']

    def latest_frame(self):
        return pd.DataFrame([{'departure_from': 'London', 'departure_to': 'Budapest',
                              'data_generated': '2026-09-09T07:00:00'}])


def test_invalid_origin_cannot_turn_into_any_origin_discovery(tmp_path):
    app = Flask(__name__)
    app.secret_key = 'test'
    app.add_url_rule('/', 'index', lambda: 'index')
    app.register_blueprint(multi_search.bp)
    db = ScanCacheDB(str(tmp_path / 'scan.sqlite3'))
    with app.test_client() as client:
        with client.session_transaction() as session:
            session['csrf_token'] = 'csrf'
        with patch.object(multi_search, '_graph', return_value=Graph()), \
             patch.object(multi_search, 'ScanCacheDB', return_value=db), \
             patch.object(multi_search, '_current_scope_run', return_value={'ready': True, 'run_id': 'run', 'scope': {}}), \
             patch.object(multi_search, '_scanned_origins') as discover, \
             patch.object(multi_search, 'cached_scan_itineraries') as search:
            response = client.post('/multi-scan', data={'csrf_token': 'csrf', 'origins': 'Typo airport', 'destinations': 'Kutaisi'})
        assert response.status_code == 302
        discover.assert_not_called()
        search.assert_not_called()
        with client.session_transaction() as session:
            assert 'starting airports' in session['_flashes'][-1][1]


def test_invalid_limit_configuration_falls_back_in_multi_search(tmp_path, monkeypatch):
    monkeypatch.setenv('AYCF_MAX_RESULTS', 'not-a-number')
    monkeypatch.setenv('AYCF_MAX_PATHS_PER_DAY', '')
    app = Flask(__name__)
    app.secret_key = 'test'
    app.register_blueprint(multi_search.bp)
    with app.test_client() as client:
        with client.session_transaction() as session:
            session['csrf_token'] = 'csrf'
        with patch.object(multi_search, '_graph', return_value=Graph()), \
             patch.object(multi_search, 'ScanCacheDB', return_value=ScanCacheDB(str(tmp_path / 'scan.sqlite3'))), \
             patch.object(multi_search, '_current_scope_run', return_value={'ready': True, 'run_id': 'run', 'scope': {}}), \
             patch.object(multi_search, 'render_template', side_effect=lambda name, **context: context), \
             patch.object(multi_search, 'cached_scan_itineraries', return_value=([], 0)) as search:
            response = client.post('/multi-scan', data={'csrf_token': 'csrf', 'origins': 'London', 'destinations': 'Kutaisi'})
        assert response.status_code == 200
        assert search.call_args.kwargs['limit'] == 100
        assert search.call_args.kwargs['max_paths_per_day'] == 250
        assert response.json['max_layover_minutes'] == 2880
        assert response.json['max_journey_minutes'] == 0


def test_long_safe_connection_ranks_ahead_of_short_tight_connection():
    def row(second_departure, arrival):
        return {'path': ['London', 'Budapest', 'Kutaisi'], 'legs': [
            {'origin': 'London', 'destination': 'Budapest', 'departure': '2026-09-09T06:00:00', 'arrival': '2026-09-09T08:00:00'},
            {'origin': 'Budapest', 'destination': 'Kutaisi', 'departure': second_departure, 'arrival': arrival},
        ]}
    tight = row('2026-09-09T10:10:00', '2026-09-09T14:00:00')
    safe = row('2026-09-11T08:00:00', '2026-09-11T12:00:00')
    result = decorate_itineraries([tight, safe])
    assert result[0]['total_minutes'] > 48 * 60
    assert result[0]['connections'][0]['minutes'] == 48 * 60
    assert result[1]['risky_connection'] is True
    assert multi_search._decorate([tight, safe]) == result


@pytest.mark.parametrize('result, exit_code', [
    ({'ok': False, 'state': 'wizz_service_unavailable'}, 1),
    ({'ok': False, 'state': 'wizz_authentication_required'}, 1),
    ({'ok': True, 'state': 'already_current'}, 0),
    ({'ok': True, 'state': 'already_running'}, 0),
])
def test_morning_command_reports_failure_to_scheduler(monkeypatch, result, exit_code):
    monkeypatch.setattr(runtime.sys, 'argv', ['runtime.py', 'morning'])
    with patch.object(runtime, 'prepare_runtime'), patch.object(automated_morning, 'run', return_value=result):
        if exit_code:
            with pytest.raises(SystemExit) as error:
                runtime.main()
            assert error.value.code == exit_code
        else:
            runtime.main()


def test_direct_web_factory_includes_watches_stability_and_multi_search(tmp_path, monkeypatch):
    monkeypatch.setenv('AYCF_CACHE_DIR', str(tmp_path))
    monkeypatch.setenv('AYCF_DB_PATH', str(tmp_path / 'scan.sqlite3'))
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path / 'config'))
    monkeypatch.setenv('AYCF_BIND_HOST', '127.0.0.1')
    monkeypatch.setenv('AYCF_WEB_PROCESS', 'false')
    (tmp_path / 'direct-data').mkdir()
    (tmp_path / 'direct-data' / 'catalog.csv').write_text('unused; graph mocked')
    with patch.object(runtime, 'prepare_runtime'), patch.object(runtime, '_ensure_pdf_catalogue'), \
         patch.object(base_app, 'CurrentRouteGraph', return_value=Graph()):
        app = runtime.create_web_app()
    endpoints = {rule.endpoint for rule in app.url_map.iter_rules()}
    assert {'multi_search.scan', 'watches.watchlist', 'stability.page', 'places.page', 'scan_settings.page', 'system_health.page'} <= endpoints
    with app.test_client() as client:
        assert client.get('/health').status_code == 200
        page = client.get('/')
        assert page.status_code == 200
        assert b'System</a>' in page.data
        assert b'Search the cached flights instantly' not in page.data


def test_inventory_counts_only_selected_scope_and_survives_failed_refresh(tmp_path):
    db = ScanCacheDB(str(tmp_path / 'scan.sqlite3'))
    day = date(2026, 9, 9)
    flight = Flight('London', 'Budapest', 'W1', datetime(2026, 9, 9, 6), datetime(2026, 9, 9, 8), '', '')
    for run in ['old', 'current']:
        db.upsert_pdf_run(run, '2026-09-09T07:00:00', None, None, 1)
        db.replace_route_check(run, 'London', 'Budapest', day, [flight])
        db.mark_pdf_scanned(run)
    scan_id = db.start_scan('current')
    db.finish_scan(scan_id, 'failed', 0, 0, 0)
    assert db.stats('current')['cached_flights'] == 1
    assert db.stats('current')['scan']['status'] == 'failed'
    assert db.stats('unknown')['cached_flights'] == 0
    assert db.stats('unknown')['pdf'] is None
    assert db.stats()['cached_flights'] == 2


def test_healthy_wizz_session_overrides_old_browser_failure():
    result = health_ui._browser_bridge({'last_repair_rc': 21, 'last_repair_attempt_at': 100}, {'ok': True, 'updated_at': 200})
    assert result['state'] == 'not_needed'
    assert result['severity'] == 'neutral'


def test_service_outage_is_not_reported_as_operational():
    with patch.object(health_ui, '_json_file', side_effect=lambda name: {'ok': True} if name.startswith('wizz') else {'health_ok': True}), \
         patch('termux.run_state.read_status', return_value={'state': 'service_unavailable'}):
        assert health_ui._snapshot()['ok'] is False


def test_log_tail_is_bounded_and_keeps_recent_lines(tmp_path, monkeypatch):
    monkeypatch.setattr(health_ui, 'LOG_DIR', tmp_path)
    (tmp_path / 'large.log').write_bytes(b'x' * 200000 + b'\nfirst\nlast\n')
    result = health_ui._tail_log('large.log', lines=2)
    assert result['lines'] == ['first', 'last']


def test_supervisor_records_wake_during_scan_retry_cooldown():
    with patch.object(supervisor, '_load', return_value={'health_ok': True, 'last_health_at': 1000, 'last_scan_attempt_at': 1000}), \
         patch.object(supervisor, 'read_status', return_value={'state': 'complete'}), \
         patch.object(supervisor.time, 'time', return_value=1100), \
         patch.object(supervisor, '_hours', return_value=set(range(24))), \
         patch.object(supervisor, '_save') as save, patch.object(supervisor, '_run') as run:
        assert supervisor.main() == 0
    assert save.call_args.args[0]['last_wake_at'] == 1100
    run.assert_not_called()


def test_duplicate_supervisor_does_not_start_another_repair(tmp_path, monkeypatch):
    from termux.run_state import process_lock
    monkeypatch.setattr(supervisor, 'STATE_DIR', tmp_path)
    with process_lock(tmp_path / 'supervisor.lock') as acquired:
        assert acquired
        with patch.object(supervisor, '_run_cycle') as cycle:
            assert supervisor.main() == 0
        cycle.assert_not_called()


def test_new_browser_failure_is_not_hidden_by_stale_session_status():
    result = health_ui._browser_bridge({'last_repair_rc': 21, 'last_repair_attempt_at': 200},
                                       {'ok': True, 'updated_at': 100})
    assert result['state'] == 'pairing_lost'
