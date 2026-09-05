"""Behavioural regressions for the deploy/termux AYCF audit."""
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from unittest.mock import patch

import pytest
from flask import Flask

from cache_db import ScanCacheDB
from itinerary_search import cached_scan_itineraries
from scan_scope import scope_fingerprint
from scanner import Flight
from termux import automated_morning, multi_search
import morning_scan
import tiered_morning

DAY = date.today()


class Graph:
    def __init__(self, edges=()):
        self.edges = set(edges)

    def edges_for_day(self, day):
        return self.edges

    def cities(self):
        return sorted({city for edge in self.edges for city in edge})


@pytest.fixture
def db(tmp_path):
    return ScanCacheDB(str(tmp_path / 'cache.sqlite3'))


def flight(a, b, code, departure=6, arrival=8):
    midnight = datetime.combine(DAY, datetime.min.time())
    return Flight(a, b, code, midnight + timedelta(hours=departure),
                  midnight + timedelta(hours=arrival), '', '')


def save(db, a, b, flights, run='run', day=DAY):
    db.replace_route_check(run, a, b, day, flights)


def search(db, graph, a, b, **kwargs):
    return cached_scan_itineraries(graph, db, a, b, DAY, days=1,
                                   pdf_run_id='run', **kwargs)[0]


def test_cached_baku_reverse_and_watch_routes_without_pdf_edges(db):
    save(db, 'Baku', 'Budapest', [flight('Baku', 'Budapest', 'reverse')])
    save(db, 'Baku', 'Rome', [flight('Baku', 'Rome', 'watch')])
    graph = Graph([('Budapest', 'Baku')])
    assert search(db, graph, 'Baku', 'Budapest')[0]['legs'][0]['flight_code'] == 'reverse'
    assert search(db, graph, 'Baku', 'Rome')[0]['legs'][0]['flight_code'] == 'watch'


def test_cached_edges_do_not_leak_other_run_or_day(db):
    save(db, 'Baku', 'Budapest', [flight('Baku', 'Budapest', 'old')], run='old')
    save(db, 'Baku', 'Rome', [], day=DAY + timedelta(days=1))
    assert search(db, Graph(), 'Baku', None) == []


def test_hub_filter_before_path_and_result_limits(db):
    for hub in ['A-unapproved', 'Z-approved']:
        save(db, 'Origin', hub, [flight('Origin', hub, hub + '1')])
        save(db, hub, 'Target', [flight(hub, 'Target', hub + '2', 10, 12)])
    rows = search(db, Graph(), 'Origin', 'Target', approved_hubs=['Z-approved'],
                  max_paths_per_day=1, limit=1)
    assert rows[0]['path'] == ['Origin', 'Z-approved', 'Target']
    assert search(db, Graph(), 'Origin', 'Target', approved_hubs=[]) == []


def test_journey_filter_before_both_limits(db):
    save(db, 'Origin', 'A-long', [flight('Origin', 'A-long', 'long', 6, 16)])
    save(db, 'Origin', 'Z-short', [flight('Origin', 'Z-short', 'short', 7, 9)])
    rows = search(db, Graph(), 'Origin', None, max_journey_minutes=180,
                  limit=1, max_paths_per_day=1)
    assert rows[0]['legs'][0]['flight_code'] == 'short'


def test_exact_airports_filtered_before_limit_in_both_directions(db):
    for a, b, physical_a, physical_b in [('London', 'Budapest', 'London Gatwick', 'Budapest'),
                                        ('Budapest', 'London', 'Budapest', 'London Gatwick')]:
        wrong_a = 'London Luton' if a == 'London' else a
        wrong_b = 'London Luton' if b == 'London' else b
        save(db, a, b, [flight(wrong_a, wrong_b, 'wrong'), flight(physical_a, physical_b, 'right', 7, 9)])
        rows = search(db, Graph(), a, b, limit=1, requested_origins=[physical_a], requested_destinations=[physical_b])
        assert rows[0]['legs'][0]['flight_code'] == 'right'
    save(db, 'London', 'Budapest', [flight('London', 'Budapest', 'ambiguous')])
    assert search(db, Graph(), 'London', 'Budapest', requested_origins=['London Gatwick']) == []
    assert len(search(db, Graph(), 'London', 'Budapest', requested_origins=['London'])) == 1


@pytest.mark.parametrize('return_trip', [False, True])
def test_multi_scan_endpoint_preserves_exact_selection(db, return_trip):
    graph = Graph([('London', 'Budapest'), ('Budapest', 'London')])
    save(db, 'London', 'Budapest', [flight('London Luton', 'Budapest', 'wrong'), flight('London Gatwick', 'Budapest', 'right', 7, 9)])
    save(db, 'Budapest', 'London', [flight('Budapest', 'London Luton', 'wrong-return'), flight('Budapest', 'London Gatwick', 'right-return', 7, 9)])
    app = Flask(__name__)
    app.secret_key = 'test-only'
    app.register_blueprint(multi_search.bp)
    with app.test_client() as client:
        with client.session_transaction() as session:
            session['csrf_token'] = 'csrf'
        with patch.object(multi_search, '_graph', return_value=graph), patch.object(multi_search, 'ScanCacheDB', return_value=db), \
             patch.object(multi_search, '_current_scope_run', return_value={'ready': True, 'run_id': 'run', 'scope': {'connection_hubs': []}}), \
             patch.object(multi_search, 'render_template', side_effect=lambda name, **context: context), \
             patch.dict(os.environ, {'AYCF_MAX_RESULTS': '1'}):
            response = client.post('/multi-scan', data={'csrf_token': 'csrf', 'origins': 'London Gatwick',
                'destinations': 'Budapest', 'start_date': DAY.isoformat(), 'days': '1',
                'return_trip': 'on' if return_trip else ''})
    assert response.status_code == 200
    assert response.json['outbound'][0]['legs'][0]['flight_code'] == 'right'
    if return_trip:
        assert response.json['returns'][0]['legs'][0]['flight_code'] == 'right-return'


def test_duplicate_watch_scope_identity():
    scope = {'watch_routes': [('Baku', 'Budapest')]}
    assert scope_fingerprint(scope) == scope_fingerprint({'watch_routes': [('Baku', 'Budapest'), ('baku', 'budapest')]})
    assert scope_fingerprint(scope) != scope_fingerprint({'watch_routes': [('Budapest', 'Baku')]})


def test_lock_recovers_orphans_and_never_reclaims_live_owner(db):
    old = db.start_scan('run')
    with db.scan_lock() as acquired:
        assert acquired
        assert not db.scan_in_progress('run')
        active = db.start_scan('run')
        with ScanCacheDB(db.path).scan_lock() as duplicate:
            assert not duplicate
        with db.connect() as conn:
            assert conn.execute('SELECT status FROM scan_runs WHERE id=?', (old,)).fetchone()[0] == 'interrupted'
            assert conn.execute('SELECT status FROM scan_runs WHERE id=?', (active,)).fetchone()[0] == 'running'


def test_killed_worker_lock_is_released_and_retry_recovers_record(db):
    code = "from cache_db import ScanCacheDB; import sys,time; db=ScanCacheDB(sys.argv[1]); lock=db.scan_lock(); assert lock.__enter__(); db.start_scan('run'); print('ready',flush=True); time.sleep(60)"
    child = subprocess.Popen([sys.executable, '-c', code, db.path], stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == 'ready'
        with db.scan_lock() as acquired:
            assert not acquired
        child.kill()
        child.wait(timeout=5)
        with db.scan_lock() as acquired:
            assert acquired
            assert not db.scan_in_progress('run')
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)


@pytest.mark.parametrize('scanner', [morning_scan, tiered_morning])
def test_core_entrypoints_obey_lock_even_for_forced_scan(db, scanner):
    with db.scan_lock(), patch.object(scanner, 'ScanCacheDB', return_value=db), \
         patch.object(scanner, '_run_locked') as run:
        assert scanner.run(force=True)['state'] == 'already_running'
        run.assert_not_called()


@pytest.mark.parametrize('result', [
    {'ok': True, 'skipped': True, 'state': 'already_running'},
    {'ok': True, 'skipped': True, 'reason': 'A scan for this PDF and scope is already running'},
    {'ok': False, 'reason': 'failed'},
    {'ok': True, 'skipped': True, 'reason': 'unknown skip'},
])
def test_wrapper_never_reports_unsuccessful_scan_as_complete(result):
    with patch.object(automated_morning, '_run_once', return_value=result), \
         patch.object(automated_morning, '_snapshot_history_after_scan') as history, \
         patch.object(automated_morning, '_refresh_stability_after_scan') as stability, \
         patch.object(automated_morning, '_check_watches_after_scan') as watches, \
         patch.object(automated_morning, 'write_status') as status:
        automated_morning.run()
    assert status.call_args.args[0] != 'complete'
    history.assert_not_called()
    stability.assert_not_called()
    watches.assert_not_called()


def test_shell_ci_rejects_syntax_error_in_second_script(tmp_path):
    from pathlib import Path
    workflow = Path('.github/workflows/tests.yml').read_text()
    block = workflow.split('      - name: Validate Termux shell scripts\n', 1)[1].split('      - name:', 1)[0]
    command = '\n'.join(line[10:] for line in block.splitlines() if line.startswith('          '))
    assert command
    scripts = tmp_path / 'termux'
    scripts.mkdir()
    (scripts / 'a.sh').write_text('true\n')
    broken = scripts / 'b.sh'
    broken.write_text('if then\n')
    assert subprocess.run(['bash', '-c', command], cwd=tmp_path, capture_output=True).returncode != 0
    broken.write_text('true\n')
    assert subprocess.run(['bash', '-c', command], cwd=tmp_path, capture_output=True).returncode == 0


def test_restart_command_invokes_handoff(tmp_path):
    from pathlib import Path
    import shutil
    termux = tmp_path / 'termux'
    termux.mkdir()
    shutil.copy('termux/aycf', termux / 'aycf')
    (termux / 'finish-full-deployment.sh').write_text('printf handoff-invoked\n')
    env = dict(os.environ, AYCF_APP_DIR=str(tmp_path), AYCF_ENV_FILE=str(tmp_path / 'absent'))
    result = subprocess.run(['bash', str(termux / 'aycf'), 'restart'], env=env, capture_output=True, text=True)
    assert result.returncode == 0
    assert result.stdout == 'handoff-invoked'


def test_legacy_scan_endpoint_preserves_exact_airport(db, tmp_path):
    import app as aycf_app
    import pandas as pd
    from types import SimpleNamespace
    graph = Graph([('London', 'Budapest')])
    graph.latest_frame = lambda: pd.DataFrame([{'departure_from': 'London', 'departure_to': 'Budapest', 'data_generated': '2026-09-05T07:00:00'}])
    with patch.dict(os.environ, {'AYCF_BIND_HOST': '127.0.0.1', 'AYCF_CACHE_DIR': str(tmp_path), 'AYCF_MAX_RESULTS': '1'}), \
         patch.object(aycf_app, 'update_data_if_needed', return_value=SimpleNamespace(data_dir=str(tmp_path))), \
         patch.object(aycf_app, 'CurrentRouteGraph', return_value=graph), \
         patch.object(aycf_app, 'ScanCacheDB', return_value=db), \
         patch.object(db, 'get_pdf_run', return_value={'scanned_at': 'done'}), \
         patch.object(aycf_app, 'scan_scope_with_preferences', return_value={'origins': ['London'], 'connection_hubs': []}), \
         patch.object(aycf_app, 'render_template', side_effect=lambda name, **context: context):
        flask_app = aycf_app.create_app()
        # Get the actual scoped run passed by the endpoint while using real cache search.
        original = aycf_app.cached_scan_itineraries
        def scoped_search(*args, **kwargs):
            run = kwargs['pdf_run_id']
            save(db, 'London', 'Budapest', [flight('London Luton', 'Budapest', 'wrong'), flight('London Gatwick', 'Budapest', 'right', 7, 9)], run=run)
            return original(*args, **kwargs)
        with flask_app.test_client() as client, patch.object(aycf_app, 'cached_scan_itineraries', side_effect=scoped_search):
            with client.session_transaction() as session:
                session['csrf_token'] = 'csrf'
            response = client.post('/scan', data={'csrf_token': 'csrf', 'origins': 'London Gatwick', 'destination': 'Budapest', 'days': '1'})
    assert response.status_code == 200
    assert response.json['outbound'][0]['legs'][0]['flight_code'] == 'right'
