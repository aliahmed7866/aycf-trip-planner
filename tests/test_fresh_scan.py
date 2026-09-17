from datetime import date, datetime, timedelta
import json
from pathlib import Path
from unittest.mock import Mock
import sqlite3

import pytest

from cache_db import ScanCacheDB
from scanner import Flight, WizzSessionExpired
from termux import automated_morning, fresh_scan, run_state, supervisor, health_ui
import wizz_rate_limit as limits


@pytest.fixture
def fresh(monkeypatch, tmp_path):
    monkeypatch.setenv('AYCF_DB_PATH', str(tmp_path / 'aycf.sqlite3'))
    monkeypatch.setattr(run_state, 'STATE_DIR', tmp_path)
    monkeypatch.setattr(run_state, 'STATUS_FILE', tmp_path / 'scan-status.json')
    monkeypatch.setattr(run_state, 'LOCK_FILE', tmp_path / 'scan.lock')
    monkeypatch.setattr(supervisor, 'STATE_DIR', tmp_path)
    monkeypatch.setattr(supervisor, 'SUPERVISOR_FILE', tmp_path / 'supervisor-status.json')
    for name in ('_snapshot_history_after_scan', '_refresh_stability_after_scan', '_check_watches_after_scan'):
        monkeypatch.setattr(automated_morning, name, lambda: {})
    monkeypatch.setattr(automated_morning, '_check_feeders_after_scan', lambda _: None)
    db = ScanCacheDB()
    today = date.today()
    past = today - timedelta(days=7)
    for key, day in [('current', today), ('past', past)]:
        db.upsert_pdf_run(key, day.isoformat(), day.isoformat(), day.isoformat(), 1)
        dep = datetime.combine(day, datetime.min.time()) + timedelta(hours=12)
        flight = Flight('MAN', 'BUD', 'W123', dep, dep+timedelta(hours=2), '', '')
        db.replace_route_check(key, 'Manchester', 'Budapest', day, [flight])
        db.mark_pdf_scanned(key)
        scan_id = db.start_scan(key)
        db.finish_scan(scan_id, 'failed' if key == 'current' else 'completed', 1, 1, 1)
    run_state.write_status('failed', 'Old failure')
    supervisor._save({'scan_pending': True, 'pending_since': 123, 'last_scan_failure': {'state': 'failed'},
                      'last_scan_attempt_at': 456, 'health_ok': True, 'last_health_success_at': 789})
    return db, today, past, tmp_path


def test_reset_preserves_searchable_flights_history_and_pacing_and_backs_up(fresh, monkeypatch):
    db, today, past, root = fresh
    state = limits.record_rate_limit()
    monkeypatch.setattr(limits.time, 'time', lambda: state['cooldown_until']+1)
    pacing_before = limits._path().read_bytes()
    def scan(force=False, *, locked_db=None, before_scan=None):
        before_scan()
        assert not force and locked_db.path == db.path
        assert not db.route_checked('current', 'Manchester', 'Budapest', today)
        assert len(db.get_flights('Manchester', 'Budapest', today, 'current')) == 1
        assert db.route_checked('past', 'Manchester', 'Budapest', past)
        assert db.get_pdf_run('past')['scanned_at']
        assert db.get_pdf_run('current')['scanned_at'] is None
        assert not supervisor._load(supervisor.SUPERVISOR_FILE)['scan_pending']
        # All three authorities remain held through reset and scanning.
        with run_state.single_scan_lock() as free:
            assert not free
        with db.scan_lock() as free:
            assert not free
        with run_state.process_lock(root / 'supervisor.lock') as free:
            assert not free
        return {'ok': True, 'state': 'complete'}
    monkeypatch.setattr(automated_morning, '_run_with_lock', scan)
    result = fresh_scan.run()
    assert result['fresh_reset']['checks_reset'] == 1
    backup = Path(result['fresh_reset']['backup'])
    with sqlite3.connect(backup / 'aycf.sqlite3') as old:
        assert old.execute("SELECT complete FROM route_checks WHERE pdf_run_id='current'").fetchone()[0] == 1
        assert old.execute("SELECT count(*) FROM scan_runs WHERE status='failed'").fetchone()[0] == 1
    assert json.loads((backup / 'scan-status.json').read_text())['message'] == 'Old failure'
    assert backup.stat().st_mode & 0o777 == 0o700
    assert (backup / 'aycf.sqlite3').stat().st_mode & 0o777 == 0o600
    with db.connect() as conn:
        assert conn.execute("SELECT count(*) FROM scan_runs WHERE status='failed'").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM scan_runs WHERE status='completed'").fetchone()[0] == 1
    assert limits._path().read_bytes() == pacing_before
    assert supervisor._load(supervisor.SUPERVISOR_FILE)['last_health_success_at'] == 789


@pytest.mark.parametrize('lock', ['scan', 'database', 'supervisor'])
def test_active_work_is_not_reset(fresh, monkeypatch, lock):
    db, today, _, root = fresh
    reset = Mock(side_effect=AssertionError('Cannot reset active work'))
    monkeypatch.setattr(fresh_scan, '_reset_pending', reset)
    manager = {'scan': run_state.single_scan_lock, 'database': db.scan_lock,
               'supervisor': lambda: run_state.process_lock(root / 'supervisor.lock')}[lock]
    with manager() as acquired:
        assert acquired
        assert fresh_scan.run()['state'] == 'already_running'
    reset.assert_not_called()
    assert db.route_checked('current', 'Manchester', 'Budapest', today)
    assert not (root / 'scan-reset-backups').exists()


def test_cooldown_allows_reset_and_queues_without_network_or_changing_deadline(fresh, monkeypatch):
    db, today, _, root = fresh
    state = limits.record_rate_limit(7200)
    before = limits._path().read_bytes()
    network = Mock(side_effect=AssertionError('No scan or auth work during cooldown'))
    monkeypatch.setattr(automated_morning.tiered_morning, '_run_locked', network)
    monkeypatch.setattr(automated_morning, '_refresh', network)
    result = fresh_scan.run()
    assert result['ok'] and result['queued'] and result['state'] == 'rate_limited'
    assert not result['scan_performed']
    assert result['cooldown_until'] == state['cooldown_until']
    assert not db.route_checked('current', 'Manchester', 'Budapest', today)
    assert len(db.get_flights('Manchester', 'Budapest', today, 'current')) == 1
    assert (Path(result['fresh_reset']['backup']) / 'aycf.sqlite3').exists()
    assert limits._path().read_bytes() == before
    assert run_state.read_status()['fresh_pending']
    assert supervisor._load(supervisor.SUPERVISOR_FILE)['scan_pending']
    assert 'Fresh scan queued' in run_state.read_status()['message']
    network.assert_not_called()


def test_backup_failure_aborts_before_clearing_work(fresh, monkeypatch):
    db, today, _, _ = fresh
    monkeypatch.setattr(fresh_scan.tempfile, 'mkdtemp', Mock(side_effect=OSError('disk full')))
    monkeypatch.setattr(automated_morning.tiered_morning, '_run_locked',
                        lambda db, force=False, before_scan=None: before_scan())
    with pytest.raises(OSError, match='disk full'):
        fresh_scan.run()
    assert db.route_checked('current', 'Manchester', 'Budapest', today)
    assert supervisor._load(supervisor.SUPERVISOR_FILE)['scan_pending']


def test_server_preflight_failure_defers_reset_and_later_supervisor_scan_completes_it(fresh, monkeypatch):
    import requests
    db, today, _, root = fresh
    response = requests.Response()
    response.status_code = 500
    scan = Mock(side_effect=requests.HTTPError('Fixture outage', response=response))
    monkeypatch.setattr(automated_morning.tiered_morning, '_run_locked', scan)
    result = fresh_scan.run()
    assert not result['reset_performed'] and result['fresh_reset_pending']
    assert db.route_checked('current', 'Manchester', 'Budapest', today)
    assert not (root / 'scan-reset-backups').exists()
    def recovered(locked_db, force=False, before_scan=None):
        before_scan()
        assert not db.route_checked('current', 'Manchester', 'Budapest', today)
        return {'ok': True}
    monkeypatch.setattr(automated_morning.tiered_morning, '_run_locked', recovered)
    assert automated_morning.run()['ok']
    assert len(list((root / 'scan-reset-backups').iterdir())) == 1
    assert not run_state.read_status().get('fresh_reset_pending')


def test_auth_recovery_does_not_repeat_reset(fresh, monkeypatch):
    db, today, _, root = fresh
    calls = []
    def scan(locked_db, force=False, before_scan=None):
        if before_scan:
            before_scan()
        calls.append(force)
        if len(calls) == 1:
            assert not db.route_checked('current', 'Manchester', 'Budapest', today)
            db.replace_route_check('current', 'Manchester', 'Budapest', today, [])
            raise WizzSessionExpired('session expired')
        assert db.route_checked('current', 'Manchester', 'Budapest', today)
        return {'ok': True, 'scan_performed': True}
    monkeypatch.setattr(automated_morning.tiered_morning, '_run_locked', scan)
    monkeypatch.setattr(automated_morning, '_refresh', lambda _: True)
    assert fresh_scan.run()['ok']
    assert calls == [False, False]
    assert len(list((root / 'scan-reset-backups').iterdir())) == 1
    assert run_state.read_status()['state'] == 'complete'


def test_new_429_keeps_new_progress_and_can_resume(fresh, monkeypatch):
    db, today, _, _ = fresh
    def scan(locked_db, force=False, before_scan=None):
        if before_scan:
            before_scan()
        db.replace_route_check('current', 'Manchester', 'Budapest', today, [])
        raise limits.WizzRateLimited(status=limits.record_rate_limit())
    monkeypatch.setattr(automated_morning.tiered_morning, '_run_locked', scan)
    result = fresh_scan.run()
    assert result['state'] == 'rate_limited'
    assert db.route_checked('current', 'Manchester', 'Budapest', today)
    assert run_state.read_status()['resume_scan']


@pytest.mark.parametrize('blocked', [False, True])
def test_ui_fresh_action_requires_csrf_and_uses_fresh_runtime_command(monkeypatch, tmp_path, blocked):
    from tests.test_rate_limit_status_ui import health_app
    monkeypatch.setattr(health_ui, 'LOG_DIR', tmp_path)
    monkeypatch.setattr(health_ui, 'rate_limit_summary', lambda: {'blocked': blocked})
    spawn = Mock()
    monkeypatch.setattr(health_ui.subprocess, 'Popen', spawn)
    client = health_app().test_client()
    client.post('/system/fresh-scan')
    spawn.assert_not_called()
    with client.session_transaction() as session:
        session['csrf_token'] = 'valid'
    assert client.post('/system/fresh-scan', data={'csrf_token': 'valid'}).status_code == 302
    assert spawn.call_args.args[0][-1] == 'fresh'
    assert spawn.call_args.kwargs['stdout'].name.endswith('manual-morning.log')


def test_runtime_dispatches_fresh_without_normal_force_flag(fresh, monkeypatch):
    from termux import runtime
    monkeypatch.setattr(runtime, 'prepare_runtime', lambda: None)
    monkeypatch.setattr(runtime.sys, 'argv', ['runtime.py', 'fresh'])
    start = Mock(return_value={'ok': True})
    monkeypatch.setattr(fresh_scan, 'run', start)
    monkeypatch.setattr(automated_morning, 'run', Mock(side_effect=AssertionError('Wrong entry point')))
    runtime.main()
    start.assert_called_once_with()


def test_queued_fresh_scan_resumes_after_cooldown_without_another_reset(fresh, monkeypatch):
    db, today, _, root = fresh
    clock = [200000.0]
    monkeypatch.setattr(limits.time, 'time', lambda: clock[0])
    monkeypatch.setattr(supervisor, '_hours', lambda: set())
    state = limits.record_rate_limit(7200)
    assert fresh_scan.run()['queued']
    blocked_send = Mock(side_effect=AssertionError('No provider work before retry time'))
    monkeypatch.setattr(supervisor, '_saved_session_health', blocked_send)
    monkeypatch.setattr(supervisor, '_run', blocked_send)
    assert supervisor.main() == 0
    blocked_send.assert_not_called()
    clock[0] = state['cooldown_until'] + 1
    monkeypatch.setattr(supervisor, '_saved_session_health', lambda: True)
    reset = Mock(side_effect=AssertionError('Queued fresh work must not reset a second time'))
    monkeypatch.setattr(fresh_scan, '_reset_pending', reset)
    monkeypatch.setattr(automated_morning.tiered_morning, 'run', lambda force=False: {'ok': True})
    commands = []
    def send(command, timeout):
        commands.append(command)
        assert command[-1] == 'morning'
        assert not db.route_checked('current', 'Manchester', 'Budapest', today)
        assert automated_morning.run()['ok']
        return 0
    monkeypatch.setattr(supervisor, '_run', send)
    assert supervisor.main() == 0
    assert len(commands) == 1
    assert not supervisor._load(supervisor.SUPERVISOR_FILE)['scan_pending']
    assert len(list((root / 'scan-reset-backups').iterdir())) == 1
    reset.assert_not_called()
