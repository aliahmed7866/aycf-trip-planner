import importlib.util
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('hub_places', ROOT / 'termux/admin_hub.py')
hub = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hub)


def test_launcher_manage_and_custom_actions(monkeypatch, tmp_path):
    row = {'id': 'aycf', 'name': 'AYCF', 'state': 'running', 'healthy': True, 'available': True,
           'health_text': 'HTTP 200', 'service_text': 'PID 123', 'open_url': 'http://127.0.0.1:8080',
           'actions': [{'id': 'morning', 'label': 'Run morning scan'}]}
    monkeypatch.setattr(hub, '_load_registry', lambda: [row])
    monkeypatch.setattr(hub, 'app_status', lambda x: x)
    monkeypatch.setattr(hub, 'HOME', tmp_path)
    client = hub.create_app().test_client()
    home = client.get('/')
    assert b'app-tile' in home.data and b'action="/apps/' not in home.data
    manage = client.get('/manage')
    assert b'action="/apps/aycf/start"' in manage.data
    assert b'Run morning scan' in manage.data
    assert b'admin-pwa.js' in manage.data
    remote = hub.create_app().test_client().get('/manage', environ_base={'REMOTE_ADDR': '192.0.2.1'})
    assert b'Use your current AYCF app password' in remote.data


def test_restart_all_requires_csrf_skips_stopped_and_reports_failures(monkeypatch):
    rows = [{'id': str(i), 'name': str(i), 'state': state} for i, state in enumerate(['running', 'stopped', 'starting'])]
    monkeypatch.setattr(hub, '_load_registry', lambda: rows)
    monkeypatch.setattr(hub, 'app_status', lambda x: x)
    calls = []
    def restart(row):
        calls.append(row['id'])
        if row['id'] == '2':
            raise RuntimeError('restart failed')
    monkeypatch.setattr(hub, 'restart_app', restart)
    client = hub.create_app().test_client()
    client.post('/restart-all')
    assert calls == []
    with client.session_transaction() as session:
        session['csrf_token'] = 'token'
    client.post('/restart-all', data={'csrf_token': 'token'})
    assert calls == ['0', '2']
    with client.session_transaction() as session:
        assert any('restart failed' in message for _, message in session['_flashes'])


def test_command_manager_and_lightweight_health(monkeypatch, tmp_path):
    runner = tmp_path / 'places'
    runner.touch()
    monkeypatch.setattr(hub, 'HOME', tmp_path)
    monkeypatch.setattr(hub, 'APP_ROOT', ROOT)
    row = {'id': 'places', 'name': 'Places', 'manager': 'command',
           'status_command': [str(runner), 'status'], 'restart_command': [str(runner), 'restart']}
    calls = []
    def run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout='running pid=123', stderr='')
    monkeypatch.setattr(hub.subprocess, 'run', run)
    monkeypatch.setattr(hub, '_health', lambda url: (True, 'HTTP 200'))
    assert hub.app_status(row)['state'] == 'running'
    hub.restart_app(row)
    assert calls[-1] == [str(runner), 'restart']
    monkeypatch.setattr(hub, 'app_status', lambda x: (_ for _ in ()).throw(AssertionError('expensive probe')))
    monkeypatch.setattr(hub, '_load_registry', lambda: [row])
    assert hub.create_app().test_client().get('/health').json == {'ok': True, 'apps': 1}


def test_registry_preserves_custom_app_port_and_actions(monkeypatch, tmp_path):
    registry = tmp_path / 'apps.json'
    registry.write_text(json.dumps({'apps': [{'id': 'mediahub', 'name': 'Media Hub', 'port': 8095,
        'open_url': 'http://127.0.0.1:8095', 'actions': [{'id': 'custom'}]}]}))
    monkeypatch.setattr(hub, 'APP_ROOT', ROOT)
    monkeypatch.setattr(hub, 'REGISTRY_PATH', registry)
    apps = {row['id']: row for row in hub._load_registry()}
    assert apps['mediahub']['port'] == 8095
    assert apps['mediahub']['actions'] == [{'id': 'custom'}]
    assert apps['mediahub']['process_match']
    assert apps['places']['manager'] == 'command'
