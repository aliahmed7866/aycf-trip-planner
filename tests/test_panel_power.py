"""Exercise the panel's normal POST routes against real local processes."""
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time

import pytest
from termux import admin_hub as hub

ROOT = Path(__file__).resolve().parents[1]
SERVER = """from http.server import BaseHTTPRequestHandler, HTTPServer
import os
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"ok":true,"service":"akhlaq"}')
HTTPServer(('127.0.0.1', int(os.environ['PORT'])), Health).serve_forever()
"""


@pytest.fixture
def panel(tmp_path, monkeypatch):
    root = tmp_path / 'app'
    root.mkdir()
    (root / 'serve.py').write_text(SERVER)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    item = dict(id='akhlaq', name='Akhlaq', working_dir=str(root), port=port,
                start=['python', 'serve.py'], process_script='serve.py',
                health_url=f'http://127.0.0.1:{port}/health')
    monkeypatch.setattr(hub, 'HOME', tmp_path)
    monkeypatch.setattr(hub, 'STATE_DIR', tmp_path / 'state')
    monkeypatch.setattr(hub, 'LOG_DIR', tmp_path / 'logs')
    monkeypatch.setattr(hub, '_load_registry', lambda: [item])
    client = hub.create_app().test_client()
    with client.session_transaction() as session:
        session['csrf_token'] = 'token'

    def post(action):
        response = client.post('/apps/akhlaq/' + action, data={'csrf_token': 'token'})
        assert response.status_code == 302
        with client.session_transaction() as session:
            return session['_flashes'][-1][1]
    yield item, post
    hub.checkout_processes.terminate(lambda: hub.checkout_processes.pids(root, ['serve.py']))


def test_repeated_controls_ignore_same_filename_in_other_checkout(panel, tmp_path):
    row, post = panel
    other = tmp_path / 'unrelated'
    other.mkdir()
    (other / 'serve.py').write_text('import time; time.sleep(60)')
    with subprocess.Popen([sys.executable, 'serve.py'], cwd=other) as neighbor:
        try:
            assert 'running and healthy' in post('start')
            first, = hub._owned_pids(row)
            assert 'running and healthy' in post('start')
            assert hub._owned_pids(row) == [first]
            assert 'running and healthy' in post('restart')
            assert first not in hub._owned_pids(row)
            assert 'is off' in post('stop')
            assert 'is off' in post('stop')
            assert hub._off_file(row).exists() and not hub._owned_pids(row)
            assert neighbor.poll() is None
        finally:
            neighbor.terminate()


def test_opted_in_recovery_skips_an_explicit_stop(panel, monkeypatch):
    from termux import recover_apps
    monkeypatch.setattr(recover_apps, 'hub', hub)
    row, post = panel
    post('start')
    hub.checkout_processes.terminate(lambda: hub._owned_pids(row))
    recover_apps.recover()
    assert hub.app_status(row)['state'] == 'running'
    post('stop')
    recover_apps.recover()
    assert hub.app_status(row)['state'] == 'stopped'


@pytest.mark.skipif(not shutil.which('runsv') or not shutil.which('sv'), reason='runit unavailable')
def test_runit_stop_survives_supervisor_recreation(panel, tmp_path, monkeypatch):
    row, post = panel
    monkeypatch.setenv('PREFIX', str(tmp_path / 'prefix'))
    service = hub._service_dir('test-app')
    service.mkdir(parents=True)
    (service / 'down').touch()
    run = service / 'run'
    run.write_text(f'#!/bin/sh\ncd "{row["working_dir"]}" || exit 1\nexport PORT={row["port"]}\nexec "{sys.executable}" serve.py\n')
    run.chmod(0o700)
    row['service'] = 'test-app'
    for cycle in range(2):
        supervisor = subprocess.Popen(['runsv', str(service)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 5
            while not (service / 'supervise' / 'ok').exists() and time.monotonic() < deadline:
                time.sleep(.05)
            assert hub.app_status(row)['state'] == 'stopped'
            assert 'running and healthy' in post('start')
            assert not (service / 'down').exists()
            assert 'is off' in post('stop')
            assert (service / 'down').exists()
        finally:
            supervisor.terminate()
            supervisor.wait(timeout=5)
