"""Run the full phone launcher with missing data and unreachable catalogue service."""
import json
import os
from pathlib import Path
import shlex
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock

import pytest
import requests
from termux import service_health

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('cached', [False, True])
def test_full_launcher_serves_health_and_home_while_catalogue_is_unavailable(tmp_path, cached):
    state, config, cache, bins = (tmp_path / name for name in ('state', 'config', 'cache', 'bin'))
    for path in (state, config, cache, bins):
        path.mkdir()
    # Valid empty DBs occur after interrupted setup, before tables are created.
    sqlite3.connect(state / 'aycf.sqlite3').close()
    if cached:
        (cache / 'direct-data').mkdir()
        (cache / 'direct-data/routes.csv').write_text(
            'departure_from,departure_to,data_generated\nLondon,Budapest,2026-11-28T08:00:00\n')
    release = threading.Event()
    entered = threading.Event()
    class Offline(BaseHTTPRequestHandler):
        def do_GET(self):
            entered.set()
            release.wait(15)
            self.send_response(503)
            self.end_headers()
        def log_message(self, *_):
            pass
    upstream = ThreadingHTTPServer(('127.0.0.1', 0), Offline)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    (config / 'env').write_text(f"export PORT='{port}'\nexport AYCF_BIND_HOST='127.0.0.1'\n")
    (bins / 'python').write_text('#!/bin/sh\nexec ' + shlex.quote(sys.executable) + ' "$@"\n')
    (bins / 'python').chmod(0o700)
    env = dict(os.environ, AYCF_APP_DIR=str(ROOT), AYCF_CONFIG_DIR=str(config),
               AYCF_STATE_DIR=str(state), AYCF_TERMUX_DB_PATH=str(state / 'aycf.sqlite3'),
               AYCF_CACHE_DIR=str(cache), AYCF_PDF_URL=f'http://127.0.0.1:{upstream.server_port}/catalogue',
               AYCF_UPSTREAM_ZIP=f'http://127.0.0.1:{upstream.server_port}/archive',
               AYCF_APP_PASSWORD='', PATH=str(bins) + os.pathsep + os.environ['PATH'])
    env.pop('PORT', None)
    with (tmp_path / 'web.log').open('w+') as log:
        process = subprocess.Popen(['bash', str(ROOT / 'termux/run-web.sh')], env=env,
                                   stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline:
                try:
                    health = requests.get(f'http://127.0.0.1:{port}/health', timeout=.5)
                    if health.status_code == 200:
                        break
                except requests.ConnectionError:
                    pass
                time.sleep(.1)
            else:
                log.seek(0)
                pytest.fail(log.read())
            assert health.json()['service'] == 'aycf'
            assert not release.is_set()
            page = requests.get(f'http://127.0.0.1:{port}/', timeout=3)
            assert page.status_code == 200, page.text
            if not cached:
                assert entered.wait(1)
                # When a scan later writes a catalogue, the running UI sees it.
                (cache / 'direct-data').mkdir(exist_ok=True)
                (cache / 'direct-data/routes.csv').write_text(
                    'departure_from,departure_to,data_generated\nLondon,Budapest,2026-11-28T08:00:00\n')
                page = requests.get(f'http://127.0.0.1:{port}/', timeout=3)
                assert page.status_code == 200 and 'Budapest' in page.text
        finally:
            process.terminate()
            process.wait(timeout=5)
            release.set()
            upstream.shutdown()
            upstream.server_close()


def test_readiness_allows_slow_start_and_reports_wrong_app(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(service_health.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(service_health.time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(service_health, 'check', lambda service: (clock[0] >= 18, 'ready' if clock[0] >= 18 else 'starting'))
    assert service_health.wait_ready(['web'], 120) == {}
    assert clock[0] == 18


def test_health_probe_rejects_another_app_on_registered_port(monkeypatch):
    response = Mock()
    response.read.return_value = b'{"ok":true,"service":"mediahub"}'
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=None)
    monkeypatch.setattr(service_health, 'urlopen', lambda *a, **kw: response)
    healthy, detail = service_health.check('web')
    assert not healthy and 'another app' in detail


def test_legacy_web_launcher_is_found_without_matching_morning_jobs(tmp_path):
    from termux import checkout_processes
    script = tmp_path / 'termux/runtime.py'
    script.parent.mkdir()
    script.write_text('import time; time.sleep(60)\n')
    processes = [subprocess.Popen([sys.executable, str(script), command]) for command in ('web', 'morning')]
    try:
        found = checkout_processes.pids(tmp_path, checkout_processes.WEB)
        assert processes[0].pid in found
        assert processes[1].pid not in found
        checkout_processes.terminate(lambda: checkout_processes.pids(tmp_path, checkout_processes.WEB))
        processes[0].wait(timeout=3)
        assert processes[1].poll() is None
    finally:
        for process in processes:
            process.terminate()
            process.wait(timeout=3)
