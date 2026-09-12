"""Run the real updater against a disposable checkout and local health endpoints."""
import fcntl
import os
import shutil
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def deployment(tmp_path):
    repo = tmp_path / 'repo'
    scripts = repo / 'termux'
    scripts.mkdir(parents=True)
    state = tmp_path / 'state'
    state.mkdir()
    config = tmp_path / 'config'
    config.mkdir()
    started = state / 'started'
    (scripts / 'auto-deploy.sh').write_text((ROOT / 'termux/auto-deploy.sh').read_text())
    (scripts / 'run-web.sh').write_text('touch "$AYCF_STATE_DIR/started"\n')
    (scripts / 'admin_hub.py').write_text('# Marks the admin as a required service\n')
    git = shutil.which('git')
    for args in [['init', '-q'], ['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '-qm', 'test']]:
        subprocess.run([git, *args], cwd=repo, check=True, capture_output=True)
    bins = tmp_path / 'bin'
    bins.mkdir()
    # Read the real working tree; remote fetch is intentionally local and inert.
    (bins / 'git').write_text(f'''#!/bin/bash
if [ "$1" = fetch ]; then exit 0; fi
if [ "$1" = rev-parse ] && [ "$2" = FETCH_HEAD ]; then exec {git} rev-parse HEAD; fi
exec {git} "$@"
''')
    (bins / 'pgrep').write_text('''#!/bin/bash
case "$*" in
  *watch_app.py*) [ -f "$AYCF_STATE_DIR/process-present" ]; exit $? ;;
esac
exit 1
''')
    # The real updater waits for its background launcher. An immediate no-op
    # races the fixture's touch process on busy runners, especially Python 3.14.
    # Wait for that marker with a bound; never manufacture a healthy response.
    (bins / 'sleep').write_text('''#!/bin/bash
if [ "$1" = 4 ]; then
  for attempt in {1..200}; do
    [ -f "$AYCF_STATE_DIR/started" ] && exit 0
    /bin/sleep 0.01
  done
fi
exit 0
''')
    (bins / 'python').symlink_to(sys.executable)
    for path in bins.iterdir():
        if not path.is_symlink():
            path.chmod(0o755)
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            healthy = self.server.service == 'admin' or started.exists()
            self.send_response(200 if healthy else 503)
            self.end_headers()
        def log_message(self, *_):
            pass
    servers = []
    for service in ['web', 'admin']:
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server.service = service
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
    # The updater must load custom ports from the existing private env file.
    (config / 'env').write_text(f'export PORT={servers[0].server_port}\nexport AYCF_ADMIN_PORT={servers[1].server_port}\n')
    env = dict(os.environ, PATH=str(bins) + os.pathsep + os.environ['PATH'], AYCF_APP_DIR=str(repo),
               AYCF_STATE_DIR=str(state), AYCF_CONFIG_DIR=str(config))
    def run():
        return subprocess.run(['bash', str(scripts / 'auto-deploy.sh')], env=env, capture_output=True, text=True, timeout=10)
    yield repo, state, started, run
    for server in servers:
        server.shutdown()
        server.server_close()


def test_current_checkout_restarts_missing_planner_and_releases_lock(deployment):
    repo, state, started, run = deployment
    # A leftover directory from the old mkdir lock no longer disables deployment.
    (state / 'deploy.lock').mkdir()
    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert started.exists()
    assert (state / 'deploy-status.txt').read_text().startswith('current ')
    with (state / 'deploy.flock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_current_checkout_does_not_claim_health_when_planner_fails(deployment):
    _, state, _, run = deployment
    (state / 'process-present').touch()
    result = run()
    assert result.returncode == 1
    assert (state / 'deploy-status.txt').read_text().startswith('unhealthy ')


def test_live_deploy_lock_blocks_another_update(deployment):
    _, state, started, run = deployment
    with (state / 'deploy.flock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = run()
    assert result.returncode == 0
    assert 'already running' in result.stdout
    assert not started.exists()
    assert not (state / 'deploy-status.txt').exists()


def test_permissions_do_not_block_updates_but_content_edits_do(deployment):
    repo, state, started, run = deployment
    started.touch()
    path = repo / 'termux/run-web.sh'
    path.chmod(0o700)
    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert (state / 'deploy-status.txt').read_text().startswith('current ')
    path.write_text(path.read_text() + '# personal change\n')
    result = run()
    assert result.returncode == 0
    assert (state / 'deploy-status.txt').read_text().startswith('deferred dirty ')
