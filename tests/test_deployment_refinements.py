"""Run the real updater against a disposable checkout and local health endpoints."""
import fcntl
import json
import shlex
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
    shutil.copy2(ROOT / 'termux/checkout_processes.py', scripts / 'checkout_processes.py')
    (scripts / 'run-admin.sh').write_text('echo restart >> "$AYCF_STATE_DIR/admin-starts"\n')
    (repo / 'requirements-termux.txt').write_text('# fixture\n')
    (repo / 'watch_app.py').write_text('import time; time.sleep(90)\n')
    git = shutil.which('git')
    remote = tmp_path / 'origin.git'
    subprocess.run([git, 'init', '--bare', '-q', str(remote)], check=True)
    for args in [['init', '-q', '-b', 'deploy/termux'], ['config', 'user.name', 'Test'],
                 ['config', 'user.email', 'test@example.com'], ['add', '.'], ['commit', '-qm', 'test'],
                 ['remote', 'add', 'origin', str(remote)], ['push', '-qu', 'origin', 'deploy/termux']]:
        subprocess.run([git, *args], cwd=repo, check=True, capture_output=True)
    bins = tmp_path / 'bin'
    bins.mkdir()
    (bins / 'sleep').write_text('#!/bin/sh\nexec /bin/sleep .02\n')
    (bins / 'python').write_text(
        '#!/bin/sh\nif [ "$1 $2" = "-m pip" ]; then\n'
        '  echo install >> "$AYCF_STATE_DIR/pip-calls"\n'
        '  [ ! -e "$AYCF_STATE_DIR/fail-pip" ]; exit $?\nfi\n'
        + 'exec ' + shlex.quote(sys.executable) + ' "$@"\n')
    for path in bins.iterdir():
        if not path.is_symlink():
            path.chmod(0o755)
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            healthy = self.server.service == 'admin' or started.exists()
            self.send_response(200 if healthy else 503)
            self.end_headers()
            identity = 'aycf-admin' if self.server.service == 'admin' else 'aycf'
            self.wfile.write(json.dumps({'ok': healthy, 'service': identity}).encode())
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
    env = dict(os.environ, HOME=str(tmp_path), PATH=str(bins) + os.pathsep + os.environ['PATH'], AYCF_APP_DIR=str(repo),
               AYCF_STATE_DIR=str(state), AYCF_CONFIG_DIR=str(config))
    def run(*args):
        return subprocess.run(['bash', str(scripts / 'auto-deploy.sh'), *args], env=env, capture_output=True, text=True, timeout=10)
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
    repo, state, _, run = deployment
    with subprocess.Popen([sys.executable, 'watch_app.py'], cwd=repo) as process:
        try:
            result = run()
        finally:
            process.terminate()
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


def test_scheduled_deploy_preserves_explicit_stop_and_checks_admin(deployment):
    _, state, started, run = deployment
    (state / 'planner.down').touch()
    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    assert not started.exists()
    assert (state / 'deploy-status.txt').read_text().startswith('current ')

def test_failed_install_after_fast_forward_resumes_same_revision(deployment):
    repo, state, started, run = deployment
    upstream = repo.parent / 'author'
    subprocess.run(['git', 'clone', '-qb', 'deploy/termux', str(repo.parent / 'origin.git'), str(upstream)], check=True)
    (upstream / 'requirements-termux.txt').write_text('# new requirements\n')
    subprocess.run(['git', '-c', 'user.name=Test', '-c', 'user.email=test@example.com',
                    'commit', '-qam', 'Update requirements'], cwd=upstream, check=True)
    subprocess.run(['git', 'push', '-q'], cwd=upstream, check=True)
    target = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=upstream, text=True).strip()
    (state / 'fail-pip').touch()
    result = run('--restart')
    assert result.returncode and 'failed dependencies' in (state / 'deploy-status.txt').read_text()
    assert (state / 'deploy-pending').read_text().strip() == target
    assert not started.exists() and not (state / 'admin-starts').exists()
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip() == target
    (state / 'fail-pip').unlink()
    result = run('--restart')
    assert result.returncode == 0, result.stdout + result.stderr
    assert (state / 'deploy-status.txt').read_text().startswith('healthy ' + target)
    assert (state / 'pip-calls').read_text().splitlines() == ['install', 'install']
    assert started.exists() and not (state / 'deploy-pending').exists()


def test_manual_restart_on_current_revision_preserves_paused_planner(deployment):
    _, state, started, run = deployment
    (state / 'planner.down').touch()
    for _ in range(2):
        result = run('--restart')
        assert result.returncode == 0, result.stdout + result.stderr
    assert (state / 'admin-starts').read_text().splitlines() == ['restart', 'restart']
    assert not started.exists()
    assert (state / 'pip-calls').read_text().splitlines() == ['install']


def test_detached_hub_update_survives_replacing_its_own_server(deployment):
    import socket
    import time
    import requests
    from termux import checkout_processes

    repo, state, _, _ = deployment
    scripts = repo / 'termux'
    for name in ('admin_hub.py', 'hub_update.py', 'run-admin.sh', 'run-web.sh'):
        shutil.copy2(ROOT / 'termux' / name, scripts / name)
    shutil.copytree(ROOT / 'termux/templates', scripts / 'templates')
    (repo / 'watch_app.py').write_text(
        'import os\nfrom flask import Flask\napp = Flask(__name__)\n'
        '@app.get("/health")\ndef health(): return {"ok": True, "service": "aycf"}\n'
        'app.run(port=int(os.environ["PORT"]))\n')
    with socket.socket() as web_sock, socket.socket() as admin_sock:
        web_sock.bind(('127.0.0.1', 0))
        admin_sock.bind(('127.0.0.1', 0))
        web_port, admin_port = web_sock.getsockname()[1], admin_sock.getsockname()[1]
    (scripts / 'apps.json.example').write_text(json.dumps({'apps': [{
        'id': 'aycf', 'name': 'AYCF', 'port': web_port, 'working_dir': str(repo),
        'health_url': f'http://127.0.0.1:{web_port}/health', 'update_branch': 'deploy/termux',
        'update_command': ['bash', '$APP_ROOT/termux/auto-deploy.sh']}]}))
    for args in (['add', '.'], ['commit', '-qm', 'Live hub'], ['push', '-q']):
        subprocess.run(['git', *args], cwd=repo, check=True)
    config = repo.parent / 'config'
    (config / 'env').write_text(f'export PORT={web_port}\nexport AYCF_ADMIN_PORT={admin_port}\n')
    env = dict(os.environ, HOME=str(repo.parent), AYCF_APP_DIR=str(repo), AYCF_CONFIG_DIR=str(config),
               AYCF_STATE_DIR=str(state), PATH=str(repo.parent / 'bin') + os.pathsep + os.environ['PATH'])
    session = requests.Session()
    admin = f'http://127.0.0.1:{admin_port}'

    def wait_for(check):
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            try:
                if check():
                    return
            except (requests.ConnectionError, requests.Timeout, FileNotFoundError):
                pass
            time.sleep(.1)
        log = state / 'hub-updates/aycf.log'
        pytest.fail(log.read_text() if log.exists() else 'Server did not become ready')

    children = []
    try:
        for script in ['run-admin.sh', 'run-web.sh']:
            children.append(subprocess.Popen(['bash', scripts / script], cwd=repo, env=env,
                                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        wait_for(lambda: session.get(admin + '/health', timeout=1).json().get('service') == 'aycf-admin')
        wait_for(lambda: session.get(f'http://127.0.0.1:{web_port}/health', timeout=1).json().get('ok'))
        token = session.get(admin + '/workspace-status', timeout=3).json()['csrf']
        response = session.post(admin + '/apps/aycf/update', data={'csrf_token': token},
                                allow_redirects=False, timeout=3)
        assert response.status_code == 302
        receipt = state / 'hub-updates/aycf.json'
        wait_for(lambda: json.loads(receipt.read_text())['state'] in {'success', 'error', 'deferred'})
        assert json.loads(receipt.read_text())['state'] == 'success', (state / 'hub-updates/aycf.log').read_text()
        assert all(child.poll() is not None for child in children)
        assert session.get(admin + '/health', timeout=2).json()['service'] == 'aycf-admin'
        assert session.get(admin + '/workspace-status', timeout=3).json()['csrf'] == token
    finally:
        checkout_processes.terminate(lambda: checkout_processes.pids(repo, checkout_processes.WEB + checkout_processes.ADMIN))
        for child in children:
            child.wait(timeout=5)
        session.close()
