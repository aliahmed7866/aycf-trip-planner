import base64
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
import hashlib
import hmac
import os
from pathlib import Path
import secrets
import threading
import time

_local_url = None
_web_lock = threading.Lock()


def android_bridge():
    from java import jclass
    return jclass('uk.aycf.mobile.Engine')


def configure(root, key):
    root = Path(root) / 'planner'
    state, config, cache = root / 'state', root / 'config', root / 'cache'
    for directory in (state / 'logs', config, cache / 'direct-data'):
        directory.mkdir(parents=True, exist_ok=True)
    os.environ.update(
        AYCF_ANDROID='1', AYCF_STATE_DIR=str(state), AYCF_CONFIG_DIR=str(config),
        AYCF_CACHE_DIR=str(cache), AYCF_DB_PATH=str(state / 'aycf.sqlite3'),
        AYCF_TERMUX_DB_PATH=str(state / 'aycf.sqlite3'), WIZZ_SESSION_FILE=str(state / 'wizz_session.enc'),
        AYCF_SESSION_ENCRYPTION_KEY=key, AYCF_AUTO_REFRESH_WIZZ_SESSION='false',
        FLASK_SECRET_KEY=hmac.new(base64.urlsafe_b64decode(key), b'flask', hashlib.sha256).hexdigest(),
    )
    import aycf_startup  # Install the same station aliases and password manager as Termux.
    _load_fare_key()
    # A fresh, offline installation has no observations or routes yet.
    catalog = cache / 'direct-data'
    if not any(catalog.glob('*.csv')):
        (catalog / 'empty.csv').write_text('departure_from,departure_to,data_generated\n')


def start_console():
    global _local_url
    with _web_lock:
        if _local_url:
            return _local_url
        from http.cookies import SimpleCookie
        from werkzeug.serving import make_server
        from termux.runtime import create_web_app
        app = create_web_app()
        app.config['ANDROID_ACTION'] = lambda action: android_bridge().requestAction(action)
        app.jinja_env.globals['android_app'] = True
        token = secrets.token_urlsafe(32)
        def private_app(env, respond):
            cookie = SimpleCookie()
            try:
                cookie.load(env.get('HTTP_COOKIE', ''))
                supplied = cookie['aycf_device'].value
            except (KeyError, ValueError):
                supplied = ''
            if env.get('HTTP_HOST') != host or not hmac.compare_digest(token, supplied):
                respond('403 Forbidden', [('Content-Type', 'text/plain')])
                return [b'Open AYCF from the Android app.']
            return app(env, respond)
        server = make_server('127.0.0.1', 0, private_app, threaded=True)
        host = f'127.0.0.1:{server.server_port}'
        threading.Thread(target=server.serve_forever, daemon=True, name='AYCF planner').start()
        _local_url = f'http://{host}/\n{token}'
        return _local_url


def connect_account(cookie_header):
    from morning_scan import CapturedRequestWizzClient, _runtime_path
    from session_vault import SessionVault
    from termux.wizz_runtime import apply_runtime, normalize_runtime, write_runtime
    from termux.run_state import write_status
    cookies = []
    for field in cookie_header.split(';'):
        name, sep, value = field.strip().partition('=')
        if sep and name:
            cookies.append(dict(name=name, value=value, domain='multipass.wizzair.com', path='/', secure=True))
    if not cookies:
        raise ValueError('Finish signing in to Wizz, then tap Connect.')
    state = {'cookies': cookies}
    client = CapturedRequestWizzClient(state)
    try:
        client.bootstrap()
        runtime, _ = normalize_runtime(dict(availability_url=client.dynamic_url, station_ids=client.station_ids))
        if not apply_runtime(client, runtime) or not client.preflight().get('availability_verified'):
            raise ValueError('Availability could not be confirmed.')
    finally:
        client.http.close()
    write_runtime(_runtime_path(), runtime)
    SessionVault().save(state)
    _record('wizz-session-status.json', ok=True, state='verified', detail='Wizz confirmed flight availability.')
    write_status('ready', 'Wizz connection verified. Your next scan can run.')


def _record(name, **values):
    from termux.wizz_runtime import write_runtime
    write_runtime(Path(os.environ['AYCF_STATE_DIR']) / name, dict(updated_at=int(time.time()), **values))


def run_job(action):
    from termux.runtime import prepare_runtime
    from termux.run_state import read_status, write_status, automatic_scan_completed_today
    from session_vault import SessionVault
    from termux import automated_morning, feeder_refresh, fresh_scan
    from termux.supervisor import _hours
    _load_fare_key()
    status = read_status()
    connected = SessionVault().exists()
    _record('supervisor-status.json', state='scheduled', health_ok=connected,
            message='Android checks for due scans when network and battery conditions permit.',
            last_wake_at=int(time.time()))
    if action == 'health':
        return False
    if not connected:
        if action != 'auto':
            write_status('attention_required', 'Open the AYCF menu and sign in to Wizz.')
        return False
    if action == 'auto':
        feeder_refresh.run()
        if status.get('state') in ('attention_required', 'auth_failed', 'request_rejected', 'request_repair_required'):
            return False
        if automatic_scan_completed_today(status):
            return False
        pending = status.get('state') in ('running', 'partial', 'service_unavailable', 'rate_limited', 'interrupted')
        if not pending and datetime.now(timezone.utc).hour not in _hours():
            return False
        if float(status.get('retry_at_epoch') or status.get('cooldown_until') or 0) > time.time():
            return False
    log = Path(os.environ['AYCF_STATE_DIR']) / 'logs' / 'manual-morning.log'
    with log.open('a', buffering=1) as stream, redirect_stdout(stream), redirect_stderr(stream):
        prepare_runtime()
        if action in ('fresh', 'pending'):
            result = fresh_scan.run(preserve_completed=action == 'pending')
        else:
            result = automated_morning.run(force=action == 'morning')
    if result.get('state') == 'wizz_authentication_required':
        _record('wizz-session-status.json', ok=False, state='sign_in_required', detail='Open Menu → Sign in to Wizz.')
        android_bridge().notifyWatch(42, 'AYCF needs your Wizz sign-in', 'Open AYCF and sign in to resume your flight scan.')
    return action != 'fresh' and result.get('state') in ('partial', 'wizz_service_unavailable', 'stale_pdf')


def _load_fare_key():
    from cryptography.fernet import Fernet
    path = Path(os.environ['AYCF_CONFIG_DIR']) / 'fare-key.enc'
    if path.exists():
        os.environ['AYCF_SERPAPI_KEY'] = Fernet(os.environ['AYCF_SESSION_ENCRYPTION_KEY'].encode()).decrypt(path.read_bytes()).decode()
    else:
        os.environ.pop('AYCF_SERPAPI_KEY', None)


def set_fare_key(key):
    from cryptography.fernet import Fernet
    path = Path(os.environ['AYCF_CONFIG_DIR']) / 'fare-key.enc'
    if key:
        tmp = path.with_suffix('.tmp')
        tmp.write_bytes(Fernet(os.environ['AYCF_SESSION_ENCRYPTION_KEY'].encode()).encrypt(key.encode()))
        tmp.replace(path)
    else:
        path.unlink(missing_ok=True)
    _load_fare_key()
