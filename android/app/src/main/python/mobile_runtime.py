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
        AYCF_SESSION_ENCRYPTION_KEY=key, AYCF_AUTO_REFRESH_WIZZ_SESSION='true',
        AYCF_WIZZ_CREDENTIAL_FILE=str(config / 'wizz_credentials.enc'),
        AYCF_MAX_AUTH_RECOVERIES_PER_SCAN='1',
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


def check_account(cookie_header, seconds_left):
    import requests
    from scanner import WizzAYCFClient, WizzEndpointUnavailable, WizzRequestRejected, WizzSessionExpired
    from termux.wizz_runtime import normalize_runtime
    from wizz_rate_limit import WizzRateLimited

    if seconds_left <= 0:
        return {'message': 'The wallet check timed out. Tap Connect to try again.'}
    deadline = time.monotonic() + seconds_left
    cookies = []
    for field in cookie_header.split(';'):
        name, sep, value = field.strip().partition('=')
        if sep and name and value:
            cookies.append(dict(name=name, value=value, domain='multipass.wizzair.com', path='/', secure=True))
    if not cookies:
        return {'message': 'Finish signing in on the Wizz page and open your Multipass wallet, then tap Connect.'}

    state = {'cookies': cookies}
    client = WizzAYCFClient(state)
    try:
        # A route probe belongs to the scan, where the user's actual routes and
        # dates are available. It cannot decide whether the wallet is signed in.
        client.bootstrap(deadline=deadline)
        runtime, _ = normalize_runtime(dict(availability_url=client.dynamic_url, station_ids=client.station_ids))
        from wizz_login_forms import _cookie_state
        return {'state': dict(_cookie_state(client.http), generation=secrets.token_hex(16)), 'runtime': runtime}
    except WizzRateLimited as exc:
        until = exc.status.get('retry_at')
        return {'message': f'Wizz has paused requests. Try again after {until}.' if until else
                'Wizz has paused requests. Check the retry time in Scan status.'}
    except WizzSessionExpired:
        return {'message': 'Wizz asked for sign-in or verification. Complete it on the Wizz page, open your wallet, then tap Connect.'}
    except WizzEndpointUnavailable:
        return {'message': 'AYCF could not recognise your pass on the Wizz page. Open your All You Can Fly wallet, then tap Connect.'}
    except WizzRequestRejected:
        return {'message': 'Wizz declined the wallet check. Complete any verification shown on the Wizz page before trying again.'}
    except (requests.Timeout, TimeoutError):
        return {'message': 'The wallet check timed out. Check your connection and tap Connect again.'}
    except requests.ConnectionError:
        return {'message': 'AYCF could not reach Wizz. Check your internet connection, then tap Connect again.'}
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        return {'message': f'Wizz returned HTTP {status}. Try again later.' if status else
                'Wizz could not load your wallet. Try again later.'}
    finally:
        client.http.close()


def save_checked_account(candidate):
    from morning_scan import _runtime_path
    from session_vault import SessionVault
    from termux.run_state import write_status
    from termux.wizz_runtime import write_runtime
    write_runtime(_runtime_path(), candidate['runtime'])
    SessionVault().save(candidate['state'])
    _record('android-auth.json', state='ready', retry_at=0)
    _record('wizz-session-status.json', ok=True, state='wallet_connected',
            detail='Wizz wallet connected. The scanner checks flight availability separately.')
    write_status('ready', 'Wizz wallet connected. Waiting for the flight scan.', wallet_scan_pending=True)



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
    from credential_vault import CredentialVault
    if not connected and not CredentialVault().exists():
        if action != 'auto':
            write_status('attention_required', 'Open the AYCF menu and sign in to Wizz.')
        return False
    # JobScheduler may already have an automatic wake queued when the user
    # connects. Honour the first scan even outside the morning window.
    if action == 'auto' and status.get('wallet_scan_pending'):
        action = 'morning'
    if action == 'auto':
        feeder_refresh.run()
        if status.get('state') in ('request_rejected', 'request_repair_required'):
            return False
        if automatic_scan_completed_today(status):
            return False
        pending = status.get('state') in ('running', 'partial', 'service_unavailable', 'rate_limited', 'interrupted', 'attention_required', 'auth_failed')
        if not pending and datetime.now(timezone.utc).hour not in _hours():
            return False
        if float(status.get('retry_at_epoch') or status.get('cooldown_until') or 0) > time.time():
            return False
    if not recover_account():
        return read_status().get('state') in {'service_unavailable', 'rate_limited'}
    log = Path(os.environ['AYCF_STATE_DIR']) / 'logs' / 'manual-morning.log'
    with log.open('a', buffering=1) as stream, redirect_stdout(stream), redirect_stderr(stream):
        prepare_runtime()
        if action in ('fresh', 'pending'):
            result = fresh_scan.run(preserve_completed=action == 'pending')
        else:
            result = automated_morning.run(force=action == 'morning')
    if result.get('state') == 'wizz_authentication_required':
        _record('wizz-session-status.json', ok=False, state='sign_in_required', detail='Open Menu → Sign in to Wizz.')
        android_bridge().notifySignIn('Open AYCF to complete Wizz sign-in or verification.')
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


def login_settings():
    from credential_vault import CredentialVault
    value = CredentialVault().load()
    return {'saved': bool(value), 'email': value['username'] if value else ''}


def set_login(email, password):
    from credential_vault import CredentialVault
    CredentialVault().save(email, password)
    _record('android-auth.json', state='ready', retry_at=0)
    from termux.run_state import write_status
    write_status('ready', 'Saved Wizz login. Checking your connection before the next scan.', wallet_scan_pending=True)


def forget_login():
    from credential_vault import CredentialVault
    CredentialVault().clear()
    _record('android-auth.json', state='ready', retry_at=0)


def recover_account():
    import json
    import requests
    from mobile_auth import recover, SignInNeeded
    from credential_vault import CredentialVault
    from session_vault import SessionVault
    from termux.run_state import write_status, process_lock
    from wizz_rate_limit import WizzRateLimited
    path = Path(os.environ['AYCF_STATE_DIR']) / 'android-auth.json'
    with process_lock(path.with_suffix('.lock')) as acquired:
        if not acquired:
            write_status('service_unavailable', 'Wizz connection recovery is already running.',
                         retry_at_epoch=int(time.time()) + 60)
            return False
        try:
            settings = json.loads(path.read_text()) if path.exists() else {}
        except (OSError, ValueError):
            settings = {}
        if settings.get('state') == 'attention_required':
            return False
        if float(settings.get('retry_at', 0)) > time.time():
            return False
        try:
            previous = SessionVault().load()
            credentials = CredentialVault().load()
        except RuntimeError:
            write_status('attention_required', 'Saved Wizz login could not be opened. Save it again in Wizz login settings.')
            return False
        # Persist the attempt before networking so process termination cannot
        # create a tight login retry loop.
        _record('android-auth.json', state='checking', retry_at=int(time.time()) + 900)
        try:
            candidate = recover(previous, time.monotonic() + 90)
            vault = SessionVault()
            with vault.lock():
                if CredentialVault().load() != credentials:
                    return False
                if vault.load() != previous:
                    # A foreground login or another scanner updated the account.
                    _record('android-auth.json', state='ready', retry_at=0)
                    return True
                from morning_scan import _runtime_path
                from termux.wizz_runtime import write_runtime
                write_runtime(_runtime_path(), candidate['runtime'])
                vault._save(candidate['state'])
            _record('android-auth.json', state='ready', retry_at=0)
            _record('wizz-session-status.json', ok=True, state='wallet_connected',
                    detail='Wizz connection checked automatically. Scanning can continue.')
            return True
        except WizzRateLimited as exc:
            retry = exc.status.get('cooldown_until') or time.time() + 900
            write_status('rate_limited', 'Wizz paused requests. AYCF will retry after the cooldown.',
                         cooldown_until=retry, resume_scan=True)
            _record('android-auth.json', state='waiting', retry_at=retry)
        except SignInNeeded as exc:
            _record('android-auth.json', state='attention_required', retry_at=int(time.time()) + 3600)
            write_status('attention_required', str(exc), retry_at_epoch=int(time.time()) + 3600)
            _record('wizz-session-status.json', ok=False, state='sign_in_required', detail=str(exc))
            if settings.get('state') != 'attention_required':
                android_bridge().notifySignIn(str(exc))
        except (requests.RequestException, TimeoutError):
            retry = int(time.time()) + 900
            _record('android-auth.json', state='waiting', retry_at=retry)
            write_status('service_unavailable', 'Wizz connection check could not finish. AYCF will retry automatically.',
                         retry_at_epoch=retry)
        return False
