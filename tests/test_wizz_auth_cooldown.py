"""A Wizz request pause must never trigger login or replace a verified session."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from unittest.mock import Mock

import pytest
import requests

import wizz_rate_limit as rate
from termux import auto_login_wizz as browser_login
from termux import refresh_wizz_direct as direct
from termux import refresh_wizz_from_chrome as chrome
from termux import run_state

ROOT = Path(__file__).resolve().parents[1]
ENDPOINT = 'https://multipass.wizzair.com/w6/subscriptions/json/availability/test'


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    state = tmp_path / 'state'
    state.mkdir()
    monkeypatch.setenv('AYCF_STATE_DIR', str(state))
    monkeypatch.setenv('AYCF_WIZZ_RATE_LIMIT_PATH', str(state / 'wizz-rate-limit.sqlite3'))
    monkeypatch.setattr(run_state, 'STATE_DIR', state)
    monkeypatch.setattr(run_state, 'STATUS_FILE', state / 'scan-status.json')
    monkeypatch.setattr(chrome, 'STATUS_FILE', state / 'wizz-session-status.json')
    verified = b'{"ok":true,"state":"saved_session_reused","updated_at":100}\n'
    chrome.STATUS_FILE.write_bytes(verified)
    runtime = tmp_path / 'wizz_runtime.json'
    runtime.write_text(json.dumps({'availability_url': ENDPOINT}))
    monkeypatch.setattr(chrome, 'RUNTIME_FILE', runtime)
    monkeypatch.setattr(direct, 'RUNTIME_FILE', runtime)
    return state, verified


def throttle():
    rate.record_rate_limit(3600)
    rate.check_cooldown()


@pytest.mark.parametrize('entrypoint', [chrome, direct, browser_login])
def test_standalone_auth_entrypoints_skip_all_work_during_cooldown(isolated, monkeypatch, entrypoint):
    state, verified = isolated
    status = rate.record_rate_limit(3600)
    work = Mock(side_effect=AssertionError('Do not enter authentication during a cooldown'))
    monkeypatch.setattr(entrypoint, '_main', work)
    assert entrypoint.main() == 6
    work.assert_not_called()
    assert chrome.STATUS_FILE.read_bytes() == verified
    scan = run_state.read_status()
    assert scan['state'] == 'rate_limited' and scan['http_status'] == 429
    assert scan['retry_at'] == status['retry_at']
    assert scan['cooldown_until'] == status['cooldown_until']


def test_candidate_rate_limit_does_not_rediscover_endpoint(isolated, monkeypatch):
    client = Mock()
    client.preflight.side_effect = throttle
    monkeypatch.setattr(chrome, 'CapturedRequestWizzClient', lambda *args, **kwargs: client)
    monkeypatch.setattr(chrome, '_normalize_runtime_in_place', lambda runtime: False)
    monkeypatch.setattr(chrome, 'apply_runtime', lambda *args: True)
    rediscover = Mock()
    monkeypatch.setattr(chrome, '_rediscover_endpoint', rediscover)
    with pytest.raises(rate.WizzRateLimited):
        chrome._validate_candidate({'cookies': []}, {})
    client.preflight.assert_called_once()
    rediscover.assert_not_called()


@pytest.mark.parametrize('previous,resume', [
    ({'state': 'complete'}, False),
    ({'state': 'running'}, True),
    ({'state': 'partial'}, True),
    ({'state': 'rate_limited', 'resume_scan': False}, False),
    ({'state': 'rate_limited', 'resume_scan': True}, True),
])
def test_auth_pause_preserves_existing_scan_intent(isolated, previous, resume):
    state = dict(previous)
    run_state.write_status(state.pop('state'), **state)
    rate.record_rate_limit(3600)
    assert chrome.main(cooldown_only=True) == 6
    assert run_state.read_status()['resume_scan'] is resume


def test_saved_session_rate_limit_propagates_without_invalidating_vault(isolated, monkeypatch):
    vault = Mock()
    vault.load.return_value = {'cookies': []}
    monkeypatch.setattr(chrome, 'SessionVault', lambda: vault)
    monkeypatch.setattr(chrome, '_validate_candidate', lambda *args: throttle())
    status = Mock()
    monkeypatch.setattr(chrome, '_status', status)
    with pytest.raises(rate.WizzRateLimited):
        chrome._try_saved_session({})
    vault.save.assert_not_called()
    vault.clear.assert_not_called()
    status.assert_not_called()


def test_saved_session_rate_limit_returns_six_without_browser_fallback(isolated, monkeypatch):
    _, verified = isolated
    monkeypatch.setattr(chrome, '_normalize_runtime_in_place', lambda runtime: False)
    monkeypatch.setattr(chrome, '_try_saved_session', lambda runtime: throttle())
    browser = Mock()
    monkeypatch.setattr(chrome, '_json_get', browser)
    assert chrome.main() == 6
    browser.assert_not_called()
    assert chrome.STATUS_FILE.read_bytes() == verified


@pytest.mark.parametrize('request_stage', ['wallet', 'login'])
def test_direct_http_429_preserves_session_and_provider_deadline(isolated, monkeypatch, request_stage):
    _, verified = isolated
    credentials = Mock()
    credentials.load.return_value = {'username': 'example-user', 'password': 'example-password'}
    monkeypatch.setattr(direct, 'CredentialVault', lambda: credentials)
    vault = Mock()
    monkeypatch.setattr(direct, 'SessionVault', lambda: vault)
    response = requests.Response()
    response.status_code = 429
    response.headers['Retry-After'] = '7200'
    response.url = 'https://multipass.wizzair.com/example-login'
    response._content = b'Too many requests'
    session = Mock()
    session.headers = {}
    session.get.return_value = response
    if request_stage == 'login':
        form = requests.Response()
        form.status_code = 200
        form.url = response.url
        form._content = (b'<form method="post" action="/example-login">'
                         b'<input name="username" type="text"><input name="password" type="password"></form>')
        session.get.return_value = form
        session.post.return_value = response
    monkeypatch.setattr(direct.requests, 'Session', lambda: session)
    # Check actual shared persistence without introducing wall-clock waits.
    monkeypatch.setattr(chrome, 'wait_for_request', lambda interval: rate.check_cooldown())
    before = time.time()
    assert direct.main() == 6
    saved = rate.rate_limit_status()
    assert saved['blocked'] and saved['cooldown_until'] >= before + 7200
    assert session.get.call_count == 1
    assert session.post.call_count == (1 if request_stage == 'login' else 0)
    vault.save.assert_not_called()
    vault.clear.assert_not_called()
    assert chrome.STATUS_FILE.read_bytes() == verified


def test_wallet_rediscovery_http_429_does_not_become_missing_endpoint(isolated, monkeypatch):
    response = requests.Response()
    response.status_code = 429
    response.headers['Retry-After'] = '3600'
    client = Mock()
    client.http.get.return_value = response
    monkeypatch.setattr(chrome, 'wait_for_request', lambda interval: rate.check_cooldown())
    with pytest.raises(rate.WizzRateLimited):
        chrome._rediscover_endpoint(client)
    assert rate.rate_limit_status()['blocked']
    client.http.get.assert_called_once()


def test_browser_submit_is_blocked_if_cooldown_starts_mid_login(isolated, monkeypatch):
    rate.record_rate_limit(3600)
    send = Mock()
    monkeypatch.setattr(browser_login, '_cdp_call', send)
    with pytest.raises(rate.WizzRateLimited):
        browser_login._eval('ws://example-browser', 'submit-login')
    send.assert_not_called()


@pytest.mark.parametrize('blocked_at', ['entry', 'direct'])
def test_shell_rate_limit_exit_skips_adb_and_browser_fallback(tmp_path, blocked_at):
    checkout = tmp_path / 'custom-checkout'
    (checkout / 'termux').mkdir(parents=True)
    shutil.copyfile(ROOT / 'termux/auto-refresh-wizz.sh', checkout / 'termux/auto-refresh-wizz.sh')
    binary = tmp_path / 'bin'
    binary.mkdir()
    capture = tmp_path / 'calls'
    python = binary / 'python'
    python.write_text('#!/bin/sh\n'
                      'printf "%s|%s\\n" "$1" "${2:-}" >> "$AYCF_TEST_CAPTURE"\n'
                      'if [ "${2:-}" = --cooldown-only ]; then\n'
                      '  [ "$AYCF_TEST_BLOCKED_AT" = entry ] && exit 6\n'
                      '  exit 0\n'
                      'fi\n'
                      'case "$1" in */refresh_wizz_direct.py) exit 6 ;; esac\n'
                      'exit 0\n')
    python.chmod(0o700)
    adb = binary / 'adb'
    adb.write_text('#!/bin/sh\nprintf "adb\\n" >> "$AYCF_TEST_CAPTURE"\nexit 1\n')
    adb.chmod(0o700)
    env = {key: value for key, value in os.environ.items() if not key.startswith('AYCF_')}
    env.update(PATH=str(binary) + ':' + os.environ['PATH'], AYCF_CONFIG_DIR=str(tmp_path / 'config'),
               AYCF_TEST_CAPTURE=str(capture), AYCF_TEST_BLOCKED_AT=blocked_at)
    result = subprocess.run(['bash', str(checkout / 'termux/auto-refresh-wizz.sh')],
                            env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 6
    calls = capture.read_text().splitlines()
    assert calls[0] == f'{checkout}/termux/refresh_wizz_from_chrome.py|--cooldown-only'
    assert not any('adb' in line or 'auto_login_wizz.py' in line for line in calls)
    assert len(calls) == (1 if blocked_at == 'entry' else 3)
