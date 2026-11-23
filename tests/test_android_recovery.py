import importlib
import json
import os
from pathlib import Path
from unittest.mock import Mock

from cryptography.fernet import Fernet
import pytest
import requests

from credential_vault import CredentialVault
from session_vault import SessionVault
from scanner import PRIVATE_PAGE, WizzAYCFClient

LOGIN = 'https://multipass.wizzair.com/auth/login'
WALLET = '<script>window.CVO.flightSearchUrlJson="/w6/subscriptions/json/availability/test";</script>'
FORM = '<form method="post" action="/auth/submit"><input name="csrf" type="hidden" value="fixture"><input name="username"><input name="password" type="password"></form>'


@pytest.fixture
def mobile(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / 'android/app/src/main/python'))
    monkeypatch.setenv('AYCF_SESSION_ENCRYPTION_KEY', Fernet.generate_key().decode())
    monkeypatch.setenv('WIZZ_SESSION_FILE', str(tmp_path / 'session.enc'))
    monkeypatch.setenv('AYCF_WIZZ_CREDENTIAL_FILE', str(tmp_path / 'credentials.enc'))
    monkeypatch.setenv('AYCF_ANDROID', '1')
    runtime = importlib.import_module('mobile_runtime')
    monkeypatch.setattr(runtime, 'android_bridge', Mock(return_value=Mock()))
    return runtime


def reply(url=PRIVATE_PAGE, text=WALLET, status=200, location=None):
    result = requests.Response()
    result.url, result.status_code, result._content = url, status, text.encode()
    if location:
        result.headers['Location'] = location
    return result


def test_saved_login_is_encrypted_and_can_be_forgotten(mobile):
    mobile.set_login('member@example.com', 'sample-password')
    content = CredentialVault().path.read_bytes()
    assert b'member@example.com' not in content and b'sample-password' not in content
    assert mobile.login_settings() == {'saved': True, 'email': 'member@example.com'}
    mobile.forget_login()
    assert not CredentialVault().exists()
    assert mobile.login_settings()['saved'] is False


def test_existing_session_rotates_and_next_process_uses_it(mobile, monkeypatch):
    original = {'cookies': [{'name': 'session', 'value': 'old', 'domain': 'multipass.wizzair.com', 'path': '/'}],
                'generation': 'one'}
    SessionVault().save(original)
    def send(session, method, url, **kwargs):
        session.cookies.set('session', 'rotated', domain='multipass.wizzair.com', path='/', secure=True)
        return reply()
    monkeypatch.setattr(requests.Session, 'request', send)
    client = WizzAYCFClient(original)
    client._request('GET', PRIVATE_PAGE)
    saved = SessionVault().load()
    assert saved['cookies'][0]['value'] == 'rotated'
    assert saved['cookies'][0]['secure']
    assert WizzAYCFClient(saved).http.cookies.get('session') == 'rotated'


def test_slow_worker_cannot_overwrite_newer_session_or_login(mobile):
    original = {'cookies': [{'name':'session','value':'old','domain':'multipass.wizzair.com','path':'/'}], 'generation':'one'}
    newer = dict(original, cookies=[dict(original['cookies'][0], value='new')])
    SessionVault().save(newer)
    stale = dict(original, cookies=[dict(original['cookies'][0], value='late')])
    SessionVault().merge_cookies(original, stale)
    assert SessionVault().load() == newer
    SessionVault().save(dict(original, generation='new-account'))
    SessionVault().merge_cookies(original, stale)
    assert SessionVault().load()['generation'] == 'new-account'


def test_pre_scan_recovers_with_one_standard_credential_post(mobile, monkeypatch):
    mobile.set_login('member@example.com', 'sample-password')
    calls = []
    def send(session, method, url, **kwargs):
        calls.append((method, url, kwargs))
        if len(calls) == 1:
            return reply(status=302, location=LOGIN)
        if len(calls) == 2:
            return reply(LOGIN, FORM)
        if len(calls) == 3:
            session.cookies.set('session', 'renewed', domain='multipass.wizzair.com', path='/')
            return reply(url, '', 302, PRIVATE_PAGE)
        return reply()
    monkeypatch.setattr(requests.Session, 'request', send)
    assert mobile.recover_account()
    assert [m for m, _, _ in calls] == ['GET','GET','POST','GET']
    assert calls[2][2]['data'] == {'csrf':'fixture','username':'member@example.com','password':'sample-password'}
    assert calls[3][2]['data'] is None
    assert SessionVault().load()['cookies'][0]['value'] == 'renewed'
    monkeypatch.setattr(requests.Session, 'request', Mock(return_value=reply()))
    assert mobile.recover_account()
    requests.Session.request.assert_called_once()


@pytest.mark.parametrize('response', [
    reply(LOGIN, '<p>Verification code required</p>'+FORM),
    reply(LOGIN, FORM.replace('/auth/submit', 'https://example.com/steal')),
    reply(LOGIN, FORM.replace('method="post"', 'method="get"')),
    reply(status=302, location='http://multipass.wizzair.com/login'),
    reply(status=302, location='https://wizzair.com.evil.example/login'),
])
def test_challenges_and_untrusted_destinations_stop_without_password(mobile, monkeypatch, response):
    mobile.set_login('member@example.com', 'sample-password')
    send = Mock(return_value=response)
    monkeypatch.setattr(requests.Session, 'request', send)
    assert not mobile.recover_account()
    assert send.call_count == 1
    assert send.call_args.kwargs['data'] is None
    assert not SessionVault().exists()
    assert not mobile.recover_account()
    assert send.call_count == 1


def test_outage_retains_session_and_schedules_retry(mobile, monkeypatch):
    original={'cookies':[],'generation':'one'}
    SessionVault().save(original)
    monkeypatch.setattr(requests.Session, 'request', Mock(return_value=reply(status=503)))
    assert not mobile.recover_account()
    from termux.run_state import read_status
    assert read_status()['state']=='service_unavailable'
    assert SessionVault().load()==original


def test_changed_credentials_during_recovery_cannot_save_old_account(mobile, monkeypatch):
    mobile.set_login('a@example.com', 'sample')
    import mobile_auth
    def recover(previous, deadline):
        mobile.forget_login()
        return {'state': {'cookies': [], 'generation':'late'}, 'runtime':{}}
    monkeypatch.setattr(mobile_auth,'recover',recover)
    assert not mobile.recover_account()
    assert not SessionVault().exists()
