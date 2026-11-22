import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

import scanner
import wizz_rate_limit as limits
from session_vault import SessionVault


@pytest.fixture
def mobile(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / 'android/app/src/main/python'))
    monkeypatch.setenv('WIZZ_SESSION_FILE', str(tmp_path / 'session.enc'))
    from cryptography.fernet import Fernet
    monkeypatch.setenv('AYCF_SESSION_ENCRYPTION_KEY', Fernet.generate_key().decode())
    import mobile_runtime
    return mobile_runtime


def response(code, text=''):
    value = requests.Response()
    value.status_code = code
    value.url = scanner.PRIVATE_PAGE
    value._content = text.encode()
    return value


def test_wallet_acceptance_is_independent_of_flight_availability(mobile, monkeypatch):
    wallet = response(200, '<script>window.CVO.flightSearchUrlJson="/w6/subscriptions/json/availability/test";</script>')
    send = Mock(return_value=wallet)
    monkeypatch.setattr(requests.Session, 'request', send)
    candidate = mobile.check_account('fixture=value', 18)
    assert 'message' not in candidate
    assert not SessionVault().exists()  # Abandoned or timed-out UI results cannot replace the account.
    send.assert_called_once()
    assert send.call_args.args == ('GET', scanner.PRIVATE_PAGE)
    assert send.call_args.kwargs['allow_redirects'] is False
    assert max(send.call_args.kwargs['timeout']) <= 10
    mobile.save_checked_account(candidate)
    assert SessionVault().load() == candidate['state']
    import os
    status = json.loads((Path(os.environ['AYCF_STATE_DIR']) / 'wizz-session-status.json').read_text())
    assert status['state'] == 'wallet_connected'


@pytest.mark.parametrize('reply, hint', [
    (response(200, '<html>public welcome page</html>'), 'recognise'),
    (response(302), 'sign-in'),
    (response(401), 'sign-in'),
    (response(418), 'declined'),
    (response(429), 'paused'),
    (response(503), 'HTTP 503'),
    (requests.ConnectionError('private details'), 'internet'),
    (requests.Timeout('private details'), 'timed out'),
])
def test_wallet_errors_are_actionable_and_preserve_previous_account(mobile, monkeypatch, reply, hint):
    original = {'cookies': [{'name': 'old', 'value': 'fixture'}]}
    SessionVault().save(original)
    send = Mock(side_effect=[reply])
    monkeypatch.setattr(requests.Session, 'request', send)
    result = mobile.check_account('fixture=value', 10)
    assert set(result) == {'message'}
    assert hint in result['message']
    assert 'private details' not in result['message']
    assert SessionVault().load() == original
    send.assert_called_once()


def test_busy_request_budget_never_sends_or_saves_a_late_login(mobile, monkeypatch):
    monkeypatch.setattr(requests.Session, 'request', Mock(side_effect=AssertionError('unexpected network')))
    limits.wait_for_request()
    count = limits.request_budget_status()['requests_24h']
    assert 'timed out' in mobile.check_account('fixture=value', .05)['message']
    assert limits.request_budget_status()['requests_24h'] == count
    assert not SessionVault().exists()
    assert 'timed out' in mobile.check_account('fixture=value', 0)['message']
    assert 'Finish signing in' in mobile.check_account('', 10)['message']
