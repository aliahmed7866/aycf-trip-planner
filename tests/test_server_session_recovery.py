"""Reproduce daily 500/retry loops without contacting Wizz or launching Chrome."""
import importlib
import subprocess
from unittest.mock import Mock

import pytest
import requests

from scanner import WizzRequestRejected
from termux import auth_recovery, automated_morning, run_state
from wizz_rate_limit import record_rate_limit


def failure(code=500):
    response = requests.Response()
    response.status_code = code
    return requests.HTTPError(f'HTTP {code}', response=response)


@pytest.fixture
def transport(monkeypatch):
    child = Mock(return_value=subprocess.CompletedProcess([], 0))
    monkeypatch.setattr(automated_morning.subprocess, 'run', child)
    monkeypatch.setattr(automated_morning, '_refresh', Mock(side_effect=AssertionError('No browser repair')))
    return child


def test_second_failed_scan_renews_then_resumes_without_force(monkeypatch, transport):
    scan = Mock(side_effect=[failure(), failure(), {'ok': True}])
    monkeypatch.setattr(automated_morning.tiered_morning, 'run', scan)
    assert automated_morning._run_once(True)['state'] == 'wizz_service_unavailable'
    transport.assert_not_called()
    # A new scheduler process still sees the first failed attempt.
    importlib.reload(auth_recovery)
    assert automated_morning._run_once(True) == {'ok': True}
    assert [call.kwargs['force'] for call in scan.call_args_list] == [True, True, False]
    transport.assert_called_once()
    command = transport.call_args.args[0]
    assert command[-1].endswith('/termux/refresh_wizz_direct.py')
    assert 'bash' not in command
    assert transport.call_args.kwargs['timeout'] == auth_recovery.refresh_timeout()


def test_recovery_preserves_locked_scan_and_reset_callback(monkeypatch, transport):
    auth_recovery.server_session_recovery_due()
    scan = Mock(side_effect=[failure(), {'ok': True}])
    monkeypatch.setattr(automated_morning.tiered_morning, '_run_locked', scan)
    db, callback = object(), Mock()
    assert automated_morning._run_once(True, locked_db=db, before_scan=callback)['ok']
    assert scan.call_count == 2
    assert all(call.args == (db,) for call in scan.call_args_list)
    assert scan.call_args.kwargs == {'force': False, 'before_scan': callback}


@pytest.mark.parametrize('rc', [10, 12, 14, 17, 20])
def test_failed_direct_login_keeps_outage_and_waits(monkeypatch, transport, rc):
    auth_recovery.server_session_recovery_due()
    transport.return_value.returncode = rc
    scan = Mock(side_effect=failure())
    monkeypatch.setattr(automated_morning.tiered_morning, 'run', scan)
    for _ in range(3):
        assert automated_morning._run_once(False)['state'] == 'wizz_service_unavailable'
    transport.assert_called_once()
    assert scan.call_count == 3


def test_real_outage_after_validated_login_cannot_loop(monkeypatch, transport):
    auth_recovery.server_session_recovery_due()
    scan = Mock(side_effect=failure())
    monkeypatch.setattr(automated_morning.tiered_morning, 'run', scan)
    assert automated_morning._run_once(False)['state'] == 'wizz_service_unavailable'
    assert scan.call_count == 2
    transport.assert_called_once()


@pytest.mark.parametrize('code', [502, 503, 504])
def test_other_server_errors_do_not_trigger_login(monkeypatch, transport, code):
    monkeypatch.setattr(automated_morning.tiered_morning, 'run', Mock(side_effect=failure(code)))
    for _ in range(3):
        assert automated_morning._run_once(False)['state'] == 'wizz_service_unavailable'
    transport.assert_not_called()


def test_timeout_preserves_retry(monkeypatch, transport):
    auth_recovery.server_session_recovery_due()
    transport.side_effect = subprocess.TimeoutExpired('direct', 300)
    monkeypatch.setattr(automated_morning.tiered_morning, 'run', Mock(side_effect=failure()))
    assert automated_morning._run_once(False)['state'] == 'wizz_service_unavailable'
    assert run_state.read_status()['state'] == 'service_unavailable'


@pytest.mark.parametrize('rc,state', [(5, 'request_rejected'), (6, 'rate_limited')])
def test_provider_stop_during_repair_does_not_resume(monkeypatch, transport, rc, state):
    auth_recovery.server_session_recovery_due()
    transport.return_value.returncode = rc
    scan = Mock(side_effect=failure())
    monkeypatch.setattr(automated_morning.tiered_morning, 'run', scan)
    assert automated_morning._run_once(False)['state'] == state
    scan.assert_called_once()
    assert run_state.read_status()['state'] == state


def test_existing_provider_cooldown_blocks_recovery(transport):
    auth_recovery.server_session_recovery_due()
    record_rate_limit(3600)
    from wizz_rate_limit import WizzRateLimited
    with pytest.raises(WizzRateLimited):
        automated_morning._refresh_after_server_failures()
    transport.assert_not_called()


def test_hourly_budget_survives_success_and_process_reload(monkeypatch):
    clock = Mock(return_value=100000)
    monkeypatch.setattr(auth_recovery.time, 'time', clock)
    assert not auth_recovery.server_session_recovery_due()
    assert auth_recovery.server_session_recovery_due()
    assert not auth_recovery.server_session_recovery_due(success=True)
    importlib.reload(auth_recovery)
    clock.return_value += 3599
    assert not auth_recovery.server_session_recovery_due()
    assert not auth_recovery.server_session_recovery_due()
    clock.return_value += 1
    assert auth_recovery.server_session_recovery_due()


def test_old_failures_and_disabled_renewal_do_not_trigger(monkeypatch):
    clock = Mock(return_value=100000)
    monkeypatch.setattr(auth_recovery.time, 'time', clock)
    assert not auth_recovery.server_session_recovery_due()
    clock.return_value += 21601
    assert not auth_recovery.server_session_recovery_due()
    monkeypatch.setenv('AYCF_AUTO_REFRESH_WIZZ_SESSION', 'false')
    assert not auth_recovery.server_session_recovery_due()


def test_direct_login_propagates_418_to_pause_status(monkeypatch):
    from termux import refresh_wizz_direct as direct
    monkeypatch.setattr(direct, '_main', Mock(side_effect=WizzRequestRejected('HTTP 418')))
    monkeypatch.setattr(direct, '_status', Mock())
    assert direct.main() == 5
    assert run_state.read_status()['state'] == 'request_rejected'


def test_login_transport_stops_on_418():
    from termux.refresh_wizz_from_chrome import _auth_request
    response = requests.Response()
    response.status_code = 418
    send = Mock(return_value=response)
    with pytest.raises(WizzRequestRejected):
        _auth_request(send, 'https://example.invalid/login')
    send.assert_called_once()
