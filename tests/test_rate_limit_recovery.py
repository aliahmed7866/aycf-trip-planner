"""Persisted cooldown must survive scans, manual forcing and supervisor wakes."""
import json
from contextlib import contextmanager
from unittest.mock import Mock

import pytest

import wizz_rate_limit as limits
from termux import automated_morning as morning, run_state, supervisor


@contextmanager
def available():
    yield True


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    clock = [1_789_737_600.0]
    monkeypatch.setattr(limits.time, 'time', lambda: clock[0])
    monkeypatch.setattr(run_state, 'STATE_DIR', tmp_path)
    monkeypatch.setattr(run_state, 'STATUS_FILE', tmp_path / 'scan-status.json')
    monkeypatch.setattr(run_state, 'LOCK_FILE', tmp_path / 'scan.lock')
    monkeypatch.setattr(supervisor, 'STATE_DIR', tmp_path)
    monkeypatch.setattr(supervisor, 'SUPERVISOR_FILE', tmp_path / 'supervisor.json')
    monkeypatch.setattr(supervisor, 'WIZZ_STATUS_FILE', tmp_path / 'wizz.json')
    monkeypatch.setattr(supervisor, '_hours', lambda: set())
    monkeypatch.setattr(supervisor, 'single_scan_lock', available)
    monkeypatch.setattr(morning, 'single_scan_lock', available)
    run_state.write_status('complete', 'Previous saved scan')
    supervisor._save({'health_ok': True, 'last_health_at': clock[0],
                      'last_health_success_at': clock[0], 'last_scan_attempt_at': clock[0] - 3600})
    return clock


def scheduler_state():
    return json.loads(supervisor.SUPERVISOR_FILE.read_text())


def test_forced_scan_during_cooldown_does_no_scan_or_repair(recovery, monkeypatch):
    limited = limits.record_rate_limit(7200)
    scan, repair = Mock(), Mock()
    monkeypatch.setattr(morning, '_run_once', scan)
    monkeypatch.setattr(morning, '_refresh', repair)
    for _ in range(2):
        result = morning.run(force=True)
        assert result['state'] == 'rate_limited'
        assert not result['scan_performed']
        assert result['cooldown_until'] == limited['cooldown_until']
    scan.assert_not_called()
    repair.assert_not_called()
    assert run_state.read_status()['resume_scan'] is True
    assert limits.rate_limit_status()['cooldown_until'] == limited['cooldown_until']


def test_scan_rate_limit_returns_status_without_traceback_auth_or_maintenance(recovery, monkeypatch):
    def rate_limited(*a, **k):
        limits.record_rate_limit(0)
        limits.check_cooldown()
    monkeypatch.setattr(morning.tiered_morning, 'run', rate_limited)
    hooks = [Mock(), Mock(), Mock(), Mock()]
    for name, hook in zip(('_refresh', '_snapshot_history_after_scan', '_check_watches_after_scan', '_check_feeders_after_scan'), hooks):
        monkeypatch.setattr(morning, name, hook)
    result = morning.run(force=True)
    assert result['state'] == run_state.read_status()['state'] == 'rate_limited'
    assert result['http_status'] == 429 and result['scan_performed']
    assert all(not hook.called for hook in hooks)


def test_supervisor_honours_manual_failure_deadline_and_resumes_when_due(recovery, monkeypatch):
    limited = limits.record_rate_limit(7200)
    morning.run(force=True)  # a manual launch must be adopted by the supervisor
    health, commands = Mock(return_value=True), []
    monkeypatch.setattr(supervisor, '_saved_session_health', health)
    def launch(command, timeout):
        commands.append(command)
        run_state.write_status('complete', 'Resumed saved checks', scan_performed=True)
        return 0
    monkeypatch.setattr(supervisor, '_run', launch)
    for instant in (recovery[0], limited['cooldown_until'] - 1):
        recovery[0] = instant
        supervisor.main()
        assert scheduler_state()['state'] == 'rate_limited'
        assert scheduler_state()['scan_pending']
        assert commands == []
        health.assert_not_called()
    recovery[0] = limited['cooldown_until']
    supervisor.main()
    assert len(commands) == 1 and commands[0][-1] == 'morning'
    assert scheduler_state()['scan_pending'] is False
    assert limits.rate_limit_status()['effective_request_interval'] >= 5


def test_health_probe_rate_limit_preserves_healthy_state_without_browser_repair(recovery, monkeypatch):
    supervisor._save({**scheduler_state(), 'last_health_at': 0})
    def health():
        limits.record_rate_limit(1800)
        limits.check_cooldown()
    monkeypatch.setattr(supervisor, '_saved_session_health', health)
    run = Mock()
    monkeypatch.setattr(supervisor, '_run', run)
    supervisor.main()
    assert scheduler_state()['state'] == 'rate_limited'
    assert scheduler_state()['health_ok'] is True
    assert not scheduler_state().get('scan_pending')
    run.assert_not_called()
    supervisor.main()
    run.assert_not_called()
    assert not scheduler_state().get('scan_pending')


def test_rate_limit_during_scheduled_scan_is_reported_on_same_wake(recovery, monkeypatch):
    run_state.write_status('failed', 'Incomplete scan')
    def launch(command, timeout):
        state = limits.record_rate_limit(3600)
        run_state.write_status('rate_limited', limits.rate_limit_message(state), resume_scan=True,
                               cooldown_until=state['cooldown_until'])
        return 1
    monkeypatch.setattr(supervisor, '_run', launch)
    supervisor.main()
    assert scheduler_state()['state'] == 'rate_limited'
    assert scheduler_state()['scan_pending']
    assert scheduler_state()['health_ok'] is True
