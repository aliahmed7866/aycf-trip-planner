"""Daily scheduling regressions from the duplicate 18 September scan."""
import json
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from termux import automated_morning, run_state, supervisor
from tests.test_supervisor_recovery import cycle, saved


def test_manual_completion_during_window_satisfies_every_later_wake(cycle, monkeypatch):
    now, status, calls, _ = cycle
    monkeypatch.setattr(supervisor, '_hours', lambda: set(range(24)))
    supervisor._save({**saved(), 'scan_pending': True, 'pending_since': now - 3600})
    status.update(state='complete', scan_performed=True, updated_at=now - 840)
    for offset in (0, 900, 3600):
        monkeypatch.setattr(supervisor.time, 'time', lambda: now + offset)
        supervisor.main()
        assert not saved()['scan_pending']
        assert 'already completed today' in saved()['message']
    assert calls == []


def test_success_receipt_survives_status_replacement_and_scheduler_restart(cycle, monkeypatch):
    now, status, calls, _ = cycle
    monkeypatch.setattr(supervisor, '_hours', lambda: set(range(24)))
    run_state.write_status('complete', scan_performed=True)
    run_state.write_status('failed', 'Unrelated later maintenance error')
    status.update(run_state.read_status())
    supervisor.SUPERVISOR_FILE.unlink()
    supervisor.main()
    assert not calls
    assert not saved()['scan_pending']
    assert run_state.automatic_scan_completed_today()


def test_next_utc_day_becomes_eligible_in_window(cycle, monkeypatch):
    now, status, calls, _ = cycle
    run_state.write_status('complete', scan_performed=True)
    status.update(run_state.read_status())
    monkeypatch.setattr(supervisor.time, 'time', lambda: now + 86400)
    supervisor.main()  # Outside the publication window: still no new scan.
    assert calls == []
    monkeypatch.setattr(supervisor, '_hours', lambda: set(range(24)))
    supervisor.main()
    assert len(calls) == 1 and calls[0][-1] == 'morning'


def test_completion_day_is_utc_even_when_local_date_is_different(monkeypatch):
    stamp = datetime(2026, 9, 18, 23, 30, tzinfo=timezone.utc).timestamp()
    monkeypatch.setattr(run_state.time, 'time', lambda: stamp)
    run_state.write_status('complete', scan_performed=True)
    assert run_state.automatic_scan_completed_today(now=stamp + 1799)
    assert not run_state.automatic_scan_completed_today(now=stamp + 1800)
    assert not run_state.automatic_scan_completed_today(now=stamp - 1)


@pytest.mark.parametrize('state', ['failed', 'partial', 'interrupted', 'service_unavailable', 'auth_failed'])
def test_unfinished_scan_does_not_satisfy_day_and_is_retried(cycle, state):
    _, status, calls, _ = cycle
    run_state.write_status(state, scan_performed=True)
    status.update(run_state.read_status())
    assert not run_state.automatic_scan_completed_today()
    supervisor.main()
    assert any(command[-1] == 'morning' for command in calls)


def test_worker_gate_prevents_duplicate_even_without_supervisor(monkeypatch):
    run_state.write_status('complete', scan_performed=True)
    before = run_state.STATUS_FILE.read_text()
    worker = Mock(side_effect=AssertionError('No provider or maintenance work allowed'))
    monkeypatch.setattr(automated_morning, '_run_with_lock', worker)
    result = automated_morning.run()
    assert result['skipped'] and not result['scan_performed']
    worker.assert_not_called()
    assert run_state.STATUS_FILE.read_text() == before


def test_explicit_manual_failure_can_resume_despite_earlier_success(cycle, monkeypatch):
    _, status, calls, _ = cycle
    run_state.write_status('complete', scan_performed=True)
    worker = Mock(return_value={'ok': False, 'state': 'wizz_service_unavailable'})
    monkeypatch.setattr(automated_morning, '_run_with_lock', worker)
    assert not automated_morning.run(force=True)['ok']
    worker.assert_called_once_with(force=True)
    run_state.write_status('service_unavailable')
    status.update(run_state.read_status())
    assert not run_state.automatic_scan_completed_today()
    supervisor.main()
    assert calls[-1][-1] == 'morning'
    run_state.write_status('complete', scan_performed=True)
    assert run_state.automatic_scan_completed_today()


def test_pre_upgrade_success_is_adopted_and_survives_status_change(cycle):
    now, _, _, _ = cycle
    run_state.STATE_DIR.mkdir(parents=True, exist_ok=True)
    run_state.STATUS_FILE.write_text(json.dumps({'state': 'complete', 'scan_performed': True,
                                                'updated_at': now - 840}))
    assert run_state.automatic_scan_completed_today()
    run_state.write_status('rate_limited', resume_scan=False)
    assert run_state.automatic_scan_completed_today()
    receipt = run_state.STATE_DIR / 'scan-schedule.json'
    assert receipt.stat().st_mode & 0o777 == 0o600
    assert not list(run_state.STATE_DIR.glob('.scan-schedule-*.tmp'))


def test_skipped_run_never_moves_completion_into_next_day(cycle, monkeypatch):
    now, _, _, _ = cycle
    run_state.write_status('complete', scan_performed=True)
    monkeypatch.setattr(run_state.time, 'time', lambda: now + 86400)
    run_state.request_manual_scan()
    run_state.write_status('complete', scan_performed=False)
    assert not run_state.automatic_scan_completed_today()
    receipt = json.loads((run_state.STATE_DIR / 'scan-schedule.json').read_text())
    assert receipt['completed_at'] == now
    assert not receipt['manual_pending']
