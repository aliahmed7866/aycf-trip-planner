"""Exercise persisted scheduler state across failure, repair and later wakes."""

import json
from contextlib import contextmanager
from unittest.mock import Mock

import pytest

from termux import supervisor


@contextmanager
def available_lock():
    yield True


@pytest.fixture
def cycle(monkeypatch, tmp_path):
    now = 1788973455  # 17:04 UTC, outside the default scan window
    monkeypatch.setattr(supervisor, "STATE_DIR", tmp_path)
    monkeypatch.setattr(supervisor, "SUPERVISOR_FILE", tmp_path / "supervisor.json")
    monkeypatch.setattr(supervisor, "WIZZ_STATUS_FILE", tmp_path / "wizz.json")
    monkeypatch.setattr(supervisor.time, "time", lambda: now)
    monkeypatch.setattr(supervisor, "_hours", lambda: set())
    monkeypatch.setattr(supervisor, "single_scan_lock", available_lock)
    monkeypatch.delenv("AYCF_AUTH_REPAIR_COOLDOWN_SECONDS", raising=False)
    monkeypatch.delenv("AYCF_SCAN_RETRY_SECONDS", raising=False)
    status = {"state": "attention_required", "updated_at": now - 1200}
    monkeypatch.setattr(supervisor, "read_status", lambda: status.copy())
    monkeypatch.setattr(supervisor, "write_status", lambda state, *a, **k: status.update(state=state, updated_at=now))
    health = Mock(return_value=True)
    monkeypatch.setattr(supervisor, "_saved_session_health", health)
    supervisor._save({"health_ok": True, "last_health_at": now - 1800,
                      "last_health_success_at": now - 1800,
                      "last_scan_attempt_at": now - 1200})
    calls = []

    def run(command, timeout):
        calls.append(command)
        if command[0] != "bash":
            status.update(state="complete", updated_at=int(supervisor.time.time()), scan_performed=True,
                          message="AYCF scan completed successfully.")
        return 0

    monkeypatch.setattr(supervisor, "_run", run)
    return now, status, calls, health


def saved():
    return json.loads(supervisor.SUPERVISOR_FILE.read_text())


def test_auth_failure_repairs_and_resumes_outside_window(cycle):
    _, _, calls, health = cycle
    supervisor.main()
    assert [c[0] == "bash" for c in calls] == [True, False]
    health.assert_not_called()  # recent but invalidated cached health
    assert saved()["scan_pending"] is False
    assert saved()["last_scan_failure"]["state"] == "attention_required"
    assert saved()["last_scan_outcome"]["scan_performed"] is True


def test_manual_repair_is_adopted_without_waiting_for_health_or_repair_cooldown(cycle):
    now, _, calls, health = cycle
    supervisor.WIZZ_STATUS_FILE.write_text(json.dumps({"ok": True, "updated_at": now - 10}))
    supervisor._save({**saved(), "health_ok": False, "last_repair_attempt_at": now - 30})
    supervisor.main()
    assert len(calls) == 1 and calls[0][-1] == "morning"
    health.assert_not_called()
    assert saved()["health_ok"] is True
    assert saved()["scan_pending"] is False


def test_failed_repair_keeps_pending_then_next_wake_recovers(cycle, monkeypatch):
    now, status, calls, _ = cycle
    successful_run = supervisor._run
    monkeypatch.setattr(supervisor, "_run", lambda *a, **k: 124)
    supervisor.main()
    assert saved()["scan_pending"] is True
    assert saved()["last_repair_rc"] == 124
    assert calls == []
    monkeypatch.setattr(supervisor.time, "time", lambda: now + 901)
    monkeypatch.setattr(supervisor, "_run", successful_run)
    supervisor.main()
    assert len(calls) == 2  # no six-hour health wait or next-morning wait
    assert saved()["scan_pending"] is False


def test_manual_completion_clears_pending_without_another_scan(cycle):
    now, status, calls, _ = cycle
    supervisor._save({**saved(), "scan_pending": True, "pending_since": now - 1200})
    status.update(state="complete", updated_at=now - 10)
    supervisor.main()
    assert saved()["scan_pending"] is False
    assert calls == []


def test_zero_exit_without_fresh_completion_keeps_pending(cycle, monkeypatch):
    now, status, _, _ = cycle
    status.update(state="complete", updated_at=now - 2000)
    supervisor._save({**saved(), "scan_pending": True, "pending_since": now - 1200})
    monkeypatch.setattr(supervisor, "_run", lambda *a, **k: 0)
    supervisor.main()
    assert saved()["scan_pending"] is True
    assert saved()["state"] == "scan_retry_pending"


def test_failed_scan_does_not_get_lost_when_window_closes(cycle, monkeypatch):
    now, status, _, _ = cycle
    status.update(state="complete", updated_at=now - 2000)
    monkeypatch.setattr(supervisor, "_hours", lambda: set(range(24)))

    def fail(*a, **k):
        status.update(state="attention_required", updated_at=now)
        return 1

    monkeypatch.setattr(supervisor, "_run", fail)
    supervisor.main()
    assert saved()["scan_pending"] is True
    assert saved()["health_ok"] is False
    assert saved()["last_scan_rc"] == 1


def test_service_failure_retries_without_auth_repair(cycle):
    _, status, calls, _ = cycle
    status["state"] = "service_unavailable"
    supervisor.main()
    assert len(calls) == 1 and calls[0][-1] == "morning"


def test_interrupted_scan_becomes_pending(cycle):
    _, status, calls, _ = cycle
    status["state"] = "running"
    supervisor.main()
    assert len(calls) == 1 and calls[0][-1] == "morning"
    assert saved()["last_scan_failure"]["state"] == "interrupted"


def test_retry_cooldown_prevents_tight_loop(cycle):
    now, _, calls, _ = cycle
    supervisor._save({**saved(), "last_repair_attempt_at": now - 10})
    supervisor.main()
    assert calls == []
    assert saved()["scan_pending"] is True
