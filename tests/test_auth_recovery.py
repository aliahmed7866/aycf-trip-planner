import subprocess
from unittest.mock import Mock

import pytest

from scanner import WizzSessionExpired
from termux import auth_recovery, automated_morning, runtime, supervisor


@pytest.mark.parametrize("value,expected", [(None, 300), ("invalid", 300), ("1", 30), ("600", 600), ("9999", 900)])
def test_shared_repair_budget(monkeypatch, value, expected):
    monkeypatch.delenv("AYCF_WIZZ_REFRESH_TIMEOUT", raising=False)
    if value is not None:
        monkeypatch.setenv("AYCF_WIZZ_REFRESH_TIMEOUT", value)
    assert auth_recovery.refresh_timeout() == expected


@pytest.mark.parametrize("rc", [0, 20])
def test_manual_repair_only_wakes_supervisor_after_success(monkeypatch, rc):
    run = Mock(return_value=subprocess.CompletedProcess([], rc))
    supervise = Mock()
    monkeypatch.setattr(runtime.subprocess, "run", run)
    monkeypatch.setattr(supervisor, "main", supervise)
    with pytest.raises(SystemExit) as exc:
        runtime._repair()
    assert exc.value.code == rc
    assert supervise.call_count == (1 if rc == 0 else 0)
    assert run.call_args.kwargs["timeout"] == auth_recovery.refresh_timeout()


def test_manual_repair_timeout_does_not_launch_scan(monkeypatch):
    monkeypatch.setattr(runtime.subprocess, "run", Mock(side_effect=subprocess.TimeoutExpired("repair", 300)))
    supervise = Mock()
    monkeypatch.setattr(supervisor, "main", supervise)
    with pytest.raises(SystemExit) as exc:
        runtime._repair()
    assert exc.value.code == 124
    supervise.assert_not_called()


def test_missing_session_renews_and_resumes(monkeypatch):
    scan = Mock(side_effect=[WizzSessionExpired("No saved Wizz session"), {"ok": True}])
    refresh = Mock(return_value=True)
    monkeypatch.setattr(automated_morning.tiered_morning, "run", scan)
    monkeypatch.setattr(automated_morning, "_refresh", refresh)
    assert automated_morning._run_once(False)["ok"] is True
    assert scan.call_count == 2
    refresh.assert_called_once()
