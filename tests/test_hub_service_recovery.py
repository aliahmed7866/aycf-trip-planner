"""Regressions for missing Termux supervision and contradictory app status."""
import io
import json
import subprocess
from unittest.mock import Mock

import pytest
from termux import admin_hub as hub


@pytest.fixture
def service(monkeypatch, tmp_path):
    directory = tmp_path / "mediahub"
    directory.mkdir()
    (directory / "run").write_text("#!/bin/sh\nexec sleep 60\n")
    monkeypatch.setattr(hub, "_service_dir", lambda _: directory)
    monkeypatch.setattr(hub, "_service_available", lambda _: True)
    monkeypatch.setattr(hub, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(hub, "_pids_for", lambda _: [])
    monkeypatch.setattr(hub, "_app_health", lambda _: (False, "Connection refused"))
    return {"id": "mediahub", "service": "mediahub", "working_dir": str(directory),
            "health_url": "http://127.0.0.1:8084/health"}


def result(code=0, out="", err=""):
    return subprocess.CompletedProcess([], code, out, err)


@pytest.mark.parametrize("action,verb", [("start", "up"), ("restart", "restart"), ("stop", "down")])
def test_control_recovers_missing_supervisor_and_retries(service, monkeypatch, action, verb):
    run = Mock(side_effect=[result(1, err="runsv not running"), result()])
    recovery = Mock()
    monkeypatch.setattr(hub.subprocess, "run", run)
    monkeypatch.setattr(hub, "_recover_supervisor", recovery)
    assert hub._service_action(service, action)
    recovery.assert_called_once_with("mediahub")
    assert [call.args[0][1] for call in run.call_args_list] == [verb, verb]


def test_recovery_failure_never_launches_app_directly(service, monkeypatch):
    monkeypatch.setattr(hub.subprocess, "run", Mock(return_value=result(1, err="runsv not running")))
    monkeypatch.setattr(hub, "_recover_supervisor", Mock(side_effect=RuntimeError("recovery failed")))
    launch = Mock(side_effect=AssertionError("duplicate launch"))
    monkeypatch.setattr(hub, "_start_command", launch)
    with pytest.raises(RuntimeError, match="recovery failed"):
        hub.start_app(service)
    launch.assert_not_called()


@pytest.mark.parametrize("action", ["start", "restart"])
def test_live_unmanaged_app_is_not_duplicated(service, monkeypatch, action):
    monkeypatch.setattr(hub.subprocess, "run", Mock(return_value=result(1, err="runsv not running")))
    monkeypatch.setattr(hub, "_app_health", lambda _: (True, "HTTP 200"))
    recover = Mock()
    monkeypatch.setattr(hub, "_recover_supervisor", recover)
    with pytest.raises(RuntimeError, match="outside its supervisor"):
        hub._service_action(service, action)
    recover.assert_not_called()


def test_unhealthy_orphan_process_is_not_duplicated(service, monkeypatch):
    monkeypatch.setattr(hub.subprocess, "run", Mock(return_value=result(1, err="runsv not running")))
    monkeypatch.setattr(hub, "_pids_for", lambda _: [123])
    with pytest.raises(RuntimeError, match="outside its supervisor"):
        hub._service_action(service, "start")


def test_stop_reports_endpoint_still_running_after_recovery(service, monkeypatch):
    monkeypatch.setattr(hub.subprocess, "run", Mock(side_effect=[result(1, err="runsv not running"), result()]))
    monkeypatch.setattr(hub, "_recover_supervisor", Mock())
    monkeypatch.setattr(hub, "_app_health", lambda _: (True, "HTTP 200"))
    with pytest.raises(RuntimeError, match="endpoint still responds"):
        hub._service_action(service, "stop")


@pytest.mark.parametrize("preexisting_down", [False, True])
def test_recovery_starts_only_supervisor_and_preserves_down_file(service, monkeypatch, preexisting_down):
    directory = hub._service_dir("mediahub")
    down = directory / "down"
    if preexisting_down:
        down.write_text("keep")
    monkeypatch.setattr(hub.shutil, "which", lambda _: "/usr/bin/runsv")
    def spawn(command, **kwargs):
        assert down.exists()  # An app must never start as a side effect of recovery.
        assert command == ["/usr/bin/runsv", str(directory)]
        assert kwargs["start_new_session"]
        assert kwargs["stdin"] == subprocess.DEVNULL
        assert kwargs["stdout"].name.endswith("supervisor-recovery.log")
        return Mock()
    monkeypatch.setattr(hub.subprocess, "Popen", spawn)
    monkeypatch.setattr(hub.subprocess, "run", Mock(return_value=result(out="down: mediahub: 0s")))
    hub._recover_supervisor("mediahub")
    assert down.exists() == preexisting_down
    if preexisting_down:
        assert down.read_text() == "keep"


def test_recovery_timeout_leaves_app_held_down(service, monkeypatch):
    monkeypatch.setattr(hub.shutil, "which", lambda _: "/usr/bin/runsv")
    monkeypatch.setattr(hub.subprocess, "Popen", Mock())
    monkeypatch.setattr(hub.time, "monotonic", Mock(side_effect=[0, 6]))
    with pytest.raises(RuntimeError, match="held down"):
        hub._recover_supervisor("mediahub")
    assert (hub._service_dir("mediahub") / "down").exists()


@pytest.mark.parametrize("runtime", ["unavailable", "stopped", "running"])
@pytest.mark.parametrize("healthy", [False, True])
def test_health_and_runtime_are_independent(service, monkeypatch, runtime, healthy):
    response = result(1, err="runsv not running") if runtime == "unavailable" else result(out=("run:" if runtime == "running" else "down:") + " mediahub")
    monkeypatch.setattr(hub.subprocess, "run", Mock(return_value=response))
    monkeypatch.setattr(hub, "_app_health", lambda _: (healthy, "HTTP 200" if healthy else "Connection refused"))
    recover = Mock(side_effect=AssertionError("Status must not mutate supervision"))
    monkeypatch.setattr(hub, "_recover_supervisor", recover)
    status = hub.app_status(service)
    expected = "running" if healthy else ("starting" if runtime == "running" else runtime)
    assert status["state"] == expected
    assert status["runtime_warning"] == (runtime == "unavailable" or (healthy and runtime != "running"))


@pytest.mark.parametrize("payload,expected", [
    ({"service": "mediahub", "ok": True}, True),
    ({"service": "sunscape", "status": "ok"}, False),
    ({"ok": True}, False),
    ({"service": "mediahub", "ok": False}, False),
    ([], False),
    ("not-json", False),
])
def test_wrong_app_on_port_cannot_pass_health(monkeypatch, payload, expected):
    raw = payload if isinstance(payload, str) else json.dumps(payload)
    response = io.BytesIO(raw.encode())
    response.status = 200
    monkeypatch.setattr(hub.urllib.request, "urlopen", lambda *a, **k: response)
    assert hub._health("http://127.0.0.1:8084/health", "mediahub")[0] is expected


def test_legacy_health_without_runtime_is_uncertain(service, monkeypatch):
    service["id"] = "places"
    monkeypatch.setattr(hub.subprocess, "run", Mock(return_value=result(out="down: places")))
    monkeypatch.setattr(hub, "_app_health", lambda _: (True, "HTTP 200"))
    assert hub.app_status(service)["state"] == "unavailable"


def test_runtime_mismatch_is_visible_in_manage(service, monkeypatch):
    monkeypatch.setattr(hub.subprocess, "run", Mock(return_value=result(1, err="runsv not running")))
    monkeypatch.setattr(hub, "_app_health", lambda _: (True, "HTTP 200"))
    monkeypatch.setattr(hub, "_load_registry", lambda: [dict(service, name="Media Hub")])
    monkeypatch.setattr(hub, "_trusted_local_request", lambda: True)
    data = hub.create_app().test_client().get("/workspace-status?section=manage").get_json()
    assert data["totals"]["running"] == 1
    assert data["totals"]["attention"] == 1
    assert "Runtime needs attention" in data["cards"]


def test_update_checks_app_identity_before_claiming_success(service, monkeypatch, tmp_path):
    from termux import hub_update
    root = hub._service_dir("mediahub")
    (root / ".git").mkdir()
    service.update(update_branch="master", update_command=["true"])
    monkeypatch.setattr(hub_update.subprocess, "check_output",
                        lambda cmd, **kwargs: "" if "status" in cmd else "master")
    monkeypatch.setattr(hub_update.subprocess, "run", Mock(return_value=result()))
    monkeypatch.setattr(hub, "_app_health", lambda _: (False, "Health endpoint belongs to another app"))
    with (tmp_path / "update.log").open("w") as log:
        with pytest.raises(RuntimeError, match="belongs to another app"):
            hub_update.update(service, hub, log)
