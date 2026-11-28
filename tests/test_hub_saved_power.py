"""Admin Hub controls, including stop and start across a real runsv restart."""
import fcntl
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import time

import pytest

from termux import admin_hub as hub


def test_locked_service_keeps_its_boot_setting(tmp_path, monkeypatch):
    (tmp_path / "run").touch()
    monkeypatch.setattr(hub, "_service_dir", lambda _: tmp_path)
    with (tmp_path / ".control.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with pytest.raises(RuntimeError, match="changing state"):
            hub._service_action({"id": "example", "service": "example"}, "stop")
    assert not (tmp_path / "down").exists()


@pytest.mark.skipif(not all(shutil.which(name) for name in ("sv", "runsv")), reason="requires runit")
def test_stop_survives_reboot_and_start_recovers_a_crash(tmp_path, monkeypatch):
    service = tmp_path / "var/service/example"
    service.mkdir(parents=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    run = service / "run"
    run.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable)
                   + f" -m http.server {port} --bind 127.0.0.1\n")
    run.chmod(0o700)
    item = dict(id="example", name="Example", service="example", working_dir=str(tmp_path),
                health_url=f"http://127.0.0.1:{port}/")
    monkeypatch.setenv("PREFIX", str(tmp_path))
    monkeypatch.setattr(hub, "STATE_DIR", tmp_path / "state")
    monkeypatch.setattr(hub, "_load_registry", lambda: [item])
    monkeypatch.setattr(hub, "_trusted_local_request", lambda: True)
    client = hub.create_app().test_client()
    with client.session_transaction() as session:
        session["csrf_token"] = "token"

    def wait_for(predicate):
        deadline = time.monotonic() + 8
        while not predicate():
            assert time.monotonic() < deadline
            time.sleep(.05)

    def supervise():
        return subprocess.Popen(["runsv", str(service)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def control(action):
        result = client.post(f"/apps/example/{action}", data={"csrf_token": "token"})
        assert result.status_code == 302

    supervisor = supervise()
    try:
        wait_for(lambda: hub.app_status(item)["state"] == "running")
        control("stop")
        assert hub.service_preference(item) == "Off"
        subprocess.run(["sv", "-w", "5", "shutdown", str(service)], check=True)
        supervisor.wait(timeout=5)
        supervisor = supervise()
        wait_for(lambda: hub.app_status(item)["state"] == "stopped")
        client.post("/restart-all", data={"csrf_token": "token"})
        control("restart")
        assert hub.service_preference(item) == "Off"
        assert not hub._app_health(item)[0]
        control("start")
        assert hub.service_preference(item) == "On"
        assert hub.app_status(item)["state"] == "running"
        pidfile = service / "supervise/pid"
        old_pid = pidfile.read_text().strip()
        subprocess.run(["sv", "kill", str(service)], check=True)
        wait_for(lambda: pidfile.read_text().strip() not in {"", old_pid} and hub._app_health(item)[0])
    finally:
        subprocess.run(["sv", "-w", "5", "shutdown", str(service)], check=False)
        supervisor.wait(timeout=8)
