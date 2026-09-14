from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

MODULE_PATH = Path(__file__).parents[1] / "termux" / "admin_hub.py"
SPEC = importlib.util.spec_from_file_location("admin_hub_refined", MODULE_PATH)
admin_hub = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(admin_hub)


def authed(client):
    with client.session_transaction() as session:
        session["admin_authenticated"] = True
        session["csrf_token"] = "token"


def test_home_is_launcher_and_manage_contains_controls(monkeypatch):
    item = {
        "id": "places", "name": "Places", "icon": "🌍", "description": "Travel journal",
        "manager": "command", "open_url": "http://127.0.0.1:8084", "port": 8084,
        "health_url": "http://127.0.0.1:8084/health",
    }
    monkeypatch.setattr(admin_hub, "_load_registry", lambda: [item])
    monkeypatch.setattr(admin_hub, "app_status", lambda x: {**x, "state": "running", "healthy": True, "health_text": "HTTP 200", "service_text": "running"})
    monkeypatch.setattr(admin_hub, "_system_status", lambda: {"storage": "10 GB free", "storage_pct": "50% used", "load": "0.10", "uptime": "2h"})
    app = admin_hub.create_app()
    app.config.update(TESTING=True, SECRET_KEY="test")
    with app.test_client() as client:
        authed(client)
        home = client.get("/")
        assert b"Everything, one tap away" in home.data
        assert b"Places" in home.data
        manage = client.get("/manage")
        assert b"Health &amp; controls" in manage.data
        assert b"Restart running apps" in manage.data
        assert b"HTTP 200" in manage.data


def test_command_managed_app_uses_declared_restart(monkeypatch, tmp_path):
    runner = tmp_path / ".local/bin/places"
    runner.parent.mkdir(parents=True)
    runner.write_text("#!/bin/sh\n")
    monkeypatch.setattr(admin_hub, "HOME", tmp_path)
    monkeypatch.setattr(admin_hub, "APP_ROOT", tmp_path)
    target = {
        "id": "places", "name": "Places", "manager": "command",
        "status_command": ["$HOME/.local/bin/places", "status"],
        "restart_command": ["$HOME/.local/bin/places", "restart"],
    }
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        if command[-1] == "status":
            return subprocess.CompletedProcess(command, 0, stdout="running pid=12", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="Places restarted", stderr="")

    monkeypatch.setattr(admin_hub.subprocess, "run", fake_run)
    ok, detail = admin_hub._run_control(target, "restart")
    assert ok is True
    assert detail == "Places restarted"
    assert calls[-1] == [str(runner), "restart"]


def test_registry_defaults_include_places():
    payload = (Path(__file__).parents[1] / "termux" / "apps.json.example").read_text()
    assert '"id": "places"' in payload
    assert 'install-places.sh' in payload
