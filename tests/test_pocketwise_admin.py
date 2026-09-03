from __future__ import annotations

import json
import subprocess

from termux import admin_hub


def test_registry_adds_new_defaults_without_overwriting_local_overrides(monkeypatch, tmp_path):
    app_root = tmp_path / "aycf"
    config = tmp_path / "config"
    (app_root / "termux").mkdir(parents=True)
    config.mkdir()
    (app_root / "termux" / "apps.json.example").write_text(json.dumps({
        "apps": [
            {"id": "aycf", "name": "AYCF", "description": "default"},
            {"id": "expenses", "name": "Pocketwise", "install_command": ["bash", "$APP_ROOT/termux/install-expense-manager.sh"]},
        ]
    }))
    (config / "apps.json").write_text(json.dumps({
        "apps": [{"id": "aycf", "name": "My AYCF", "description": "custom"}]
    }))
    monkeypatch.setattr(admin_hub, "APP_ROOT", app_root)
    monkeypatch.setattr(admin_hub, "CONFIG_DIR", config)
    monkeypatch.setattr(admin_hub, "REGISTRY_PATH", config / "apps.json")
    monkeypatch.setattr(admin_hub, "HOME", tmp_path)
    apps = admin_hub._load_registry()
    assert [row["id"] for row in apps] == ["aycf", "expenses"]
    assert apps[0]["name"] == "My AYCF"
    assert apps[0]["description"] == "custom"
    assert apps[1]["install_ready"] is True


def test_start_missing_service_runs_trusted_installer(monkeypatch, tmp_path):
    script = tmp_path / "aycf" / "termux" / "install-expense-manager.sh"
    script.parent.mkdir(parents=True)
    script.write_text("#!/bin/sh\n")
    app = {
        "id": "expenses", "name": "Pocketwise",
        "working_dir": str(tmp_path / "Expense_manager"),
        "service": "expense-manager", "process_match": "Expense_manager/gunicorn",
        "install_command": ["bash", "$APP_ROOT/termux/install-expense-manager.sh"],
    }
    monkeypatch.setattr(admin_hub, "HOME", tmp_path)
    monkeypatch.setattr(admin_hub, "APP_ROOT", tmp_path / "aycf")
    monkeypatch.setattr(admin_hub, "_pids_for", lambda _match: [])
    availability = iter([False, True])
    monkeypatch.setattr(admin_hub, "_service_available", lambda _app: next(availability))
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="installed", stderr="")

    monkeypatch.setattr(admin_hub.subprocess, "run", fake_run)
    assert admin_hub.start_app(app) is None
    assert calls[0][0] == ["bash", str(script)]
    assert calls[0][1]["timeout"] == 300
    assert calls[1][0] == ["sv", "up", "expense-manager"]

def test_canonical_pocketwise_process_match_follows_waitress_entrypoint():
    registry = json.loads(
        (admin_hub.APP_ROOT / "termux" / "apps.json.example").read_text(encoding="utf-8")
    )
    pocketwise = next(app for app in registry["apps"] if app["id"] == "expenses")
    assert pocketwise["process_match"] == "Expense_manager/.venv/bin/python termux/run-web.py"
