from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "termux" / "admin_hub.py"
SPEC = importlib.util.spec_from_file_location("admin_hub_under_test", MODULE_PATH)
admin_hub = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(admin_hub)


def test_install_command_is_expanded_and_restricted_to_home(monkeypatch, tmp_path):
    monkeypatch.setattr(admin_hub, "HOME", tmp_path)
    monkeypatch.setattr(admin_hub, "APP_ROOT", tmp_path / "custom-aycf")
    command = admin_hub._install_command(
        {"install_command": ["bash", "$APP_ROOT/termux/install-expense-manager.sh"]}
    )
    assert command == ["bash", str(tmp_path / "custom-aycf/termux/install-expense-manager.sh")]
    assert admin_hub._install_command({"install_command": ["bash", "/tmp/not-allowed.sh"]}) == []
    assert admin_hub._install_command({"install_command": ["sh", "$HOME/setup.sh"]}) == []


def test_starting_a_missing_service_runs_its_setup_command(monkeypatch, tmp_path):
    monkeypatch.setattr(admin_hub, "HOME", tmp_path)
    monkeypatch.setattr(admin_hub, "APP_ROOT", tmp_path)
    target = {
        "id": "expenses", "name": "Pocketwise", "service": "expense-manager",
        "install_command": ["bash", "$HOME/setup-expenses.sh"],
    }
    monkeypatch.setattr(admin_hub, "_load_registry", lambda: [target])
    monkeypatch.setattr(admin_hub, "_service_status", lambda _service: ("missing", "not installed"))
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="installed", stderr="")

    monkeypatch.setattr(admin_hub.subprocess, "run", fake_run)
    app = admin_hub.create_app()
    app.config.update(TESTING=True, SECRET_KEY="test")
    with app.test_client() as client:
        with client.session_transaction() as session:
            session["admin_authenticated"] = True
            session["csrf_token"] = "token"
        response = client.post("/apps/expenses/up", data={"csrf_token": "token"})
    assert response.status_code == 302
    assert calls[0][0] == ["bash", str(tmp_path / "setup-expenses.sh")]
    assert calls[0][1]["timeout"] == 300
