import importlib.util
import json
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "termux" / "admin_hub.py"
spec = importlib.util.spec_from_file_location("admin_hub", MODULE_PATH)
admin_hub = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(admin_hub)


def test_registry_expands_home_and_filters_invalid_entries(tmp_path, monkeypatch):
    registry = tmp_path / "apps.json"
    registry.write_text(json.dumps({"apps": [
        {"id": "one", "name": "One", "working_dir": "~/one", "start": ["true"]},
        {"name": "missing id"},
        "bad",
    ]}), encoding="utf-8")
    monkeypatch.setattr(admin_hub, "REGISTRY_PATH", registry)
    apps = admin_hub._load_registry()
    assert [app["id"] for app in apps] == ["one"]
    assert apps[0]["working_dir"].endswith("/one")


def test_status_reports_missing_without_health_probe(tmp_path, monkeypatch):
    monkeypatch.setattr(admin_hub, "_pids_for", lambda match: [])
    monkeypatch.setattr(admin_hub, "_health", lambda url: (_ for _ in ()).throw(AssertionError("health should not run")))
    state = admin_hub.app_status({
        "id": "missing",
        "name": "Missing",
        "working_dir": str(tmp_path / "nope"),
        "process_match": "nothing",
        "health_url": "http://127.0.0.1:1",
    })
    assert state["state"] == "missing"
    assert state["available"] is False


def test_login_reuses_aycf_password(monkeypatch):
    monkeypatch.setenv("AYCF_APP_PASSWORD", "correct-horse")
    app = admin_hub.create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    response = client.post("/login", data={"password": "correct-horse"}, follow_redirects=False)
    assert response.status_code == 302

    with client.session_transaction() as sess:
        assert sess["admin_authenticated"] is True
        assert sess.get("csrf_token")
