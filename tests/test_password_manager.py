import os
from pathlib import Path

from password_manager import PasswordStore


def test_password_store_uses_bootstrap_then_hash(tmp_path, monkeypatch):
    monkeypatch.setenv("AYCF_APP_PASSWORD", "bootstrap-secret")
    store = PasswordStore(Path(tmp_path) / "app-password.json")

    assert store.configured()
    assert not store.has_override()
    assert store.verify("bootstrap-secret")
    assert not store.verify("wrong")

    store.save("replacement-secret")

    assert store.has_override()
    assert store.verify("replacement-secret")
    assert not store.verify("bootstrap-secret")
    raw = store.path.read_text(encoding="utf-8")
    assert "replacement-secret" not in raw
    assert "bootstrap-secret" not in raw


def test_password_store_override_survives_env_change(tmp_path, monkeypatch):
    path = Path(tmp_path) / "app-password.json"
    monkeypatch.setenv("AYCF_APP_PASSWORD", "first-bootstrap")
    PasswordStore(path).save("persistent-secret")

    monkeypatch.setenv("AYCF_APP_PASSWORD", "different-bootstrap")
    reloaded = PasswordStore(path)

    assert reloaded.verify("persistent-secret")
    assert not reloaded.verify("different-bootstrap")
