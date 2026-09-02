import os
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

import app as aycf_app


def _request(remote_addr: str = "127.0.0.1"):
    flask_app = Flask(__name__)
    return flask_app.test_request_context("/", environ_base={"REMOTE_ADDR": remote_addr})


def test_aycf_trusts_direct_loopback_when_bound_to_loopback():
    with patch.dict(os.environ, {"AYCF_BIND_HOST": "127.0.0.1"}, clear=False), _request():
        assert aycf_app._trusted_local_request() is True


def test_aycf_does_not_trust_remote_client():
    with patch.dict(os.environ, {"AYCF_BIND_HOST": "127.0.0.1"}, clear=False), _request("192.0.2.10"):
        assert aycf_app._trusted_local_request() is False


def test_aycf_does_not_bypass_password_on_non_loopback_bind():
    with patch.dict(os.environ, {"AYCF_BIND_HOST": "0.0.0.0"}, clear=False), _request():
        assert aycf_app._trusted_local_request() is False


def test_local_password_can_be_forced_explicitly():
    with patch.dict(os.environ, {"AYCF_BIND_HOST": "127.0.0.1", "AYCF_REQUIRE_LOCAL_PASSWORD": "true"}, clear=False), _request():
        assert aycf_app._trusted_local_request() is False


def test_aycf_fails_closed_when_exposed_without_password():
    with patch.dict(os.environ, {"AYCF_BIND_HOST": "0.0.0.0", "AYCF_APP_PASSWORD": ""}, clear=False):
        try:
            aycf_app.create_app()
        except RuntimeError as exc:
            assert "AYCF_APP_PASSWORD" in str(exc)
        else:
            raise AssertionError("non-loopback AYCF must require a password")


def test_refresh_rejects_missing_csrf_before_updating(tmp_path):
    with patch.dict(os.environ, {
        "AYCF_BIND_HOST": "127.0.0.1",
        "AYCF_APP_PASSWORD": "configured",
        "AYCF_CACHE_DIR": str(tmp_path / "cache"),
        "AYCF_DB_PATH": str(tmp_path / "aycf.sqlite3"),
    }, clear=False), patch.object(
        aycf_app,
        "update_data_if_needed",
        return_value=SimpleNamespace(data_dir=str(tmp_path)),
    ) as update:
        flask_app = aycf_app.create_app()
        flask_app.config.update(TESTING=True)
        response = flask_app.test_client().post("/refresh", follow_redirects=False)
    assert response.status_code == 302
    assert update.call_count == 1


def test_official_cache_skips_legacy_download_on_startup(tmp_path):
    direct = tmp_path / "cache" / "direct-data"
    direct.mkdir(parents=True)
    (direct / "snapshot.csv").write_text("departure_from,departure_to,data_generated\nA,B,2026-09-03T07:00:00\n", encoding="utf-8")
    with patch.dict(os.environ, {
        "AYCF_BIND_HOST": "127.0.0.1",
        "AYCF_CACHE_DIR": str(tmp_path / "cache"),
        "AYCF_DB_PATH": str(tmp_path / "aycf.sqlite3"),
    }, clear=False), patch.object(aycf_app, "update_data_if_needed", side_effect=AssertionError("legacy download must not run")):
        flask_app = aycf_app.create_app()
    assert flask_app is not None


def test_manual_refresh_uses_official_pdf_source(tmp_path):
    direct = tmp_path / "cache" / "direct-data"
    direct.mkdir(parents=True)
    (direct / "snapshot.csv").write_text("departure_from,departure_to,data_generated\nA,B,2026-09-03T07:00:00\n", encoding="utf-8")
    with patch.dict(os.environ, {
        "AYCF_BIND_HOST": "127.0.0.1",
        "AYCF_CACHE_DIR": str(tmp_path / "cache"),
        "AYCF_DB_PATH": str(tmp_path / "aycf.sqlite3"),
        "AYCF_PDF_URL": "https://example.test/aycf.pdf",
    }, clear=False), patch.object(aycf_app, "update_data_if_needed", side_effect=AssertionError("legacy download must not run")), patch.object(aycf_app, "refresh_direct_snapshot") as refresh:
        flask_app = aycf_app.create_app()
        flask_app.config.update(TESTING=True)
        client = flask_app.test_client()
        with client.session_transaction() as session:
            session["csrf_token"] = "token"
        response = client.post("/refresh", data={"csrf_token": "token"}, follow_redirects=False)
    assert response.status_code == 302
    refresh.assert_called_once_with(str(tmp_path / "cache"), "https://example.test/aycf.pdf")
