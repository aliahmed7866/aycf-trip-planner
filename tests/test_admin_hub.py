import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from werkzeug.security import generate_password_hash


MODULE_PATH = Path(__file__).resolve().parents[1] / "termux" / "admin_hub.py"
spec = importlib.util.spec_from_file_location("admin_hub", MODULE_PATH)
admin_hub = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(admin_hub)


class AdminHubTests(unittest.TestCase):
    def test_registry_expands_home_and_filters_invalid_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = Path(tmp) / "apps.json"
            registry.write_text(json.dumps({"apps": [
                {"id": "one", "name": "One", "working_dir": "~/one", "start": ["true"]},
                {"name": "missing id"},
                "bad",
            ]}), encoding="utf-8")
            with patch.object(admin_hub, "REGISTRY_PATH", registry):
                apps = admin_hub._load_registry()
            self.assertEqual([app["id"] for app in apps], ["one"])
            self.assertTrue(apps[0]["working_dir"].endswith("/one"))

    def test_stale_sunscape_registry_is_migrated_to_flask_service(self):
        stale = {
            "id": "sunscape",
            "name": "Sunscape",
            "working_dir": "~/Sunscape",
            "port": 3000,
            "health_url": "http://127.0.0.1:3000",
            "open_url": "http://127.0.0.1:3000",
            "start": ["npm", "run", "start"],
            "process_match": "next start",
        }
        with patch.object(admin_hub, "_sunscape_manifest", return_value={}):
            migrated = admin_hub._normalize_registry_app(stale)
        self.assertEqual(migrated["port"], 8081)
        self.assertEqual(migrated["health_url"], "http://127.0.0.1:8081/health")
        self.assertEqual(migrated["service"], "sunscape")
        self.assertIn("gunicorn", migrated["process_match"])
        self.assertTrue(migrated["working_dir"].endswith("/sunscape"))

    def test_installed_sunscape_manifest_overrides_stale_registry(self):
        manifest = {
            "working_dir": "/tmp/sunscape",
            "port": 8091,
            "health_url": "http://127.0.0.1:8091/health",
            "open_url": "http://127.0.0.1:8091",
            "service": "sunscape",
            "start": ["sv", "up", "sunscape"],
            "process_match": "sunscape/.venv/bin/gunicorn",
        }
        with patch.object(admin_hub, "_sunscape_manifest", return_value=manifest):
            migrated = admin_hub._normalize_registry_app({"id": "sunscape", "name": "Sunscape", "working_dir": "~/Sunscape", "port": 3000})
        self.assertEqual(migrated["port"], 8091)
        self.assertEqual(migrated["working_dir"], "/tmp/sunscape")

    def test_status_reports_missing_without_health_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "nope"
            with patch.object(admin_hub, "_pids_for", return_value=[]), patch.object(admin_hub, "_health", side_effect=AssertionError("health should not run")):
                state = admin_hub.app_status({
                    "id": "missing",
                    "name": "Missing",
                    "working_dir": str(missing),
                    "process_match": "nothing",
                    "health_url": "http://127.0.0.1:1",
                })
            self.assertEqual(state["state"], "missing")
            self.assertFalse(state["available"])

    def test_login_reuses_bootstrap_aycf_password(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(admin_hub, "PASSWORD_STORE", Path(tmp) / "missing.json"), patch.dict(os.environ, {"AYCF_APP_PASSWORD": "correct-horse"}, clear=False):
            app = admin_hub.create_app()
            app.config.update(TESTING=True)
            client = app.test_client()
            response = client.post("/login", data={"password": "correct-horse"}, follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            with client.session_transaction() as sess:
                self.assertTrue(sess["admin_authenticated"])
                self.assertTrue(sess.get("csrf_token"))

    def test_login_uses_in_app_password_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Path(tmp) / "app-password.json"
            store.write_text(json.dumps({"version": 1, "password_hash": generate_password_hash("new-secure-password", method="scrypt")}), encoding="utf-8")
            with patch.object(admin_hub, "PASSWORD_STORE", store), patch.dict(os.environ, {"AYCF_APP_PASSWORD": "old-bootstrap-password"}, clear=False):
                self.assertTrue(admin_hub._verify_admin_password("new-secure-password"))
                self.assertFalse(admin_hub._verify_admin_password("old-bootstrap-password"))

    def test_loopback_request_is_passwordless_when_bound_to_loopback(self):
        with patch.dict(os.environ, {"AYCF_ADMIN_BIND_HOST": "127.0.0.1", "AYCF_APP_PASSWORD": "configured"}, clear=False):
            app = admin_hub.create_app()
            app.config.update(TESTING=True)
            response = app.test_client().get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Manage every local Flask service", response.data)

    def test_remote_request_still_requires_password(self):
        with patch.dict(os.environ, {"AYCF_ADMIN_BIND_HOST": "127.0.0.1", "AYCF_APP_PASSWORD": "configured"}, clear=False):
            app = admin_hub.create_app()
            app.config.update(TESTING=True)
            response = app.test_client().get("/", environ_base={"REMOTE_ADDR": "192.0.2.10"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Use your current AYCF app password", response.data)

    def test_non_loopback_binding_disables_passwordless_access(self):
        with patch.dict(os.environ, {"AYCF_ADMIN_BIND_HOST": "0.0.0.0", "AYCF_APP_PASSWORD": "configured"}, clear=False):
            app = admin_hub.create_app()
            app.config.update(TESTING=True)
            response = app.test_client().get("/")
        self.assertIn(b"Use your current AYCF app password", response.data)

    def test_non_loopback_hub_fails_closed_without_password(self):
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(admin_hub, "PASSWORD_STORE", Path(tmp) / "missing.json"), \
             patch.dict(os.environ, {"AYCF_ADMIN_BIND_HOST": "0.0.0.0", "AYCF_APP_PASSWORD": ""}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "password is required"):
                admin_hub.create_app()

    def test_service_control_uses_sv(self):
        target = {"service": "sunscape"}
        with patch.object(admin_hub.subprocess, "run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = ""
            run.return_value.stderr = ""
            self.assertTrue(admin_hub._service_action(target, "restart"))
            run.assert_called_once_with(["sv", "restart", "sunscape"], capture_output=True, text=True, timeout=12, check=False)


if __name__ == "__main__":
    unittest.main()
