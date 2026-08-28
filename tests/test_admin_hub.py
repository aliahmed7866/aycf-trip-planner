import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


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

    def test_login_reuses_aycf_password(self):
        with patch.dict(os.environ, {"AYCF_APP_PASSWORD": "correct-horse"}, clear=False):
            app = admin_hub.create_app()
            app.config.update(TESTING=True)
            client = app.test_client()
            response = client.post("/login", data={"password": "correct-horse"}, follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            with client.session_transaction() as sess:
                self.assertTrue(sess["admin_authenticated"])
                self.assertTrue(sess.get("csrf_token"))


if __name__ == "__main__":
    unittest.main()
