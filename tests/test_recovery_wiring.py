from pathlib import Path
import json
import unittest

ROOT = Path(__file__).resolve().parents[1]


class RecoveryWiringTests(unittest.TestCase):
    def test_termux_runs_watch_enabled_app(self):
        text = (ROOT / "termux" / "run-web.py").read_text(encoding="utf-8")
        self.assertIn("from watch_app import app", text)
        self.assertNotIn("from app import create_app", text)

    def test_watch_blueprint_exposes_expected_routes(self):
        text = (ROOT / "watch_blueprint.py").read_text(encoding="utf-8")
        for route in [
            '@bp.route("/watches", methods=["GET"])',
            '@bp.route("/watches/add", methods=["POST"])',
            '@bp.route("/watches/check", methods=["POST"])',
        ]:
            self.assertIn(route, text)

    def test_pre_flight_console_ui_is_restored(self):
        base = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")
        index = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
        self.assertIn("AYCF Trip Planner", base)
        self.assertNotIn("Flight Console", base)
        self.assertIn("Plan routes (phone-friendly)", index)
        self.assertNotIn("Build a bookable trip, faster", index)
        self.assertIn("data-picker-open=\"bases\"", index)
        self.assertIn("data-picker-open=\"hubs\"", index)
        self.assertIn("data-picker-open=\"targets\"", index)
        self.assertIn('id="cityPickerModal"', index)
        self.assertIn('id="picker-done"', index)
        self.assertIn("window.__PICKER_OPTIONS__", index)
        self.assertIn("bootstrap.bundle.min.js", base)
        self.assertIn("new bootstrap.Modal", base)

    def test_watch_page_uses_non_blocking_defaults(self):
        text = (ROOT / "watch_blueprint.py").read_text(encoding="utf-8")
        self.assertIn("defaults = planner.ui_defaults()", text)
        self.assertNotIn("planner.city_options(lookback_days=365)", text)

    def test_watch_service_has_no_pandas_runtime_dependency(self):
        text = (ROOT / "watch_service.py").read_text(encoding="utf-8")
        self.assertNotIn("import pandas", text)
        self.assertNotIn("pd.", text)
        self.assertIn("planner._load_runs()", text)

    def test_admin_catalog_contains_aycf_and_sunscape(self):
        payload = json.loads((ROOT / "termux" / "apps.json.example").read_text(encoding="utf-8"))
        apps = {item["id"]: item for item in payload["apps"]}
        self.assertEqual(apps["aycf"]["service"], "aycf")
        self.assertEqual(apps["aycf"]["port"], 8080)
        self.assertEqual(apps["sunscape"]["service"], "sunscape")
        self.assertEqual(apps["sunscape"]["port"], 8081)

    def test_auto_deploy_runner_invokes_watcher_through_bash(self):
        text = (ROOT / "termux" / "install-auto-deploy.sh").read_text(encoding="utf-8")
        self.assertIn('exec /data/data/com.termux/files/usr/bin/bash "$APP_DIR/termux/auto-deploy.sh"', text)


if __name__ == "__main__":
    unittest.main()
