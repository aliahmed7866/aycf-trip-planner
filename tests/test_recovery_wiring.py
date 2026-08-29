from pathlib import Path
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

    def test_planner_links_to_registered_watch_route(self):
        text = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
        self.assertIn('href="/watches"', text)
        self.assertNotIn("Old towns", text)
        self.assertNotIn("Mountain edges", text)
        self.assertNotIn("Unexpected routes", text)

    def test_watch_page_uses_non_blocking_defaults(self):
        text = (ROOT / "watch_blueprint.py").read_text(encoding="utf-8")
        self.assertIn("defaults = planner.ui_defaults()", text)
        self.assertNotIn("planner.city_options(lookback_days=365)", text)


if __name__ == "__main__":
    unittest.main()
