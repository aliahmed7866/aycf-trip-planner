import unittest
from pathlib import Path

from flask import Flask, render_template


class ResultsTemplateTests(unittest.TestCase):
    def _app(self):
        root = Path(__file__).resolve().parents[1]
        app = Flask(__name__, template_folder=str(root / "templates"))

        @app.get("/")
        def index():
            return "ok"

        @app.get("/flights")
        def all_flights():
            return "ok"

        return app

    def _render(self, outbound):
        app = self._app()
        with app.test_request_context("/results"):
            return render_template(
                "results.html",
                outbound=outbound,
                returns=[],
                origin="London",
                destination="Budapest",
                start_date="2026-08-28",
                return_start_date=None,
                days=4,
                min_transfer_minutes=120,
                live_requests=0,
                return_requested=False,
                result_source="morning-cache",
                cache_misses=0,
                cache_stats={"pdf": {"generated_at": "2026-08-27T07:00:04"}},
                result_hubs=[],
            )

    def test_direct_itinerary_with_none_connection_renders(self):
        direct = {
            "path": ["London", "Budapest"],
            "date": "2026-08-28",
            "connection_minutes": None,
            "legs": [{
                "origin": "London",
                "destination": "Budapest",
                "flight_code": "W60001",
                "departure": "2026-08-28T08:00:00",
                "arrival": "2026-08-28T11:20:00",
                "duration": "3h 20m",
            }],
        }
        html = self._render([direct])
        self.assertIn("Direct", html)
        self.assertIn("London", html)
        self.assertIn("Budapest", html)

    def test_two_hour_connection_is_flagged_risky(self):
        connection = {
            "path": ["London", "Budapest", "Yerevan"],
            "date": "2026-08-28",
            "connection_minutes": 120,
            "legs": [
                {"origin": "London", "destination": "Budapest", "flight_code": "W60001", "departure": "2026-08-28T06:00:00", "arrival": "2026-08-28T09:00:00", "duration": "3h"},
                {"origin": "Budapest", "destination": "Yerevan", "flight_code": "W60002", "departure": "2026-08-28T11:00:00", "arrival": "2026-08-28T15:00:00", "duration": "4h"},
            ],
        }
        html = self._render([connection])
        self.assertIn("Risky connection", html)
        self.assertIn("below the recommended 2h30 buffer", html)
        self.assertIn('data-safety="risky"', html)

    def test_two_and_half_hour_connection_is_recommended(self):
        connection = {
            "path": ["London", "Budapest", "Yerevan"],
            "date": "2026-08-28",
            "connection_minutes": 150,
            "legs": [
                {"origin": "London", "destination": "Budapest", "flight_code": "W60001", "departure": "2026-08-28T06:00:00", "arrival": "2026-08-28T09:00:00", "duration": "3h"},
                {"origin": "Budapest", "destination": "Yerevan", "flight_code": "W60002", "departure": "2026-08-28T11:30:00", "arrival": "2026-08-28T15:30:00", "duration": "4h"},
            ],
        }
        html = self._render([connection])
        self.assertIn("Recommended connection", html)
        self.assertIn('data-safety="recommended"', html)


if __name__ == "__main__":
    unittest.main()
