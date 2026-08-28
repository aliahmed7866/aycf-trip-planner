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
                max_stops=2,
                min_transfer_minutes=120,
                max_layover_minutes=480,
                max_journey_minutes=720,
                live_requests=0,
                return_requested=False,
                result_source="morning-cache",
                cache_misses=0,
                cache_stats={"pdf": {"generated_at": "2026-08-27T07:00:04"}},
                result_hubs=["Budapest", "Rome"],
            )

    def test_direct_itinerary_renders(self):
        direct = {
            "path": ["London", "Budapest"],
            "date": "2026-08-28",
            "origin": "London",
            "destination": "Budapest",
            "hubs": [],
            "stop_count": 0,
            "is_direct": True,
            "risky_connection": False,
            "connections": [],
            "connection_minutes": None,
            "total_minutes": 200,
            "departure_time": "08:00",
            "arrival_time": "11:20",
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
            "origin": "London",
            "destination": "Yerevan",
            "hubs": ["Budapest"],
            "stop_count": 1,
            "is_direct": False,
            "risky_connection": True,
            "connections": [{"hub": "Budapest", "minutes": 120, "risky": True}],
            "connection_minutes": 120,
            "total_minutes": 540,
            "departure_time": "06:00",
            "arrival_time": "15:00",
            "legs": [
                {"origin": "London", "destination": "Budapest", "flight_code": "W60001", "departure": "2026-08-28T06:00:00", "arrival": "2026-08-28T09:00:00", "duration": "3h"},
                {"origin": "Budapest", "destination": "Yerevan", "flight_code": "W60002", "departure": "2026-08-28T11:00:00", "arrival": "2026-08-28T15:00:00", "duration": "4h"},
            ],
        }
        html = self._render([connection])
        self.assertIn("Risky connection", html)
        self.assertIn("Below the recommended 2h30 buffer", html)
        self.assertIn('data-safety="risky"', html)

    def test_two_and_half_hour_connection_is_recommended(self):
        connection = {
            "path": ["London", "Budapest", "Yerevan"],
            "date": "2026-08-28",
            "origin": "London",
            "destination": "Yerevan",
            "hubs": ["Budapest"],
            "stop_count": 1,
            "is_direct": False,
            "risky_connection": False,
            "connections": [{"hub": "Budapest", "minutes": 150, "risky": False}],
            "connection_minutes": 150,
            "total_minutes": 570,
            "departure_time": "06:00",
            "arrival_time": "15:30",
            "legs": [
                {"origin": "London", "destination": "Budapest", "flight_code": "W60001", "departure": "2026-08-28T06:00:00", "arrival": "2026-08-28T09:00:00", "duration": "3h"},
                {"origin": "Budapest", "destination": "Yerevan", "flight_code": "W60002", "departure": "2026-08-28T11:30:00", "arrival": "2026-08-28T15:30:00", "duration": "4h"},
            ],
        }
        html = self._render([connection])
        self.assertIn("Recommended connections", html)
        self.assertIn('data-safety="recommended"', html)

    def test_two_stop_itinerary_shows_each_connection(self):
        connection = {
            "path": ["London", "Budapest", "Rome", "Athens"],
            "date": "2026-08-28",
            "origin": "London",
            "destination": "Athens",
            "hubs": ["Budapest", "Rome"],
            "stop_count": 2,
            "is_direct": False,
            "risky_connection": True,
            "connections": [
                {"hub": "Budapest", "minutes": 160, "risky": False},
                {"hub": "Rome", "minutes": 125, "risky": True},
            ],
            "connection_minutes": 125,
            "total_minutes": 720,
            "departure_time": "06:00",
            "arrival_time": "18:00",
            "legs": [
                {"origin": "London", "destination": "Budapest", "flight_code": "W1", "departure": "2026-08-28T06:00:00", "arrival": "2026-08-28T09:00:00", "duration": "3h"},
                {"origin": "Budapest", "destination": "Rome", "flight_code": "W2", "departure": "2026-08-28T11:40:00", "arrival": "2026-08-28T13:00:00", "duration": "1h20m"},
                {"origin": "Rome", "destination": "Athens", "flight_code": "W3", "departure": "2026-08-28T15:05:00", "arrival": "2026-08-28T18:00:00", "duration": "2h55m"},
            ],
        }
        html = self._render([connection])
        self.assertIn("2 stops", html)
        self.assertIn("Budapest", html)
        self.assertIn("Rome", html)
        self.assertIn("2h 5m", html)
        self.assertIn('data-route="2"', html)


if __name__ == "__main__":
    unittest.main()
