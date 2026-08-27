import unittest
from pathlib import Path

from flask import Flask, render_template


class ResultsTemplateTests(unittest.TestCase):
    def test_direct_itinerary_with_none_connection_renders(self):
        root = Path(__file__).resolve().parents[1]
        app = Flask(__name__, template_folder=str(root / "templates"))

        @app.get("/")
        def index():
            return "ok"

        @app.get("/flights")
        def all_flights():
            return "ok"

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
        with app.test_request_context("/results"):
            html = render_template(
                "results.html",
                outbound=[direct],
                returns=[],
                origin="London",
                destination="Budapest",
                start_date="2026-08-28",
                return_start_date=None,
                days=4,
                min_transfer_minutes=150,
                live_requests=0,
                return_requested=False,
                result_source="morning-cache",
                cache_misses=0,
                cache_stats={"pdf": {"generated_at": "2026-08-27T07:00:04"}},
            )
        self.assertIn("Direct", html)
        self.assertIn("London", html)
        self.assertIn("Budapest", html)


if __name__ == "__main__":
    unittest.main()
