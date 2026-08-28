import unittest
from datetime import date, datetime

from itinerary_search import cached_scan_itineraries
from scanner import Flight


class FakeGraph:
    def edges_for_day(self, day):
        return {
            ("London", "Budapest"),
            ("Budapest", "Rome"),
            ("Rome", "Athens"),
            ("London", "Athens"),
        }


class FakeDB:
    def __init__(self, second_departure_minute=30):
        self.second_departure_minute = second_departure_minute

    def latest_completed_pdf_run(self):
        return {"run_id": "run"}

    def get_flights(self, origin, destination, travel_day, pdf_run_id=None):
        if travel_day != date(2026, 8, 28):
            return []
        flights = {
            ("London", "Budapest"): [Flight("London", "Budapest", "W1", datetime(2026, 8, 28, 6), datetime(2026, 8, 28, 9), "06:00", "09:00")],
            ("Budapest", "Rome"): [Flight("Budapest", "Rome", "W2", datetime(2026, 8, 28, 11, self.second_departure_minute), datetime(2026, 8, 28, 13), "", "")],
            ("Rome", "Athens"): [Flight("Rome", "Athens", "W3", datetime(2026, 8, 28, 15, 30), datetime(2026, 8, 28, 18), "", "")],
            ("London", "Athens"): [],
        }
        return flights.get((origin, destination), None)


class ItinerarySearchTests(unittest.TestCase):
    def test_two_stop_itinerary_is_built_from_cache(self):
        rows, misses = cached_scan_itineraries(
            FakeGraph(), FakeDB(), "London", "Athens", date(2026, 8, 28),
            days=1, max_stops=2, min_transfer_minutes=120, pdf_run_id="run",
        )
        two_stop = [r for r in rows if len(r["legs"]) == 3]
        self.assertEqual(misses, 0)
        self.assertEqual(len(two_stop), 1)
        self.assertEqual(two_stop[0]["path"], ["London", "Budapest", "Rome", "Athens"])
        self.assertEqual(two_stop[0]["connection_minutes_list"], [150, 150])

    def test_two_hours_is_allowed(self):
        rows, _ = cached_scan_itineraries(
            FakeGraph(), FakeDB(second_departure_minute=0), "London", "Rome", date(2026, 8, 28),
            days=1, max_stops=1, min_transfer_minutes=120, pdf_run_id="run",
        )
        self.assertTrue(any(r["connection_minutes"] == 120 for r in rows))

    def test_under_two_hours_is_rejected_even_if_requested_lower(self):
        # 10:59 departure after a 09:00 arrival = 119 minutes. The search hard floor is two hours.
        class UnderTwoDB(FakeDB):
            def get_flights(self, origin, destination, travel_day, pdf_run_id=None):
                if (origin, destination) == ("Budapest", "Rome") and travel_day == date(2026, 8, 28):
                    return [Flight("Budapest", "Rome", "W2", datetime(2026, 8, 28, 10, 59), datetime(2026, 8, 28, 13), "", "")]
                return super().get_flights(origin, destination, travel_day, pdf_run_id)

        rows, _ = cached_scan_itineraries(
            FakeGraph(), UnderTwoDB(), "London", "Rome", date(2026, 8, 28),
            days=1, max_stops=1, min_transfer_minutes=90, pdf_run_id="run",
        )
        self.assertFalse(any(len(r["legs"]) == 2 for r in rows))

    def test_max_layover_is_enforced_per_connection(self):
        rows, _ = cached_scan_itineraries(
            FakeGraph(), FakeDB(), "London", "Rome", date(2026, 8, 28),
            days=1, max_stops=1, min_transfer_minutes=120, max_transfer_minutes=140, pdf_run_id="run",
        )
        self.assertFalse(any(len(r["legs"]) == 2 for r in rows))


if __name__ == "__main__":
    unittest.main()
