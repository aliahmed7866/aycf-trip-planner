import os
import tempfile
import unittest
from datetime import date, datetime

import pandas as pd

from scanner import CurrentRouteGraph, Flight, TTLCache, WizzAYCFClient, _parse_dt, combine_path


class FakeClient:
    def __init__(self):
        self.calls = []

    def check(self, origin, destination, day):
        self.calls.append((origin, destination, day))
        if (origin, destination) == ("London", "Budapest"):
            return [Flight(origin, destination, "W6001", datetime(2026, 8, 25, 8), datetime(2026, 8, 25, 11), "08:00", "11:00")]
        if (origin, destination) == ("Budapest", "Kutaisi") and day == date(2026, 8, 25):
            return [Flight(origin, destination, "W6002", datetime(2026, 8, 25, 14), datetime(2026, 8, 25, 18), "14:00", "18:00")]
        return []


class ScannerTests(unittest.TestCase):
    def test_latest_pdf_graph_builds_one_stop_path(self):
        with tempfile.TemporaryDirectory() as root:
            pd.DataFrame([
                {"departure_from": "London", "departure_to": "Budapest", "availability_start": "2026-08-25", "availability_end": "2026-08-28", "data_generated": "2026-08-25T07:00:00"},
                {"departure_from": "Budapest", "departure_to": "Kutaisi", "availability_start": "2026-08-25", "availability_end": "2026-08-28", "data_generated": "2026-08-25T07:00:00"},
                {"departure_from": "London", "departure_to": "Kutaisi", "availability_start": "2026-08-20", "availability_end": "2026-08-24", "data_generated": "2026-08-25T07:00:00"},
            ]).to_csv(os.path.join(root, "latest.csv"), index=False)
            graph = CurrentRouteGraph(root)
            self.assertEqual(
                graph.paths("London", "Kutaisi", date(2026, 8, 25), max_stops=1),
                [["London", "Budapest", "Kutaisi"]],
            )

    def test_only_newest_snapshot_is_used(self):
        with tempfile.TemporaryDirectory() as root:
            pd.DataFrame([
                {"departure_from": "London", "departure_to": "Old City", "data_generated": "2026-08-24T07:00:00"},
            ]).to_csv(os.path.join(root, "old.csv"), index=False)
            pd.DataFrame([
                {"departure_from": "London", "departure_to": "New City", "data_generated": "2026-08-25T07:00:00"},
            ]).to_csv(os.path.join(root, "new.csv"), index=False)
            graph = CurrentRouteGraph(root)
            self.assertEqual(graph.paths("London", None, date(2026, 8, 25), max_stops=0), [["London", "New City"]])

    def test_direct_paths_are_returned_before_connections(self):
        with tempfile.TemporaryDirectory() as root:
            pd.DataFrame([
                {"departure_from": "A", "departure_to": "B", "data_generated": "2026-08-25T07:00:00"},
                {"departure_from": "A", "departure_to": "H", "data_generated": "2026-08-25T07:00:00"},
                {"departure_from": "H", "departure_to": "C", "data_generated": "2026-08-25T07:00:00"},
            ]).to_csv(os.path.join(root, "latest.csv"), index=False)
            graph = CurrentRouteGraph(root)
            paths = graph.paths("A", None, date(2026, 8, 25), max_stops=1)
            self.assertEqual(paths[0], ["A", "B"])
            self.assertEqual(paths[1], ["A", "H"])
            self.assertIn(["A", "H", "C"], paths)

    def test_connection_threshold(self):
        client = FakeClient()
        result = combine_path(client, ["London", "Budapest", "Kutaisi"], date(2026, 8, 25), 150)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["connection_minutes"], 180)
        rejected = combine_path(client, ["London", "Budapest", "Kutaisi"], date(2026, 8, 25), 240)
        self.assertEqual(rejected, [])

    def test_iso_timestamp_is_normalised_to_utc_for_connections(self):
        parsed = _parse_dt("2026-08-25", "2026-08-25T14:00:00+02:00")
        self.assertEqual(parsed, datetime(2026, 8, 25, 12, 0, 0))

    def test_time_only_timestamp_uses_search_day(self):
        self.assertEqual(_parse_dt("2026-08-25", "08:45"), datetime(2026, 8, 25, 8, 45))

    def test_response_shape_variants(self):
        row = {"flightCode": "W6001"}
        self.assertEqual(WizzAYCFClient._flight_rows({"flightsOutbound": [row]}), [row])
        self.assertEqual(WizzAYCFClient._flight_rows({"data": {"outboundFlights": [row]}}), [row])
        self.assertEqual(WizzAYCFClient._flight_rows({"result": {"flights": [row]}}), [row])
        self.assertEqual(WizzAYCFClient._flight_rows([]), [])

    def test_ttl_cache_can_store_empty_flight_results(self):
        cache = TTLCache()
        cache.set("leg", [], 60)
        self.assertEqual(cache.get("leg"), [])


if __name__ == "__main__":
    unittest.main()
