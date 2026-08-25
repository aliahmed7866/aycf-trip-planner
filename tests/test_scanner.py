import os
import tempfile
import unittest
from datetime import date, datetime

import pandas as pd

from scanner import CurrentRouteGraph, Flight, combine_path


class FakeClient:
    def check(self, origin, destination, day):
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

    def test_connection_threshold(self):
        result = combine_path(FakeClient(), ["London", "Budapest", "Kutaisi"], date(2026, 8, 25), 150)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["connection_minutes"], 180)
        rejected = combine_path(FakeClient(), ["London", "Budapest", "Kutaisi"], date(2026, 8, 25), 240)
        self.assertEqual(rejected, [])


if __name__ == "__main__":
    unittest.main()
