import csv
import os
import tempfile
import time
import unittest

from planner import AYCFPlanner


class PlannerStabilityTests(unittest.TestCase):
    def make_planner(self):
        tmp = tempfile.TemporaryDirectory()
        path = os.path.join(tmp.name, "runs.csv")
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["departure_from", "departure_to", "run_ts"])
            writer.writeheader()
            for origin, destination in [
                ("Liverpool", "Budapest"),
                ("Budapest", "Kutaisi"),
                ("Kutaisi", "Budapest"),
                ("Budapest", "Liverpool"),
            ]:
                writer.writerow({"departure_from": origin, "departure_to": destination, "run_ts": now})
        return tmp, AYCFPlanner(tmp.name)

    def test_home_defaults_do_not_scan_csvs(self):
        tmp, planner = self.make_planner()
        self.addCleanup(tmp.cleanup)
        planner._csv_paths = lambda: (_ for _ in ()).throw(AssertionError("homepage touched CSV dataset"))
        defaults = planner.ui_defaults()
        self.assertIn("Liverpool", defaults["base_options"])
        self.assertTrue(defaults["hub_options"])

    def test_route_counts_are_cached_and_iterrows_compatible(self):
        tmp, planner = self.make_planner()
        self.addCleanup(tmp.cleanup)
        first = planner.route_counts(180)
        self.assertEqual(len(first), 4)
        self.assertEqual(len(list(first.iterrows())), 4)
        cache_path = planner._route_cache_path(180)
        self.assertTrue(os.path.exists(cache_path))
        second = planner.route_counts(180)
        self.assertEqual(first, second)

    def test_round_trip_suggestion_survives_pandas_removal(self):
        tmp, planner = self.make_planner()
        self.addCleanup(tmp.cleanup)
        rows = planner.suggest_itineraries(
            180, 150, None, None,
            ["Liverpool"], ["Budapest"], ["Kutaisi"], True, 25,
        )
        self.assertTrue(rows)
        self.assertIn("Kutaisi", rows[0]["itinerary"])
        self.assertIn("Liverpool", rows[0]["return"])


if __name__ == "__main__":
    unittest.main()
