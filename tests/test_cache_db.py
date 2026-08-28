import os
import tempfile
import unittest
from datetime import date, datetime, timedelta

import pandas as pd

from cache_db import ScanCacheDB, cached_scan_itineraries
from scanner import CurrentRouteGraph, Flight


class CacheDBTests(unittest.TestCase):
    def test_zero_flight_check_is_cached(self):
        with tempfile.TemporaryDirectory() as root:
            db = ScanCacheDB(os.path.join(root, "cache.sqlite3"))
            db.upsert_pdf_run("run1", "2026-08-25T07:00:00", "2026-08-25T07:00:00", "2026-08-28T23:59:59", 1)
            day = date(2026, 8, 25)
            db.replace_route_check("run1", "London", "Budapest", day, [])
            self.assertTrue(db.route_checked("run1", "London", "Budapest", day))
            self.assertEqual(db.route_flight_count("run1", "London", "Budapest", day), 0)
            self.assertEqual(db.get_flights("London", "Budapest", day), [])
            self.assertIsNone(db.route_flight_count("run1", "London", "Budapest", date(2026, 8, 26)))
            self.assertIsNone(db.get_flights("London", "Budapest", date(2026, 8, 26)))

    def test_route_flight_count_respects_freshness_window(self):
        with tempfile.TemporaryDirectory() as root:
            db = ScanCacheDB(os.path.join(root, "cache.sqlite3"))
            db.upsert_pdf_run("run1", "2026-08-25T07:00:00", "2026-08-25T07:00:00", "2026-08-28T23:59:59", 1)
            day = date(2026, 8, 25)
            db.replace_route_check("run1", "London", "Budapest", day, [])
            self.assertEqual(db.route_flight_count("run1", "London", "Budapest", day, max_age_seconds=1800), 0)
            stale = (datetime.utcnow() - timedelta(hours=2)).isoformat()
            with db.connect() as conn:
                conn.execute(
                    "UPDATE route_checks SET fetched_at=? WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date=?",
                    (stale, "run1", "London", "Budapest", day.isoformat()),
                )
            self.assertIsNone(db.route_flight_count("run1", "London", "Budapest", day, max_age_seconds=1800))
            self.assertEqual(db.route_flight_count("run1", "London", "Budapest", day), 0)

    def test_route_flight_count_survives_restart(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "cache.sqlite3")
            day = date(2026, 8, 25)
            db = ScanCacheDB(path)
            db.upsert_pdf_run("run1", "2026-08-25T07:00:00", "2026-08-25T07:00:00", "2026-08-28T23:59:59", 1)
            flights = [
                Flight("London", "Budapest", "W6001", datetime(2026, 8, 25, 8), datetime(2026, 8, 25, 11), "08:00", "11:00"),
                Flight("London", "Budapest", "W6003", datetime(2026, 8, 25, 12), datetime(2026, 8, 25, 15), "12:00", "15:00"),
            ]
            db.replace_route_check("run1", "London", "Budapest", day, flights)
            reopened = ScanCacheDB(path)
            self.assertEqual(reopened.route_flight_count("run1", "London", "Budapest", day), 2)
            self.assertEqual(len(reopened.get_flights("London", "Budapest", day, "run1")), 2)

    def test_running_scan_is_detected(self):
        with tempfile.TemporaryDirectory() as root:
            db = ScanCacheDB(os.path.join(root, "cache.sqlite3"))
            scan_id = db.start_scan("run1")
            self.assertTrue(db.scan_in_progress("run1"))
            db.finish_scan(scan_id, "completed", 0, 0, 0)
            self.assertFalse(db.scan_in_progress("run1"))

    def test_cached_itinerary_builds_connection_without_live_client(self):
        with tempfile.TemporaryDirectory() as root:
            data_dir = os.path.join(root, "data")
            os.makedirs(data_dir)
            pd.DataFrame([
                {"departure_from": "London", "departure_to": "Budapest", "availability_start": "2026-08-25", "availability_end": "2026-08-28", "data_generated": "2026-08-25T07:00:00"},
                {"departure_from": "Budapest", "departure_to": "Kutaisi", "availability_start": "2026-08-25", "availability_end": "2026-08-28", "data_generated": "2026-08-25T07:00:00"},
            ]).to_csv(os.path.join(data_dir, "run.csv"), index=False)
            graph = CurrentRouteGraph(data_dir)
            db = ScanCacheDB(os.path.join(root, "cache.sqlite3"))
            db.upsert_pdf_run("run1", "2026-08-25T07:00:00", "2026-08-25T07:00:00", "2026-08-28T23:59:59", 2)
            first = Flight("London", "Budapest", "W6001", datetime(2026, 8, 25, 8), datetime(2026, 8, 25, 11), "08:00", "11:00")
            second = Flight("Budapest", "Kutaisi", "W6002", datetime(2026, 8, 25, 14), datetime(2026, 8, 25, 18), "14:00", "18:00")
            db.replace_route_check("run1", "London", "Budapest", date(2026, 8, 25), [first])
            db.replace_route_check("run1", "Budapest", "Kutaisi", date(2026, 8, 25), [second])
            db.replace_route_check("run1", "Budapest", "Kutaisi", date(2026, 8, 26), [])
            results, misses = cached_scan_itineraries(graph, db, "London", "Kutaisi", date(2026, 8, 25), days=1)
            self.assertEqual(misses, 0)
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["connection_minutes"], 180)
            self.assertEqual(results[0]["source"], "morning-cache")


if __name__ == "__main__":
    unittest.main()
