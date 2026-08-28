import os
import sqlite3
import tempfile
import unittest
from datetime import date, datetime

from cache_db import ScanCacheDB
from scanner import Flight


class PhysicalAirportCacheTests(unittest.TestCase):
    def test_legacy_positive_row_is_stale_for_incremental_refresh(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "cache.sqlite3")
            db = ScanCacheDB(path)
            day = date(2026, 8, 25)
            db.upsert_pdf_run("run1", "2026-08-25T07:00:00", "2026-08-25", "2026-08-28", 1)
            db.replace_route_check(
                "run1", "London", "Budapest", day,
                [Flight("London Gatwick", "Budapest", "W6001", datetime(2026, 8, 25, 8), datetime(2026, 8, 25, 11), "08:00", "11:00")],
            )
            with db.connect() as conn:
                conn.execute("UPDATE route_flights SET physical_origin=NULL, physical_destination=NULL WHERE pdf_run_id='run1'")
            info = db.route_check_info("run1", "London", "Budapest", day)
            self.assertTrue(info["physical_missing"])
            self.assertEqual(info["age_seconds"], float("inf"))
            self.assertIsNone(db.route_flight_count("run1", "London", "Budapest", day, max_age_seconds=3600))
            self.assertEqual(db.route_flight_count("run1", "London", "Budapest", day), 1)

    def test_migration_keeps_legacy_db_readable(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "cache.sqlite3")
            conn = sqlite3.connect(path)
            conn.execute("CREATE TABLE route_flights (pdf_run_id TEXT NOT NULL, origin TEXT NOT NULL, destination TEXT NOT NULL, travel_date TEXT NOT NULL, flight_code TEXT NOT NULL, departure TEXT NOT NULL, arrival TEXT NOT NULL, departure_text TEXT, arrival_text TEXT, duration TEXT, fetched_at TEXT NOT NULL, PRIMARY KEY (pdf_run_id, origin, destination, travel_date, flight_code, departure))")
            conn.commit(); conn.close()
            db = ScanCacheDB(path)
            with db.connect() as migrated:
                columns = {row["name"] for row in migrated.execute("PRAGMA table_info(route_flights)")}
            self.assertIn("physical_origin", columns)
            self.assertIn("physical_destination", columns)


if __name__ == "__main__":
    unittest.main()
