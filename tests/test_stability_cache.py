import tempfile
import unittest
from pathlib import Path
from unittest import mock

import stability_cache


class StabilityCacheTests(unittest.TestCase):
    def test_refresh_materializes_and_read_reuses_rows(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "history.sqlite3")
            archive = [{
                "origin": "A", "destination": "B", "archive_score": 80.0,
                "recent_30d": 90.0, "first_seen": "2026-01-01",
                "last_seen": "2026-09-01", "observed_days": 10,
                "eligible_days": 12, "context_scores": {},
            }]
            local = [{
                "origin": "A", "destination": "B", "observed_scans": 2,
                "positive_checks": 3, "total_checks": 4, "available_dates": 3,
                "last_seen": "2026-09-02", "flight_appearances": 5,
                "availability_rate": 75.0,
            }]
            with mock.patch.object(stability_cache, "archive_scores", return_value=archive) as score_mock, \
                 mock.patch.object(stability_cache, "stability_rows", return_value=local), \
                 mock.patch.object(stability_cache, "history_stats", return_value={"snapshots": 2}), \
                 mock.patch.object(stability_cache, "external_stats", return_value={"snapshot_days": 349}):
                summary = stability_cache.refresh_stability_cache(db)
                self.assertEqual(summary["rows"], 1)
                self.assertEqual(score_mock.call_count, 1)
                stability_cache.stability_rows.assert_called_once_with(path=db, limit=5000)

            cached = stability_cache.read_stability_cache(db)
            self.assertEqual(cached["rows"][0]["archive_score"], 80.0)
            self.assertEqual(cached["rows"][0]["availability_rate"], 75.0)
            self.assertEqual(cached["external"]["snapshot_days"], 349)

    def test_read_empty_cache_is_fast_and_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / "history.sqlite3")
            self.assertIsNone(stability_cache.read_stability_cache(db))


if __name__ == "__main__":
    unittest.main()
