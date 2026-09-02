import unittest
from unittest.mock import patch

from flask import Flask

import stability_blueprint


class StabilityPagePerformanceTests(unittest.TestCase):
    def test_stability_page_reads_materialized_cache_without_rebuild(self):
        app = Flask(__name__)
        cache_payload = {
            "generated_at": "2026-09-02T07:30:00+00:00",
            "stats": {},
            "external": {},
            "rows": [
                {
                    "origin": "A",
                    "destination": "B",
                    "archive": None,
                    "archive_score": 50.0,
                    "recent_30d": 60.0,
                    "availability_rate": None,
                },
                {
                    "origin": "C",
                    "destination": "D",
                    "archive": None,
                    "archive_score": 40.0,
                    "recent_30d": 50.0,
                    "availability_rate": None,
                },
            ],
        }
        with app.test_request_context("/stability"):
            with patch.object(stability_blueprint, "ScanCacheDB") as cache_cls, \
                 patch.object(stability_blueprint, "snapshot_latest_run"), \
                 patch.object(stability_blueprint, "read_stability_cache", return_value=cache_payload) as read_cache, \
                 patch.object(stability_blueprint, "refresh_stability_cache") as refresh_cache, \
                 patch.object(stability_blueprint, "render_template", return_value="ok"):
                cache_cls.return_value = object()
                response = stability_blueprint.page()

        self.assertEqual(response, "ok")
        read_cache.assert_called_once_with()
        refresh_cache.assert_not_called()


if __name__ == "__main__":
    unittest.main()
