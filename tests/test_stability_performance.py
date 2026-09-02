import unittest
from unittest.mock import patch

from flask import Flask

import stability_blueprint


class StabilityPagePerformanceTests(unittest.TestCase):
    def test_stability_page_builds_combined_archive_rows_once(self):
        app = Flask(__name__)
        rows = [
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
        ]
        with app.test_request_context("/stability"):
            with patch.object(stability_blueprint, "ScanCacheDB") as cache_cls, \
                 patch.object(stability_blueprint, "snapshot_latest_run"), \
                 patch.object(stability_blueprint, "_combined_rows", return_value=rows) as combined, \
                 patch.object(stability_blueprint, "history_stats", return_value={}), \
                 patch.object(stability_blueprint, "external_stats", return_value={}), \
                 patch.object(stability_blueprint, "render_template", return_value="ok"):
                cache_cls.return_value = object()
                response = stability_blueprint.page()

        self.assertEqual(response, "ok")
        combined.assert_called_once_with(limit=5000)


if __name__ == "__main__":
    unittest.main()
