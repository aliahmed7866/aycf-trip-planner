import unittest
from unittest.mock import patch

import requests

from termux import automated_morning


class AutomatedMorningTests(unittest.TestCase):
    @staticmethod
    def _http_error(status):
        response = requests.Response()
        response.status_code = status
        response.url = "https://multipass.wizzair.com/w6/subscriptions/json/availability/test"
        return requests.HTTPError(f"HTTP {status}", response=response)

    def test_persistent_5xx_reports_service_outage_without_auth_refresh(self):
        error = self._http_error(500)
        with patch.object(
            automated_morning.tiered_morning,
            "run",
            side_effect=error,
        ) as scan, patch.object(
            automated_morning,
            "_refresh",
        ) as refresh, patch.object(
            automated_morning,
            "write_status",
        ) as status:
            result = automated_morning.run(force=True)

        self.assertEqual(scan.call_count, 1)
        refresh.assert_not_called()
        self.assertFalse(result["ok"])
        self.assertEqual(result["state"], "wizz_service_unavailable")
        self.assertFalse(result["scan_performed"])
        self.assertIn("HTTP 500", result["message"])
        self.assertEqual(status.call_args_list[-1].args[0], "service_unavailable")

    def test_service_outage_skips_post_scan_maintenance(self):
        error = self._http_error(503)
        with patch.object(
            automated_morning,
            "_run_once",
            return_value={
                "ok": False,
                "state": "wizz_service_unavailable",
                "scan_performed": False,
                "message": str(error),
            },
        ), patch.object(
            automated_morning,
            "_snapshot_history_after_scan",
        ) as history, patch.object(
            automated_morning,
            "_refresh_stability_after_scan",
        ) as stability, patch.object(
            automated_morning,
            "_check_watches_after_scan",
        ) as watches:
            result = automated_morning.run()

        self.assertEqual(result["state"], "wizz_service_unavailable")
        history.assert_not_called()
        stability.assert_not_called()
        watches.assert_not_called()

    def test_non_5xx_http_error_is_not_reclassified(self):
        error = self._http_error(400)
        with patch.object(
            automated_morning.tiered_morning,
            "run",
            side_effect=error,
        ), patch.object(
            automated_morning,
            "_refresh",
            return_value=False,
        ) as refresh:
            with self.assertRaises(requests.HTTPError) as raised:
                automated_morning.run()
        self.assertIs(raised.exception, error)
        self.assertEqual(refresh.call_count, 0)

    def test_skipped_scan_is_reported_as_not_performed(self):
        skipped = {"ok": True, "skipped": True, "reason": "Current PDF already scanned"}
        with patch.object(automated_morning, "_run_once", return_value=skipped), \
             patch.object(automated_morning, "_snapshot_history_after_scan", return_value={}), \
             patch.object(automated_morning, "_refresh_stability_after_scan", return_value={}), \
             patch.object(automated_morning, "_check_watches_after_scan", return_value={}), \
             patch.object(automated_morning, "write_status") as status:
            automated_morning.run()
        final = status.call_args_list[-1]
        self.assertEqual(final.args[0], "complete")
        self.assertFalse(final.kwargs["scan_performed"])
        self.assertIn("already current", final.args[1])


if __name__ == "__main__":
    unittest.main()
