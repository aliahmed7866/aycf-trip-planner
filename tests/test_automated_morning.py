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

    def test_persistent_5xx_refreshes_endpoint_and_resumes_once(self):
        expected = {"status": "completed"}
        with patch.object(
            automated_morning.tiered_morning,
            "run",
            side_effect=[self._http_error(500), expected],
        ) as scan, patch.object(
            automated_morning,
            "_refresh",
            return_value=True,
        ) as refresh:
            result = automated_morning.run(force=True)

        self.assertEqual(result, expected)
        self.assertEqual(scan.call_count, 2)
        self.assertEqual(refresh.call_count, 1)
        self.assertIn("persistent Wizz server error", refresh.call_args.args[0])

    def test_persistent_5xx_returns_attention_when_refresh_is_unavailable(self):
        error = self._http_error(503)
        with patch.object(
            automated_morning.tiered_morning,
            "run",
            side_effect=error,
        ) as scan, patch.object(
            automated_morning,
            "_refresh",
            return_value=False,
        ) as refresh:
            result = automated_morning.run()

        self.assertEqual(scan.call_count, 1)
        self.assertEqual(refresh.call_count, 1)
        self.assertFalse(result["ok"])
        self.assertEqual(result["state"], "wizz_authentication_required")
        self.assertFalse(result["scan_performed"])
        self.assertIn("HTTP 503", result["message"])

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


if __name__ == "__main__":
    unittest.main()
