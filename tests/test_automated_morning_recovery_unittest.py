import unittest
from unittest import mock

import requests

from scanner import WizzIntegrationChanged
from termux import automated_morning


class AutomatedMorningRecoveryTests(unittest.TestCase):
    def test_http_400_session_expired_is_renewed_and_resumed(self):
        expired = WizzIntegrationChanged(
            "Wizz rejected AYCF search Budapest (BUD) -> Thessaloniki (SKG) on 2026-09-02 "
            "with HTTP 400: The session=<redacted> expired. Please, try again."
        )
        completed = {"ok": True, "route_day_checks": 206, "flights_found": 12}
        with mock.patch.object(automated_morning.tiered_morning, "run", side_effect=[expired, completed]) as run_scan, \
             mock.patch.object(automated_morning, "_refresh", return_value=True) as refresh:
            result = automated_morning._run_once(force=False)

        self.assertEqual(result, completed)
        self.assertEqual(run_scan.call_count, 2)
        refresh.assert_called_once()
        self.assertIn("session expired during scan", refresh.call_args.args[0])

    def test_repeated_expiry_can_recover_more_than_once(self):
        expired = WizzIntegrationChanged("HTTP 400: The session=<redacted> expired. Please, try again.")
        completed = {"ok": True}
        with mock.patch.dict("os.environ", {"AYCF_MAX_AUTH_RECOVERIES_PER_SCAN": "3"}, clear=False), \
             mock.patch.object(automated_morning.tiered_morning, "run", side_effect=[expired, expired, completed]) as run_scan, \
             mock.patch.object(automated_morning, "_refresh", return_value=True) as refresh:
            result = automated_morning._run_once(force=False)

        self.assertEqual(result, completed)
        self.assertEqual(run_scan.call_count, 3)
        self.assertEqual(refresh.call_count, 2)

    def test_unrelated_integration_change_is_not_misclassified(self):
        changed = WizzIntegrationChanged("HTTP 400: request schema changed")
        with mock.patch.object(automated_morning.tiered_morning, "run", side_effect=changed), \
             mock.patch.object(automated_morning, "_refresh") as refresh:
            with self.assertRaises(WizzIntegrationChanged):
                automated_morning._run_once(force=False)
        refresh.assert_not_called()

    def test_plain_http_400_is_not_auto_repaired_as_server_error(self):
        response = requests.Response()
        response.status_code = 400
        error = requests.HTTPError("400 bad request", response=response)
        with mock.patch.object(automated_morning.tiered_morning, "run", side_effect=error), \
             mock.patch.object(automated_morning, "_refresh") as refresh:
            with self.assertRaises(requests.HTTPError):
                automated_morning._run_once(force=False)
        refresh.assert_not_called()


if __name__ == "__main__":
    unittest.main()
