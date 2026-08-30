import os
import subprocess
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import termux.refresh_wizz_from_chrome as refresh
from morning_scan import CapturedRequestWizzClient
from termux.wizz_runtime import apply_runtime, normalize_runtime


CANONICAL = "https://multipass.wizzair.com/w6/subscriptions/json/availability/803e9c9c-5331-4b98-aa74-3104bb3b858e"
ROOT = Path(__file__).resolve().parents[1]


class FakeClient:
    def __init__(self, *_args, **_kwargs):
        self.dynamic_url = ""
        self.captured_request_method = ""
        self.captured_template_type = ""
        self.captured_request_template = None
        self.station_ids = {}
        self.http = object()
        self.preflight_calls = 0

    def preflight(self):
        self.preflight_calls += 1
        template = self.captured_request_template
        if not isinstance(template, dict):
            return {"ok": False, "reason": "no captured request template"}
        if not all(str(template.get(key) or "").strip() for key in ("origin", "destination", "departure")):
            return {"ok": False, "reason": "invalid blank preflight template"}
        return {"ok": True, "response": "no-availability"}


def assert_valid_probe(testcase: unittest.TestCase, template: dict):
    testcase.assertEqual(template["flightType"], "OW")
    testcase.assertTrue(template["origin"])
    testcase.assertTrue(template["destination"])
    testcase.assertNotEqual(template["origin"], template["destination"])
    testcase.assertEqual(template["departure"], date.today().isoformat())
    testcase.assertEqual(template["arrival"], "")
    testcase.assertIsNone(template["intervalSubtype"])


class WizzRuntimeRecoveryTests(unittest.TestCase):
    def test_canonical_get_capture_is_normalized_to_valid_probe(self):
        runtime = {
            "availability_url": CANONICAL,
            "request_method": "GET",
            "request_template_type": None,
            "request_template": None,
        }
        normalized, repaired = normalize_runtime(runtime)
        self.assertTrue(repaired)
        self.assertEqual(normalized["request_method"], "POST")
        self.assertEqual(normalized["request_template_type"], "json")
        assert_valid_probe(self, normalized["request_template"])

    def test_blank_post_template_is_upgraded_to_probe(self):
        runtime = {
            "availability_url": CANONICAL,
            "request_method": "POST",
            "request_template_type": "json",
            "request_template": {
                "flightType": "OW",
                "origin": "",
                "destination": "",
                "departure": "",
                "arrival": "",
                "intervalSubtype": None,
            },
            "station_ids": {"Budapest": "bud", "London Luton": "ltn"},
        }
        normalized, repaired = normalize_runtime(runtime)
        self.assertTrue(repaired)
        self.assertEqual(normalized["request_template"]["origin"], "BUD")
        self.assertEqual(normalized["request_template"]["destination"], "LTN")
        assert_valid_probe(self, normalized["request_template"])

    def test_real_captured_template_is_preserved(self):
        template = {
            "flightType": "OW",
            "origin": "LTN",
            "destination": "BUD",
            "departure": "2026-09-01",
            "arrival": "",
            "intervalSubtype": None,
        }
        runtime = {
            "availability_url": CANONICAL,
            "request_method": "POST",
            "request_template_type": "json",
            "request_template": dict(template),
        }
        normalized, repaired = normalize_runtime(runtime)
        self.assertFalse(repaired)
        self.assertEqual(normalized["request_template"], template)

    def test_apply_runtime_uses_the_supplied_runtime(self):
        runtime = {
            "availability_url": CANONICAL,
            "request_method": "GET",
            "request_template_type": None,
            "request_template": None,
            "station_ids": {"Budapest": "bud", "London Luton": "ltn"},
        }
        client = FakeClient()
        self.assertTrue(apply_runtime(client, runtime))
        self.assertEqual(client.dynamic_url, CANONICAL)
        self.assertEqual(client.captured_request_method, "POST")
        self.assertEqual(client.captured_template_type, "json")
        assert_valid_probe(self, client.captured_request_template)
        self.assertEqual(client.station_ids["london luton"], "LTN")

    def test_real_scanner_replaces_probe_with_requested_route_and_date(self):
        runtime = {
            "availability_url": CANONICAL,
            "request_method": "POST",
            "request_template_type": "json",
            "request_template": {
                "flightType": "OW",
                "origin": "",
                "destination": "",
                "departure": "",
                "arrival": "",
                "intervalSubtype": None,
            },
            "station_ids": {"Budapest": "BUD", "London Luton": "LTN"},
        }
        normalized, repaired = normalize_runtime(runtime)
        self.assertTrue(repaired)

        client = CapturedRequestWizzClient({"cookies": []}, cache_ttl=30, min_delay=0.2)
        self.assertTrue(apply_runtime(client, normalized))
        sent = []

        def fake_send(payload, context, allow_no_availability=True):
            sent.append((dict(payload), context, allow_no_availability))
            return {"flightsOutbound": []}

        with patch.object(client, "_send_and_decode", side_effect=fake_send):
            preflight = client.preflight()
            flights = client.check("London Luton", "Budapest", date(2026, 9, 7))

        self.assertTrue(preflight["ok"])
        self.assertEqual(flights, [])
        self.assertEqual(len(sent), 2)
        preflight_payload = sent[0][0]
        actual_payload = sent[1][0]
        assert_valid_probe(self, preflight_payload)
        self.assertEqual(actual_payload["flightType"], "OW")
        self.assertEqual(actual_payload["origin"], "LTN")
        self.assertEqual(actual_payload["destination"], "BUD")
        self.assertEqual(actual_payload["departure"], "2026-09-07")
        self.assertEqual(actual_payload["arrival"], "")
        self.assertIsNone(actual_payload["intervalSubtype"])

    def test_stale_endpoint_plus_missing_template_self_repairs(self):
        runtime = {
            "availability_url": "https://multipass.wizzair.com/legacy/flight-search",
            "request_method": "GET",
            "request_template_type": None,
            "request_template": None,
            "station_ids": {"Budapest": "BUD", "London Luton": "LTN"},
        }
        with patch.object(refresh, "CapturedRequestWizzClient", FakeClient), patch.object(
            refresh, "_rediscover_endpoint", return_value=CANONICAL
        ):
            client, preflight = refresh._validate_candidate({"cookies": []}, runtime)

        self.assertTrue(preflight["ok"])
        self.assertEqual(client.dynamic_url, CANONICAL)
        self.assertEqual(client.captured_request_method, "POST")
        self.assertEqual(client.captured_template_type, "json")
        assert_valid_probe(self, client.captured_request_template)
        self.assertEqual(runtime["availability_url"], CANONICAL)
        assert_valid_probe(self, runtime["request_template"])

    def test_canonical_blank_template_needs_no_rediscovery(self):
        runtime = {
            "availability_url": CANONICAL,
            "request_method": "POST",
            "request_template_type": "json",
            "request_template": {
                "flightType": "OW",
                "origin": "",
                "destination": "",
                "departure": "",
                "arrival": "",
                "intervalSubtype": None,
            },
            "station_ids": {"Budapest": "BUD", "London Luton": "LTN"},
        }
        with patch.object(refresh, "CapturedRequestWizzClient", FakeClient), patch.object(
            refresh, "_rediscover_endpoint", side_effect=AssertionError("unexpected rediscovery")
        ):
            client, preflight = refresh._validate_candidate({"cookies": []}, runtime)

        self.assertTrue(preflight["ok"])
        self.assertEqual(client.preflight_calls, 1)
        self.assertEqual(runtime["request_method"], "POST")
        self.assertEqual(runtime["request_template_type"], "json")
        assert_valid_probe(self, runtime["request_template"])

    def test_manual_import_loads_env_before_freezing_runtime_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            home = Path(temp_dir)
            default_config = home / ".config" / "aycf"
            custom_config = home / "custom-aycf-config"
            default_config.mkdir(parents=True)
            (default_config / "env").write_text(
                f"export AYCF_CONFIG_DIR='{custom_config}'\n",
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["HOME"] = str(home)
            env.pop("AYCF_CONFIG_DIR", None)
            env["PYTHONPATH"] = str(ROOT)
            process = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "import termux.import_wizz_from_chrome as m; print(m.CONFIG_DIR)",
                ],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )

            self.assertEqual(process.stdout.strip(), str(custom_config))


if __name__ == "__main__":
    unittest.main()
