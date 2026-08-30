import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import termux.refresh_wizz_from_chrome as refresh
from termux.wizz_runtime import DEFAULT_TEMPLATE, apply_runtime, normalize_runtime


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
        if not isinstance(self.captured_request_template, dict):
            return {"ok": False, "reason": "no captured request template"}
        return {"ok": True, "response": "no-availability"}


class WizzRuntimeRecoveryTests(unittest.TestCase):
    def test_canonical_get_capture_is_normalized(self):
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
        self.assertEqual(normalized["request_template"], DEFAULT_TEMPLATE)

    def test_apply_runtime_uses_the_supplied_runtime(self):
        runtime = {
            "availability_url": CANONICAL,
            "request_method": "GET",
            "request_template_type": None,
            "request_template": None,
            "station_ids": {"London Luton": "ltn"},
        }
        client = FakeClient()
        self.assertTrue(apply_runtime(client, runtime))
        self.assertEqual(client.dynamic_url, CANONICAL)
        self.assertEqual(client.captured_request_method, "POST")
        self.assertEqual(client.captured_template_type, "json")
        self.assertEqual(client.captured_request_template, DEFAULT_TEMPLATE)
        self.assertEqual(client.station_ids["london luton"], "LTN")

    def test_stale_endpoint_plus_missing_template_self_repairs(self):
        runtime = {
            "availability_url": "https://multipass.wizzair.com/legacy/flight-search",
            "request_method": "GET",
            "request_template_type": None,
            "request_template": None,
        }
        with patch.object(refresh, "CapturedRequestWizzClient", FakeClient), patch.object(
            refresh, "_rediscover_endpoint", return_value=CANONICAL
        ):
            client, preflight = refresh._validate_candidate({"cookies": []}, runtime)

        self.assertTrue(preflight["ok"])
        self.assertEqual(client.dynamic_url, CANONICAL)
        self.assertEqual(client.captured_request_method, "POST")
        self.assertEqual(client.captured_template_type, "json")
        self.assertEqual(client.captured_request_template, DEFAULT_TEMPLATE)
        self.assertEqual(runtime["availability_url"], CANONICAL)
        self.assertEqual(runtime["request_template"], DEFAULT_TEMPLATE)

    def test_canonical_missing_template_needs_no_rediscovery(self):
        runtime = {
            "availability_url": CANONICAL,
            "request_method": "GET",
            "request_template_type": None,
            "request_template": None,
        }
        with patch.object(refresh, "CapturedRequestWizzClient", FakeClient), patch.object(
            refresh, "_rediscover_endpoint", side_effect=AssertionError("unexpected rediscovery")
        ):
            client, preflight = refresh._validate_candidate({"cookies": []}, runtime)

        self.assertTrue(preflight["ok"])
        self.assertEqual(client.preflight_calls, 1)
        self.assertEqual(runtime["request_method"], "POST")
        self.assertEqual(runtime["request_template_type"], "json")
        self.assertEqual(runtime["request_template"], DEFAULT_TEMPLATE)

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
