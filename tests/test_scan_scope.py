import os
import tempfile
import unittest
from unittest.mock import patch

from scan_scope import (
    default_scope,
    filter_routes,
    load_scope,
    normalize_name,
    origin_options,
    origin_variants,
    save_scope,
    scope_fingerprint,
)


class ScanScopeTests(unittest.TestCase):
    def test_normalization_handles_airport_name_variants(self):
        self.assertEqual(normalize_name("Leeds/Bradford"), "leeds bradford")
        self.assertEqual(normalize_name("  São Paulo  "), "sao paulo")
        self.assertEqual(normalize_name("Basel & Mulhouse"), "basel and mulhouse")

    def test_scope_fingerprint_is_order_independent(self):
        a = {"origins": ["Liverpool", "Birmingham"], "destination_mode": "only", "destinations": ["Warsaw", "Budapest"]}
        b = {"origins": ["Birmingham", "Liverpool"], "destination_mode": "only", "destinations": ["Budapest", "Warsaw"]}
        self.assertEqual(scope_fingerprint(a), scope_fingerprint(b))

    def test_filter_routes_supports_all_only_and_exclude(self):
        pairs = [("Liverpool", "Warsaw"), ("Liverpool", "Budapest"), ("Birmingham", "Warsaw"), ("London Luton", "Rome")]
        base = {"origins": ["Liverpool", "Birmingham"], "destinations": []}
        self.assertEqual(filter_routes(pairs, {**base, "destination_mode": "all"}), [("Birmingham", "Warsaw"), ("Liverpool", "Budapest"), ("Liverpool", "Warsaw")])
        self.assertEqual(filter_routes(pairs, {**base, "destination_mode": "only", "destinations": ["Warsaw"]}), [("Birmingham", "Warsaw"), ("Liverpool", "Warsaw")])
        self.assertEqual(filter_routes(pairs, {**base, "destination_mode": "exclude", "destinations": ["Warsaw"]}), [("Liverpool", "Budapest")])

    def test_generic_london_route_expands_selected_airports(self):
        scope = {"origins": ["London Gatwick", "London Luton"], "destination_mode": "all", "destinations": []}
        self.assertEqual(filter_routes([("London", "Rome")], scope), [("London", "Rome")])
        self.assertEqual(origin_variants("London", scope), ["London Gatwick", "London Luton"])
        self.assertEqual(origin_options(["London", "Liverpool"]), ["Liverpool", "London Gatwick", "London Luton", "London Stansted"])

    def test_scope_round_trip_is_local_and_private(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, {"AYCF_CONFIG_DIR": root}, clear=False):
                saved = save_scope(["Liverpool", "Liverpool", "Birmingham"], "only", ["Warsaw", "Budapest"])
                loaded = load_scope()
                self.assertEqual(saved, loaded)
                self.assertEqual(loaded["origins"], ["Liverpool", "Birmingham"])
                mode = os.stat(os.path.join(root, "scan_scope.json")).st_mode & 0o777
                self.assertEqual(mode, 0o600)

    def test_invalid_or_empty_scope_falls_back_safely(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, {"AYCF_CONFIG_DIR": root}, clear=False):
                self.assertEqual(load_scope(), default_scope())
                with self.assertRaises(ValueError):
                    save_scope([], "all", [])
                with self.assertRaises(ValueError):
                    save_scope(["Liverpool"], "only", [])


if __name__ == "__main__":
    unittest.main()
