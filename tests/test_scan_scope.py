import os
import tempfile
import unittest
from unittest.mock import patch

from scan_scope import (
    airport_variants,
    default_scope,
    expand_scan_routes,
    filter_routes,
    load_scope,
    normalize_name,
    origin_options,
    origin_variants,
    save_scope,
    scan_plan,
    scope_fingerprint,
)


class ScanScopeTests(unittest.TestCase):
    def test_normalization_handles_airport_name_variants(self):
        self.assertEqual(normalize_name("Leeds/Bradford"), "leeds bradford")
        self.assertEqual(normalize_name("  São Paulo  "), "sao paulo")
        self.assertEqual(normalize_name("Basel & Mulhouse"), "basel and mulhouse")

    def test_scope_fingerprint_is_order_independent(self):
        a = {"origins": ["Liverpool", "Birmingham"], "destination_mode": "only", "destinations": ["Warsaw", "Budapest"], "connection_hubs": ["Budapest", "Warsaw"]}
        b = {"origins": ["Birmingham", "Liverpool"], "destination_mode": "only", "destinations": ["Budapest", "Warsaw"], "connection_hubs": ["Warsaw", "Budapest"]}
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
        self.assertEqual(airport_variants("London", scope), ["London Gatwick", "London Luton"])
        self.assertEqual(origin_options(["London", "Liverpool"]), ["Liverpool", "London Gatwick", "London Luton", "London Stansted"])

    def test_hub_routes_expand_only_when_reachable(self):
        pairs = [("Liverpool", "Budapest"), ("Liverpool", "Rome"), ("Budapest", "Tirana"), ("Budapest", "Athens"), ("Warsaw", "Tirana")]
        scope = {"origins": ["Liverpool"], "destination_mode": "all", "destinations": [], "connection_hubs": ["Budapest", "Warsaw"]}
        primary, hubs = expand_scan_routes(pairs, scope)
        self.assertEqual(primary, [("Liverpool", "Budapest"), ("Liverpool", "Rome")])
        self.assertEqual(hubs, [("Budapest", "Athens"), ("Budapest", "Tirana")])

    def test_bidirectional_scan_adds_reverse_base_and_hub_legs(self):
        pairs = [("Liverpool", "Budapest"), ("Budapest", "Liverpool"), ("Budapest", "Tirana"), ("Tirana", "Budapest")]
        scope = {"origins": ["Liverpool"], "destination_mode": "all", "destinations": [], "connection_hubs": ["Budapest"]}
        primary, hubs = expand_scan_routes(pairs, scope)
        self.assertEqual(primary, [("Budapest", "Liverpool"), ("Liverpool", "Budapest")])
        self.assertEqual(hubs, [("Budapest", "Tirana"), ("Tirana", "Budapest")])

    def test_only_mode_keeps_hub_ingress_for_connection(self):
        pairs = [("Liverpool", "Budapest"), ("Budapest", "Tirana"), ("Liverpool", "Rome")]
        scope = {"origins": ["Liverpool"], "destination_mode": "only", "destinations": ["Tirana"], "connection_hubs": ["Budapest"]}
        primary, hubs = expand_scan_routes(pairs, scope)
        self.assertEqual(primary, [("Liverpool", "Budapest")])
        self.assertEqual(hubs, [("Budapest", "Tirana")])

    def test_scan_estimate_counts_grouped_london_requests(self):
        pairs = [("London", "Budapest"), ("Budapest", "Tirana")]
        scope = {"origins": ["London Gatwick", "London Luton", "London Stansted"], "destination_mode": "all", "destinations": [], "connection_hubs": ["Budapest"]}
        plan = scan_plan(pairs, scope, days=4, seconds_per_request=1.0)
        self.assertEqual(plan["checks"], 8)
        self.assertEqual(plan["request_units"], 16)

    def test_reverse_grouped_london_counts_destination_variants(self):
        pairs = [("London", "Budapest"), ("Budapest", "London")]
        scope = {"origins": ["London Gatwick", "London Luton", "London Stansted"], "destination_mode": "all", "destinations": [], "connection_hubs": ["Budapest"]}
        plan = scan_plan(pairs, scope, days=1, seconds_per_request=1.0)
        self.assertEqual(plan["checks"], 2)
        self.assertEqual(plan["request_units"], 6)

    def test_scope_round_trip_is_local_and_private(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, {"AYCF_CONFIG_DIR": root}, clear=False):
                saved = save_scope(["Liverpool", "Liverpool", "Birmingham"], "only", ["Warsaw", "Budapest"], ["Warsaw"])
                loaded = load_scope()
                self.assertEqual(saved, loaded)
                self.assertEqual(loaded["origins"], ["Liverpool", "Birmingham"])
                self.assertEqual(loaded["connection_hubs"], ["Warsaw"])
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
