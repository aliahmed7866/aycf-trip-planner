import os
import tempfile
import unittest
from unittest.mock import patch

from scan_scope import (
    DEFAULT_HUBS,
    DEFAULT_ORIGINS,
    airport_variants,
    default_scope,
    destination_priority,
    expand_scan_routes,
    filter_routes,
    is_high_value_destination,
    is_high_value_route,
    load_scope,
    normalize_name,
    origin_options,
    origin_variants,
    route_priority,
    save_scope,
    scan_plan,
    scope_fingerprint,
)


class ScanScopeTests(unittest.TestCase):
    def test_normalization_handles_airport_name_variants(self):
        self.assertEqual(normalize_name("Leeds/Bradford"), "leeds bradford")
        self.assertEqual(normalize_name("  São Paulo  "), "sao paulo")
        self.assertEqual(normalize_name("Basel & Mulhouse"), "basel and mulhouse")

    def test_default_home_points_and_hubs(self):
        self.assertEqual(DEFAULT_ORIGINS, ["Liverpool", "Leeds/Bradford", "Birmingham", "London Gatwick", "London Luton", "London Stansted"])
        for hub in ["Bucharest", "Budapest", "Rome", "Milan Malpensa", "Warsaw", "Gdansk", "Krakow", "Katowice"]:
            self.assertIn(hub, DEFAULT_HUBS)

    def test_priority_destination_classification(self):
        for city in ["Amman", "Jeddah", "Kutaisi", "Baku", "Yerevan", "Hurghada", "Sharm El-Sheikh", "Giza Sphinx"]:
            self.assertEqual(destination_priority(city), 1, city)
        for city in ["Belgrade", "Pristina", "Skopje", "Sofia", "Tirana", "Oslo", "Reykjavik Keflavik"]:
            self.assertEqual(destination_priority(city), 2, city)
        self.assertEqual(destination_priority("Rome"), 3)
        self.assertTrue(is_high_value_destination("Kutaisi"))
        self.assertTrue(is_high_value_route("Budapest", "Kutaisi"))
        self.assertFalse(is_high_value_route("Budapest", "Rome"))

    def test_all_mode_selected_destinations_become_priority_zero(self):
        scope = {"destination_mode": "all", "destinations": ["Rome"]}
        self.assertEqual(destination_priority("Rome", scope), 0)
        self.assertEqual(route_priority("Budapest", "Rome", scope), 0)
        self.assertEqual(destination_priority("Athens", scope), 3)

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

    def test_unreachable_hub_cannot_create_connection(self):
        pairs = [("Liverpool", "Rome"), ("Warsaw", "Tirana")]
        scope = {"origins": ["Liverpool"], "destination_mode": "all", "destinations": [], "connection_hubs": ["Warsaw"]}
        primary, hubs = expand_scan_routes(pairs, scope)
        self.assertEqual(primary, [("Liverpool", "Rome")])
        self.assertEqual(hubs, [])

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

    def test_preferred_inbound_only_routes_add_bounded_two_way_topology(self):
        pairs = [
            ("Nice", "Liverpool"),
            ("Budapest", "Liverpool"),
            ("Nice", "Budapest"),
            ("Budapest", "Athens"),
            ("Warsaw", "Liverpool"),
            ("Nice", "Warsaw"),
        ]
        scope = {
            "origins": ["Liverpool"],
            "destination_mode": "only",
            "destinations": ["Rome"],
            "preferred_destinations": ["Nice"],
            "connection_hubs": ["Budapest"],
        }
        primary, hubs = expand_scan_routes(pairs, scope)
        self.assertEqual(primary, [("Budapest", "Liverpool"), ("Liverpool", "Budapest"), ("Liverpool", "Nice"), ("Nice", "Liverpool")])
        self.assertEqual(hubs, [("Budapest", "Nice"), ("Nice", "Budapest")])

    def test_preferred_outbound_connection_keeps_both_published_directions(self):
        pairs = [
            ("Liverpool", "Budapest"),
            ("Budapest", "Liverpool"),
            ("Budapest", "Nice"),
            ("Nice", "Budapest"),
        ]
        scope = {
            "origins": ["Liverpool"],
            "destination_mode": "only",
            "destinations": [],
            "preferred_destinations": ["Nice"],
            "connection_hubs": ["Budapest"],
        }
        primary, hubs = expand_scan_routes(pairs, scope)
        self.assertEqual(primary, [("Budapest", "Liverpool"), ("Liverpool", "Budapest")])
        self.assertEqual(hubs, [("Budapest", "Nice"), ("Nice", "Budapest")])

    def test_preferred_hub_edge_scans_both_directions_from_one_pdf_leg(self):
        pairs = [
            ("Liverpool", "Budapest"),
            ("Budapest", "Baku"),
        ]
        scope = {
            "origins": ["Liverpool"],
            "destination_mode": "only",
            "destinations": [],
            "preferred_destinations": ["Baku"],
            "connection_hubs": ["Budapest"],
        }
        primary, hubs = expand_scan_routes(pairs, scope)
        routes = set(primary + hubs)
        self.assertIn(("Liverpool", "Budapest"), routes)
        self.assertIn(("Budapest", "Liverpool"), routes)
        self.assertIn(("Budapest", "Baku"), routes)
        self.assertIn(("Baku", "Budapest"), routes)

    def test_active_watch_direction_is_scanned_when_opposite_pdf_leg_exists(self):
        pairs = [("Budapest", "Baku"), ("Paris", "Rome")]
        scope = {
            "origins": ["Liverpool"],
            "destination_mode": "only",
            "destinations": [],
            "preferred_destinations": [],
            "watch_routes": [("Baku", "Budapest")],
            "connection_hubs": [],
        }
        primary, hubs = expand_scan_routes(pairs, scope)
        self.assertIn(("Baku", "Budapest"), primary)
        self.assertNotIn(("Rome", "Paris"), primary + hubs)

    def test_watch_routes_change_scan_fingerprint(self):
        base = {
            "origins": ["Liverpool"],
            "destination_mode": "all",
            "destinations": [],
            "connection_hubs": ["Budapest"],
        }
        watched = {**base, "watch_routes": [("Baku", "Budapest")]}
        self.assertNotEqual(scope_fingerprint(base), scope_fingerprint(watched))

    def test_preferences_change_priority_and_scan_fingerprint(self):
        base = {
            "origins": ["Liverpool"],
            "destination_mode": "all",
            "destinations": [],
            "connection_hubs": ["Budapest"],
        }
        preferred = {**base, "preferred_destinations": ["Nice"]}
        self.assertEqual(destination_priority("Nice", preferred), 0)
        self.assertNotEqual(scope_fingerprint(base), scope_fingerprint(preferred))

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
                saved = save_scope(["Liverpool", "Liverpool", "Birmingham"], "all", ["Rome", "Tirana"], ["Warsaw"])
                loaded = load_scope()
                self.assertEqual(saved, loaded)
                self.assertEqual(loaded["origins"], ["Liverpool", "Birmingham"])
                self.assertEqual(loaded["destinations"], ["Rome", "Tirana"])
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
