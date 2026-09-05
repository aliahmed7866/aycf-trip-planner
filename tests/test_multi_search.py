import hashlib
import unittest
from unittest.mock import patch

import pandas as pd

from termux import multi_search


class MultiSearchTests(unittest.TestCase):
    def test_scope_readiness_uses_same_enriched_identity_as_morning_scan(self):
        generated = "2026-09-05T07:00:00+01:00"
        frame = pd.DataFrame([
            {
                "departure_from": "Liverpool",
                "departure_to": "Kutaisi",
                "data_generated": generated,
            }
        ])

        class FakeGraph:
            def latest_frame(self):
                return frame

        class FakeDB:
            requested_run_id = None

            def get_pdf_run(self, run_id):
                self.requested_run_id = run_id
                return {"scanned_at": "2026-09-05T08:00:00+01:00"}

        base_scope = {
            "origins": ["Liverpool"],
            "destination_mode": "all",
            "destinations": [],
            "connection_hubs": [],
        }
        enriched_scope = {
            **base_scope,
            "preferred_destinations": ["Kutaisi"],
            "watch_routes": [("Kutaisi", "Budapest")],
        }
        database = FakeDB()

        with patch.object(multi_search, "load_scope", return_value=base_scope), patch.object(
            multi_search,
            "scan_scope_with_preferences",
            return_value=enriched_scope,
        ) as enrich:
            context = multi_search._current_scope_run(FakeGraph(), database)

        routes = multi_search.scan_plan(
            [("Liverpool", "Kutaisi")],
            enriched_scope,
            days=4,
        )["routes"]
        expected_scope_id = multi_search.scope_fingerprint(enriched_scope)
        expected_run_id = hashlib.sha256(
            (
                generated
                + "\n"
                + expected_scope_id
                + "\n"
                + "\n".join(f"{origin}>{destination}" for origin, destination in routes)
            ).encode()
        ).hexdigest()[:20]

        enrich.assert_called_once_with(base_scope)
        self.assertEqual(context["scope"], enriched_scope)
        self.assertEqual(context["run_id"], expected_run_id)
        self.assertEqual(database.requested_run_id, expected_run_id)
        self.assertTrue(context["ready"])

    def test_approved_connections_keeps_direct_and_selected_hubs_only(self):
        items = [
            {"path": ["London", "Budapest"]},
            {"path": ["London", "Rome", "Tirana"]},
            {"path": ["London", "Paris", "Tirana"]},
        ]
        scope = {"connection_hubs": ["Rome"]}
        result = multi_search._approved_connections(items, scope)
        self.assertEqual(
            [item["path"] for item in result],
            [["London", "Budapest"], ["London", "Rome", "Tirana"]],
        )

    def test_decorate_prefers_direct_then_safe_connections(self):
        items = [
            {
                "path": ["London", "Rome", "Tirana"],
                "connection_minutes_list": [130],
                "legs": [
                    {"origin": "London", "destination": "Rome", "flight_code": "W1", "departure": "2026-08-29T06:00:00", "arrival": "2026-08-29T08:00:00"},
                    {"origin": "Rome", "destination": "Tirana", "flight_code": "W2", "departure": "2026-08-29T10:10:00", "arrival": "2026-08-29T11:30:00"},
                ],
            },
            {
                "path": ["London", "Budapest"],
                "connection_minutes_list": [],
                "legs": [
                    {"origin": "London", "destination": "Budapest", "flight_code": "W3", "departure": "2026-08-29T09:00:00", "arrival": "2026-08-29T12:20:00"},
                ],
            },
            {
                "path": ["London", "Rome", "Bucharest"],
                "connection_minutes_list": [180],
                "legs": [
                    {"origin": "London", "destination": "Rome", "flight_code": "W4", "departure": "2026-08-29T05:00:00", "arrival": "2026-08-29T07:00:00"},
                    {"origin": "Rome", "destination": "Bucharest", "flight_code": "W5", "departure": "2026-08-29T10:00:00", "arrival": "2026-08-29T12:00:00"},
                ],
            },
        ]
        result = multi_search._decorate(items)
        self.assertEqual(result[0]["destination"], "Budapest")
        self.assertEqual(result[1]["destination"], "Bucharest")
        self.assertEqual(result[2]["destination"], "Tirana")
        self.assertFalse(result[0]["risky_connection"])
        self.assertFalse(result[1]["risky_connection"])
        self.assertTrue(result[2]["risky_connection"])

    def test_append_unique_deduplicates_identical_itinerary(self):
        itinerary = {
            "legs": [
                {"flight_code": "W1", "departure": "2026-08-29T06:00:00", "arrival": "2026-08-29T08:00:00"}
            ]
        }
        target, seen = [], set()
        multi_search._append_unique(target, seen, [itinerary, dict(itinerary)])
        self.assertEqual(len(target), 1)

    def test_decorate_respects_max_journey(self):
        items = [{
            "path": ["London", "Budapest"],
            "connection_minutes_list": [],
            "legs": [
                {"origin": "London", "destination": "Budapest", "flight_code": "W1", "departure": "2026-08-29T06:00:00", "arrival": "2026-08-29T14:30:00"}
            ],
        }]
        self.assertEqual(multi_search._decorate(items, max_journey_minutes=480), [])
        self.assertEqual(len(multi_search._decorate(items, max_journey_minutes=540)), 1)


if __name__ == "__main__":
    unittest.main()
