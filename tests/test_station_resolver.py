import os
import unittest
from unittest.mock import patch

from station_resolver import prepare_required_stations


class DummyClient:
    def __init__(self):
        self.station_ids = {}


class StationResolverTests(unittest.TestCase):
    def test_current_pdf_station_aliases_resolve_without_network(self):
        client = DummyClient()
        with patch.dict(os.environ, {"AYCF_DISABLE_PUBLIC_STATION_MAP": "true"}, clear=False):
            report = prepare_required_stations(
                client,
                ["Barcelona", "Giza", "Zakinthos Island"],
            )
        self.assertEqual([], report["unresolved"])
        self.assertEqual("BCN", client.station_ids["barcelona"])
        self.assertEqual("SPX", client.station_ids["giza"])
        self.assertEqual("ZTH", client.station_ids["zakinthos island"])

    def test_zakynthos_common_spelling_also_resolves(self):
        client = DummyClient()
        with patch.dict(os.environ, {"AYCF_DISABLE_PUBLIC_STATION_MAP": "true"}, clear=False):
            report = prepare_required_stations(client, ["Zakynthos Island"])
        self.assertEqual([], report["unresolved"])
        self.assertEqual("ZTH", client.station_ids["zakynthos island"])


if __name__ == "__main__":
    unittest.main()
