import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from wizz_sitemap import load_sitemap_routes, merge_route_pairs, parse_city_to_city_html


class WizzSitemapTests(unittest.TestCase):
    def test_parses_route_links_and_pagination(self):
        html = """
        <a href="/en-gb/cheap-flights-from-copenhagen-to-bucharest">Copenhagen to Bucharest</a>
        <a href="https://www.wizzair.com/en-gb/cheap-flights-from-ni%C5%A1-to-london">Nis to London</a>
        <a href="/en-gb/sitemap/city-to-city-flights/page-7">7</a>
        """
        routes, max_page = parse_city_to_city_html(html)
        self.assertEqual(max_page, 7)
        self.assertIn(("Copenhagen", "Bucharest"), routes)
        self.assertIn(("Niš", "London"), routes)

    def test_merge_preserves_pdf_city_spelling_and_adds_network_routes(self):
        pdf = [("Niš", "London"), ("London", "Budapest")]
        sitemap = [("Nis", "London"), ("London", "Barcelona")]
        merged = merge_route_pairs(pdf, sitemap)
        self.assertIn(("Niš", "London"), merged)
        self.assertIn(("London", "Barcelona"), merged)
        self.assertEqual(sum(1 for route in merged if route[0] == "Niš" and route[1] == "London"), 1)

    def test_fresh_cache_avoids_network_request(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "wizz-sitemap" / "catalog.json"
            path.parent.mkdir(parents=True)
            path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "fetched_at": int(time.time()),
                        "route_count": 2,
                        "digest": "abc123",
                        "routes": [["London", "Barcelona"], ["Barcelona", "London"]],
                    }
                ),
                encoding="utf-8",
            )
            with patch("wizz_sitemap.requests.Session.get") as get:
                routes, info = load_sitemap_routes(root)
            get.assert_not_called()
            self.assertEqual(len(routes), 2)
            self.assertTrue(info["cache_hit"])
            self.assertFalse(info["stale"])

    def test_can_be_disabled_without_touching_network(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.dict(os.environ, {"AYCF_INCLUDE_WIZZ_SITEMAP": "false"}, clear=False):
                with patch("wizz_sitemap.requests.Session.get") as get:
                    routes, info = load_sitemap_routes(root)
            get.assert_not_called()
            self.assertEqual(routes, [])
            self.assertFalse(info["enabled"])


if __name__ == "__main__":
    unittest.main()
