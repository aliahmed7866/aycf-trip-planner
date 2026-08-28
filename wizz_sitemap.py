"""Supplemental Wizz city-to-city network catalog ingestion.

The AYCF PDF remains the authoritative point-in-time availability snapshot. This
module ingests Wizz's public city-to-city sitemap as a broader route universe so
scoped live scans can also discover pass seats that appear after the PDF was
published.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import unicodedata
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit, urlunsplit

import requests

DEFAULT_SITEMAP_URL = "https://www.wizzair.com/en-gb/sitemap/city-to-city-flights/page-1"
_ROUTE_PATH = re.compile(r"/cheap-flights-from-(?P<origin>.+)-to-(?P<destination>.+?)/?$", re.I)
_PAGE_PATH = re.compile(r"/sitemap/city-to-city-flights/page-(?P<page>\d+)/?$", re.I)


class _LinkCollector(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        if str(tag).casefold() != "a":
            return
        for key, value in attrs:
            if str(key).casefold() == "href" and value:
                self.hrefs.append(str(value))
                return


def _normalize(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").strip().casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _slug_label(value: str) -> str:
    text = unquote(str(value or "")).replace("-", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return " ".join(part[:1].upper() + part[1:] for part in text.split(" ") if part)


def parse_city_to_city_html(html: str) -> tuple[list[tuple[str, str]], int]:
    """Extract route pairs and the highest linked sitemap page from one page."""
    parser = _LinkCollector()
    parser.feed(str(html or ""))
    pairs: set[tuple[str, str]] = set()
    max_page = 1
    for href in parser.hrefs:
        path = urlsplit(href).path
        page_match = _PAGE_PATH.search(path)
        if page_match:
            max_page = max(max_page, int(page_match.group("page")))
        route_match = _ROUTE_PATH.search(path)
        if not route_match:
            continue
        origin = _slug_label(route_match.group("origin"))
        destination = _slug_label(route_match.group("destination"))
        if origin and destination and _normalize(origin) != _normalize(destination):
            pairs.add((origin, destination))
    return sorted(pairs), max_page


def merge_route_pairs(pdf_pairs, sitemap_pairs) -> list[tuple[str, str]]:
    """Union route sources while preserving Wizz PDF city spelling when known."""
    pdf_pairs = [(str(a).strip(), str(b).strip()) for a, b in pdf_pairs if str(a).strip() and str(b).strip()]
    labels: dict[str, str] = {}
    for origin, destination in pdf_pairs:
        labels.setdefault(_normalize(origin), origin)
        labels.setdefault(_normalize(destination), destination)

    merged: dict[tuple[str, str], tuple[str, str]] = {}
    for origin, destination in list(pdf_pairs) + list(sitemap_pairs):
        origin = str(origin or "").strip()
        destination = str(destination or "").strip()
        origin_key, destination_key = _normalize(origin), _normalize(destination)
        if not origin_key or not destination_key or origin_key == destination_key:
            continue
        merged[(origin_key, destination_key)] = (
            labels.get(origin_key, origin),
            labels.get(destination_key, destination),
        )
    return sorted(merged.values(), key=lambda pair: (_normalize(pair[0]), _normalize(pair[1])))


def _cache_path(cache_root: str) -> Path:
    return Path(cache_root) / "wizz-sitemap" / "catalog.json"


def _read_cache(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("routes"), list):
        return None
    return payload


def _write_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp.replace(path)


def _candidate_urls(url: str) -> list[str]:
    candidates = [url]
    parsed = urlsplit(url)
    if parsed.hostname in {"www.wizzair.com", "wizzair.com"}:
        for host in ("ssr-weu2.wizzair.com", "ssr-weu.wizzair.com"):
            candidates.append(urlunsplit((parsed.scheme or "https", host, parsed.path, parsed.query, parsed.fragment)))
    out = []
    for item in candidates:
        if item not in out:
            out.append(item)
    return out


def _page_url(seed_url: str, page: int) -> str:
    parsed = urlsplit(seed_url)
    path = re.sub(r"/page-\d+/?$", f"/page-{int(page)}", parsed.path)
    return urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment))


def _fetch_html(session: requests.Session, url: str, timeout: int) -> tuple[str, str]:
    last_error: Exception | None = None
    for candidate in _candidate_urls(url):
        try:
            response = session.get(candidate, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
            text = str(response.text or "")
            if "cheap-flights-from-" not in text and "city-to-city-flights" not in text:
                raise RuntimeError(f"Wizz sitemap response did not contain expected route links: {candidate}")
            return text, str(response.url or candidate)
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Could not fetch Wizz city-to-city sitemap: {last_error}")


def _route_digest(routes) -> str:
    raw = "\n".join(f"{_normalize(a)}>{_normalize(b)}" for a, b in routes)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def load_sitemap_routes(cache_root: str, force: bool = False) -> tuple[list[tuple[str, str]], dict]:
    """Return cached/refreshed Wizz network routes without making scans brittle.

    A stale cached catalog is preferred over failing the AYCF scan. If Wizz's
    public sitemap is unreachable and no cache exists, callers receive an empty
    supplemental route list and continue with the PDF-only graph.
    """
    if os.environ.get("AYCF_INCLUDE_WIZZ_SITEMAP", "true").lower() == "false":
        return [], {"enabled": False, "route_count": 0, "stale": False}

    seed_url = os.environ.get("WIZZ_CITY_SITEMAP_URL", DEFAULT_SITEMAP_URL)
    ttl = max(300, min(604800, int(os.environ.get("WIZZ_SITEMAP_TTL_SECONDS", "86400"))))
    max_pages = max(1, min(100, int(os.environ.get("WIZZ_SITEMAP_MAX_PAGES", "50"))))
    timeout = max(5, min(60, int(os.environ.get("WIZZ_SITEMAP_TIMEOUT_SECONDS", "20"))))
    path = _cache_path(cache_root)
    cached = _read_cache(path)
    now = int(time.time())

    if cached and not force:
        fetched_at = int(cached.get("fetched_at") or 0)
        if fetched_at and now - fetched_at <= ttl:
            routes = [tuple(row[:2]) for row in cached.get("routes") or [] if isinstance(row, list) and len(row) >= 2]
            info = dict(cached)
            info.update({"enabled": True, "stale": False, "cache_hit": True, "route_count": len(routes)})
            return sorted(set(routes)), info

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": os.environ.get(
                "WIZZ_USER_AGENT",
                "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.8",
            "Referer": "https://www.wizzair.com/",
        }
    )

    try:
        html, resolved_seed = _fetch_html(session, seed_url, timeout)
        routes, discovered_max = parse_city_to_city_html(html)
        route_set = set(routes)
        page = 2
        target_max = min(max_pages, max(1, discovered_max))
        while page <= target_max:
            page_html, _ = _fetch_html(session, _page_url(resolved_seed, page), timeout)
            page_routes, page_max = parse_city_to_city_html(page_html)
            route_set.update(page_routes)
            target_max = min(max_pages, max(target_max, page_max))
            page += 1
        routes = sorted(route_set)
        if not routes:
            raise RuntimeError("Wizz city-to-city sitemap contained no route links")
        payload = {
            "version": 1,
            "source_url": seed_url,
            "resolved_url": resolved_seed,
            "fetched_at": now,
            "page_count": target_max,
            "route_count": len(routes),
            "digest": _route_digest(routes),
            "routes": [[a, b] for a, b in routes],
        }
        _write_cache(path, payload)
        info = dict(payload)
        info.update({"enabled": True, "stale": False, "cache_hit": False})
        return routes, info
    except Exception as exc:
        if cached:
            routes = [tuple(row[:2]) for row in cached.get("routes") or [] if isinstance(row, list) and len(row) >= 2]
            info = dict(cached)
            info.update({"enabled": True, "stale": True, "cache_hit": True, "route_count": len(routes), "error": str(exc)})
            return sorted(set(routes)), info
        return [], {"enabled": True, "stale": True, "cache_hit": False, "route_count": 0, "error": str(exc), "source_url": seed_url}
