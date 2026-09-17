"""Directed airport routes captured from the authenticated Multipass route menu.

This narrows PDF-backed requests; it never supplies AYCF availability.
"""
import json
import os
import re
import time
from pathlib import Path

MAX_AGE_SECONDS = 7 * 86400
REFRESH_AFTER_SECONDS = 86400


def directory_path():
    return Path(os.environ.get('AYCF_CONFIG_DIR', str(Path.home() / '.config/aycf'))) / 'airport_routes.json'


def _code(station):
    if not isinstance(station, dict):
        return None
    value = str(station.get('iata') or station.get('iataCode') or station.get('id') or '').upper()
    return value if re.fullmatch('[A-Z]{3}', value) else None


def parse_directory(rows):
    routes, invalid = {}, set()
    if not isinstance(rows, list):
        return routes
    for row in rows:
        if not isinstance(row, dict):
            continue
        origin = _code(row.get('departureStation'))
        arrivals = row.get('arrivalStations')
        if not origin:
            continue
        # Missing/empty/lazy-loaded rows cannot establish excluded destinations.
        if not isinstance(arrivals, list) or not arrivals or any(_code(s) is None for s in arrivals):
            invalid.add(origin)
            continue
        routes.setdefault(origin, set()).update(_code(s) for s in arrivals if _code(s) != origin)
    return {a: sorted(bs) for a, bs in sorted(routes.items()) if a not in invalid and bs}


def save_directory(rows):
    from termux.wizz_runtime import write_runtime
    routes = parse_directory(rows)
    if not routes:
        return False
    write_runtime(directory_path(), {'source': 'Multipass CVO.routes', 'captured_at': int(time.time()), 'routes': routes})
    return True


def load_directory(now=None):
    try:
        data = json.loads(directory_path().read_text())
        age = (time.time() if now is None else now) - float(data['captured_at'])
        routes = data['routes']
        if data.get('source') != 'Multipass CVO.routes' or not 0 <= age <= MAX_AGE_SECONDS or not isinstance(routes, dict) or not routes:
            return {}
        for a, bs in routes.items():
            if not re.fullmatch('[A-Z]{3}', a) or not isinstance(bs, list) or not bs or any(not isinstance(b, str) or not re.fullmatch('[A-Z]{3}', b) for b in bs):
                return {}
        return data
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return {}


def pair_listed(origin_code, destination_code, directory):
    routes = directory.get('routes', {})
    # No evidence for an origin must not silently remove its routes.
    return not origin_code or not destination_code or origin_code not in routes or destination_code in routes[origin_code]


def directory_status():
    data = load_directory()
    if not data:
        return {'state': 'missing_or_expired', 'age_seconds': None, 'refresh_due': True,
                'message': 'Airport directory missing or expired; uncovered airports retain fallback checks.'}
    age = max(0, int(time.time() - data['captured_at']))
    due = age >= REFRESH_AFTER_SECONDS
    return {'state': 'refresh_due' if due else 'current', 'captured_at': data['captured_at'],
            'age_seconds': age, 'refresh_due': due, 'departure_airports': len(data['routes']),
            'message': f"Airport directory: {age // 3600} hours old." +
                (' Refresh due at a scheduled session health check.' if due else '')}


def capture_from_html(html):
    """Parse only the provider's JSON route assignment; never execute scripts."""
    if not isinstance(html, str):
        return False
    match = re.search(r'(?:window\.)?CVO\.routes\s*=\s*', html)
    if not match:
        return False
    try:
        rows, _ = json.JSONDecoder().raw_decode(html[match.end():].lstrip())
        return save_directory(rows)
    except (ValueError, TypeError, OSError):
        return False


def refresh_if_due(client):
    """One paced wallet request during scheduled health work; keep old data on failure."""
    from scanner import PRIVATE_PAGE, WizzSessionExpired
    import requests
    if not directory_status()['refresh_due']:
        return False
    try:
        response = client._request('GET', PRIVATE_PAGE, allow_redirects=False)
        return response.status_code == 200 and capture_from_html(response.text)
    except (requests.RequestException, WizzSessionExpired):
        # Optional topology maintenance cannot invalidate verified availability.
        return False
