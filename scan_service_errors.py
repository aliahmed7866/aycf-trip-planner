"""Bound retries across route checks without turning failures into empty results."""
from collections import deque
import threading

import requests


def is_service_error(exc):
    if not isinstance(exc, requests.HTTPError) or exc.response is None:
        return False
    if 500 <= exc.response.status_code < 600:
        return True
    if exc.response.status_code != 400:
        return False
    try:
        error = exc.response.json()
    except (ValueError, TypeError):
        return False
    # A specific Wizz backend search exception, not arbitrary malformed requests.
    return (isinstance(error, dict) and error.get('code') == 'PASS-0000'
            and error.get('key') == 'wallet.error.generic'
            and str(error.get('message', '')).startswith('cyf.flights.FlightSearchException:'))


def route_service_error(exc, origin, destination, day):
    # Deliberately omit captured URLs, request headers and response bodies.
    return requests.HTTPError(
        f'HTTP {exc.response.status_code} for {origin} -> {destination} on {day}; '
        'availability remains pending after request retries.', response=exc.response)


class ServiceFailureTracker:
    """Shared by workers; counts exhausted logical checks, not HTTP attempts."""

    def __init__(self):
        self._lock = threading.Lock()
        self._recent = deque(maxlen=10)
        self._consecutive = 0
        self._outage = None

    def _raise_if_paused(self):
        if self._outage is not None:
            # Each worker needs its own exception/partial-result attributes.
            raise requests.HTTPError(str(self._outage), response=self._outage.response)

    def check(self):
        with self._lock:
            self._raise_if_paused()

    def success(self):
        with self._lock:
            self._recent.append(False)
            self._consecutive = 0

    def failure(self, exc):
        with self._lock:
            self._recent.append(True)
            self._consecutive += 1
            if self._consecutive >= 3 or sum(self._recent) >= 5:
                self._outage = requests.HTTPError(
                    f'Pausing scan after {self._consecutive} consecutive server failures '
                    f'or {sum(self._recent)} in the last {len(self._recent)} checks. {exc}',
                    response=exc.response)
            self._raise_if_paused()
