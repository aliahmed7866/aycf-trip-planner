"""Bounded parallel AYCF route fetches with shared global request pacing."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace


class GlobalStartLimiter:
    def __init__(self, interval_seconds: float = 1.0):
        self.interval = max(0.2, float(interval_seconds))
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next - now)
            slot = max(now, self._next)
            self._next = slot + self.interval
        if wait > 0:
            time.sleep(wait)


class ParallelFetcher:
    """One client per worker thread; coordinator owns persistence/progress."""

    def __init__(self, client_factory, workers: int = 3, start_interval: float = 1.0):
        self.workers = max(1, min(5, int(workers)))
        self.client_factory = client_factory
        self.limiter = GlobalStartLimiter(start_interval)
        self._local = threading.local()

    def _client(self):
        client = getattr(self._local, "client", None)
        if client is None:
            client = self.client_factory()
            client._throttle = self.limiter.wait
            self._local.client = client
        return client

    def _job(self, item):
        tier, origin, destination, day, origin_variants, destination_variants = item
        client = self._client()
        before = (
            client.live_requests,
            client.no_availability_responses,
            client.wallet_redirects,
            client.html_retries,
        )
        flights = []
        seen = set()
        for concrete_origin in origin_variants:
            for concrete_destination in destination_variants:
                for flight in client.check(concrete_origin, concrete_destination, day):
                    key = (flight.flight_code, flight.departure, flight.arrival)
                    if key in seen:
                        continue
                    seen.add(key)
                    # Persist against the logical PDF route so grouped labels
                    # such as London can be read back by the route graph.
                    flights.append(replace(flight, origin=origin, destination=destination))
        flights.sort(key=lambda f: f.departure)
        after = (
            client.live_requests,
            client.no_availability_responses,
            client.wallet_redirects,
            client.html_retries,
        )
        return {
            "tier": tier,
            "origin": origin,
            "destination": destination,
            "day": day,
            "origin_variants": origin_variants,
            "destination_variants": destination_variants,
            "variants": origin_variants,
            "flights": flights,
            "live_requests": after[0] - before[0],
            "no_availability": after[1] - before[1],
            "wallet_redirects": after[2] - before[2],
            "html_retries": after[3] - before[3],
        }

    def run(self, items, on_result):
        items = list(items)
        if not items:
            return
        with ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="aycf") as pool:
            futures = {pool.submit(self._job, item): item for item in items}
            try:
                for future in as_completed(futures):
                    on_result(future.result())
            except Exception:
                for future in futures:
                    future.cancel()
                raise
