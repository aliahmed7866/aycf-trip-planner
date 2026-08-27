"""Bounded parallel AYCF route fetches with shared global request pacing."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


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
            # Replace per-client pacing with one shared global start limiter.
            client._throttle = self.limiter.wait
            self._local.client = client
        return client

    def _job(self, item):
        tier, origin, destination, day, variants = item
        client = self._client()
        before = (
            client.live_requests,
            client.no_availability_responses,
            client.wallet_redirects,
            client.html_retries,
        )
        flights = []
        for concrete_origin in variants:
            flights.extend(client.check(concrete_origin, destination, day))
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
            "variants": variants,
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
