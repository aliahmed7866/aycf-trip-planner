"""Bounded parallel AYCF route fetches with shared global request pacing."""

from __future__ import annotations

import threading
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from scanner import WizzAvailabilityUnknown


def fetch_group(client, requests, day):
    """Attempt every concrete airport; retain verified flights if another is unknown."""
    flights, checked, unknown = [], [], []
    for a, b in requests:
        try:
            rows = client.check(a, b, day)
        except WizzAvailabilityUnknown as exc:
            unknown.append(str(exc))
            continue
        flights.extend(rows)
        checked.append((a, b))
    return flights, checked, unknown


class GlobalStartLimiter:
    def __init__(self, interval_seconds: float = 1.0):
        self.interval = max(0.2, float(interval_seconds))
        self._lock = threading.Lock()
        self._next = 0.0
        self._stopped = threading.Event()
        self.rate_limits = 0

    def cooldown(self, seconds):
        with self._lock:
            self.rate_limits += 1
            self.interval = max(self.interval, min(30.0, self.interval * 2))
            delay = max(seconds, self.interval)
            self._next = max(self._next, time.monotonic() + delay)
            print(f"[AYCF] Rate limit #{self.rate_limits}: all workers paused for at least {delay:.1f}s; request spacing now {self.interval:.2f}s.", flush=True)

    def stop(self):
        self._stopped.set()

    def status(self):
        with self._lock:
            return {"rate_limit_responses": self.rate_limits, "effective_request_interval": self.interval,
                    "cooldown_remaining_seconds": max(0.0, self._next - time.monotonic())}

    def wait(self):
        # Recheck after waking: another worker may have extended the cooldown.
        # Allocate only the start that is actually due, never future slots.
        while True:
            with self._lock:
                if self._stopped.is_set():
                    raise CancelledError("AYCF scan stopped")
                now = time.monotonic()
                wait = self._next - now
                if wait <= 0:
                    self._next = now + self.interval
                    return
            self._stopped.wait(wait)


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
            client._rate_limit_cooldown = self.limiter.cooldown
            self._local.client = client
        return client

    def _job(self, item):
        tier, origin, destination, day, origin_variants, destination_variants = item[:6]
        requests = item[6] if len(item) > 6 else [(a, b) for a in origin_variants for b in destination_variants if a != b]
        client = self._client()
        before = (
            client.live_requests,
            client.no_availability_responses,
            client.wallet_redirects,
            client.html_retries,
        )
        rows, checked, unknown = fetch_group(client, requests, day)
        flights = list({(f.flight_code, f.departure, f.arrival, f.origin, f.destination): f for f in rows}.values())
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
            "checked_pairs": checked,
            "unknown": unknown,
            "live_requests": after[0] - before[0],
            "no_availability": after[1] - before[1],
            "wallet_redirects": after[2] - before[2],
            "html_retries": after[3] - before[3],
        }

    def run(self, items, on_result, on_unknown=None):
        items = list(items)
        if not items:
            return
        with ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="aycf") as pool:
            futures = {pool.submit(self._job, item): item for item in items}
            try:
                for future in as_completed(futures):
                    try:
                        result = future.result()
                    except WizzAvailabilityUnknown as exc:
                        if on_unknown is None:
                            raise
                        on_unknown(futures[future], exc)
                        continue
                    on_result(result)
            except BaseException:
                self.limiter.stop()
                for future in futures:
                    future.cancel()
                raise
