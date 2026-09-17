"""Bounded parallel AYCF route fetches with shared global request pacing."""

from __future__ import annotations

import threading
import time
import requests as http_requests
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from scanner import WizzAvailabilityUnknown, WizzSessionExpired, WizzIntegrationChanged, WizzRequestRejected
import wizz_rate_limit
from scan_observability import log as print

PRESERVABLE_FAILURES = (wizz_rate_limit.WizzRateLimited, WizzSessionExpired,
                       WizzIntegrationChanged, WizzRequestRejected, http_requests.RequestException)


def fetch_group(client, requests, day):
    """Attempt every concrete airport; retain verified flights if another is unknown."""
    flights, checked, unknown = [], [], []
    for a, b in requests:
        try:
            rows = client.check(a, b, day)
        except WizzAvailabilityUnknown as exc:
            unknown.append(str(exc))
            continue
        except PRESERVABLE_FAILURES as exc:
            exc.partial_group = (flights, checked, unknown + [
                'Remaining airport checks are pending after an interrupted request.'])
            raise
        flights.extend(rows)
        checked.append((a, b))
    return flights, checked, unknown


class GlobalStartLimiter:
    def __init__(self, interval_seconds: float = 1.0):
        self._configured_interval = max(0.2, float(interval_seconds))
        self.interval = max(self._configured_interval, wizz_rate_limit.rate_limit_status()['effective_request_interval'])
        self._lock = threading.Lock()
        self._next = 0.0
        self._stopped = threading.Event()
        self.rate_limits = 0

    def cooldown(self, seconds):
        retained = wizz_rate_limit.rate_limit_status()
        with self._lock:
            self.rate_limits += 1
            self.interval = max(self._configured_interval, retained['effective_request_interval'])
            print(f"[AYCF] {wizz_rate_limit.rate_limit_message(retained)} Request spacing: {self.interval:.2f}s.", flush=True)

    def stop(self):
        self._stopped.set()

    def status(self):
        retained = wizz_rate_limit.rate_limit_status()
        with self._lock:
            self.interval = max(self._configured_interval, retained['effective_request_interval'])
            return {"rate_limit_responses": self.rate_limits, "effective_request_interval": self.interval,
                    "cooldown_remaining_seconds": max(0.0, retained['cooldown_until'] - time.time()),
                    "request_budget": wizz_rate_limit.request_budget_status()}

    def wait(self):
        # Recheck after waking: another worker may have extended the cooldown.
        # Allocate only the start that is actually due, never future slots.
        while True:
            if self._stopped.is_set():
                raise CancelledError("AYCF scan stopped")
            retained = wizz_rate_limit.check_cooldown()
            with self._lock:
                if self._stopped.is_set():
                    raise CancelledError("AYCF scan stopped")
                now = time.monotonic()
                self.interval = max(self._configured_interval, retained['effective_request_interval'])
                wait = self._next - now
                if wait <= 0:
                    self._next = now + self.interval
                    return
            self._stopped.wait(min(1.0, wait))


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
            with self.limiter._lock:
                self.limiter._configured_interval = max(
                    self.limiter._configured_interval, float(getattr(client, 'min_delay', 0)))
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
        failure = None
        try:
            rows, checked, unknown = fetch_group(client, requests, day)
        except PRESERVABLE_FAILURES as exc:
            failure = exc
            rows, checked, unknown = exc.partial_group
        flights = list({(f.flight_code, f.departure, f.arrival, f.origin, f.destination): f for f in rows}.values())
        flights.sort(key=lambda f: f.departure)
        after = (
            client.live_requests,
            client.no_availability_responses,
            client.wallet_redirects,
            client.html_retries,
        )
        result = {
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
        if failure is not None:
            if checked:
                failure.partial_result = result
            raise failure
        return result

    def run(self, items, on_result, on_unknown=None):
        items = list(items)
        if not items:
            return
        with ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="aycf") as pool:
            futures = {pool.submit(self._job, item): item for item in items}
            handled = set()
            try:
                for future in as_completed(futures):
                    handled.add(future)
                    try:
                        result = future.result()
                    except PRESERVABLE_FAILURES as exc:
                        partial = getattr(exc, 'partial_result', None)
                        if partial:
                            on_result(partial)
                        raise
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
                # Already-started calls may complete successfully while another
                # worker reports 429. Settle them before draining, then preserve
                # each verified result exactly once, without marking cancelled
                # or untouched groups complete.
                pool.shutdown(wait=True, cancel_futures=True)
                for future in futures:
                    if future in handled or future.cancelled():
                        continue
                    handled.add(future)
                    try:
                        result = future.result()
                    except PRESERVABLE_FAILURES as exc:
                        result = getattr(exc, 'partial_result', None)
                    except Exception:
                        continue
                    if result:
                        on_result(result)
                raise
