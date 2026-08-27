import threading
import time
import unittest

from parallel_fetch import GlobalStartLimiter, ParallelFetcher
from scan_scope import default_scope, scope_fingerprint


class FakeClient:
    def __init__(self):
        self.live_requests = 0
        self.no_availability_responses = 0
        self.wallet_redirects = 0
        self.html_retries = 0
        self.starts = []
        self._throttle = lambda: None

    def check(self, origin, destination, day):
        self._throttle()
        self.live_requests += 1
        self.starts.append(time.monotonic())
        time.sleep(0.02)
        return []


class ParallelFetchTests(unittest.TestCase):
    def test_worker_count_is_bounded(self):
        self.assertEqual(ParallelFetcher(FakeClient, workers=0).workers, 1)
        self.assertEqual(ParallelFetcher(FakeClient, workers=99).workers, 5)

    def test_global_limiter_spaces_concurrent_starts(self):
        limiter = GlobalStartLimiter(0.05)
        starts = []
        lock = threading.Lock()

        def run():
            limiter.wait()
            with lock:
                starts.append(time.monotonic())

        threads = [threading.Thread(target=run) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        starts.sort()
        self.assertGreaterEqual(starts[1] - starts[0], 0.035)
        self.assertGreaterEqual(starts[2] - starts[1], 0.035)

    def test_workers_do_not_change_cache_fingerprint(self):
        a = default_scope()
        b = dict(a)
        b["workers"] = 5
        self.assertEqual(scope_fingerprint(a), scope_fingerprint(b))


if __name__ == "__main__":
    unittest.main()
