from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import Mock

import pytest
import requests
from concurrent.futures import CancelledError

from parallel_fetch import GlobalStartLimiter, ParallelFetcher
from scanner import WizzAYCFClient, WizzRateLimited, _retry_after_seconds


def response(status=429, retry='120'):
    r = requests.Response()
    r.status_code = status
    r.headers['Retry-After'] = retry
    return r


def test_retry_after_honours_long_numeric_and_date_delays():
    assert _retry_after_seconds(response()) == 120
    future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=180))
    assert 178 <= _retry_after_seconds(response(retry=future)) <= 180
    for invalid in ('garbage', 'nan', 'inf'):
        assert _retry_after_seconds(response(retry=invalid)) == 4


def test_waiter_rechecks_extended_shared_cooldown(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr('parallel_fetch.time.monotonic', lambda: clock[0])
    limiter = GlobalStartLimiter(1)
    limiter.wait()
    waits = []
    def wait(seconds):
        waits.append(seconds)
        if len(waits) == 1:
            clock[0] = 0.5
            limiter.cooldown(120)
        else:
            clock[0] += seconds
    monkeypatch.setattr(limiter._stopped, 'wait', wait)
    limiter.wait()
    assert clock[0] == 120.5
    assert limiter.status()['rate_limit_responses'] == 1
    assert limiter.interval == 2
    limiter.wait()
    assert clock[0] == 122.5  # No burst after the cooldown.


def test_stopped_limiter_does_not_wait_for_retry_deadline():
    limiter = GlobalStartLimiter()
    limiter.cooldown(3600)
    limiter.stop()
    with pytest.raises(CancelledError):
        limiter.wait()


def test_retry_uses_shared_cooldown_and_every_attempt_is_paced(monkeypatch):
    client = WizzAYCFClient({})
    fetcher = ParallelFetcher(lambda: client, workers=4)
    fetcher._client()
    clock = [0.0]
    monkeypatch.setattr('parallel_fetch.time.monotonic', lambda: clock[0])
    monkeypatch.setattr(fetcher.limiter._stopped, 'wait', lambda delay: clock.__setitem__(0, clock[0] + delay))
    starts = []
    replies = iter([response(), response(200)])
    def send(*args, **kwargs):
        starts.append(clock[0])
        return next(replies)
    client.http.request = send
    assert client._request('GET', 'https://example.invalid').status_code == 200
    assert starts == [0.0, 120.0]
    assert fetcher.limiter.status()['rate_limit_responses'] == 1


def test_exhausted_rate_limit_stops_other_workers(monkeypatch):
    monkeypatch.setenv('AYCF_HTTP_ATTEMPTS', '2')
    client = WizzAYCFClient({})
    fetcher = ParallelFetcher(lambda: client, workers=1)
    clock = [0.0]
    monkeypatch.setattr('parallel_fetch.time.monotonic', lambda: clock[0])
    monkeypatch.setattr(fetcher.limiter._stopped, 'wait', lambda delay: clock.__setitem__(0, clock[0] + delay))
    client.http.request = Mock(return_value=response())
    client.no_availability_responses = client.wallet_redirects = client.html_retries = 0
    client.check = lambda *args: client._request('GET', 'https://example.invalid')
    job = ('primary', 'A', 'B', None, ['A'], ['B'])
    with pytest.raises(WizzRateLimited):
        fetcher.run([job], lambda result: None)
    assert fetcher.limiter._stopped.is_set()
    assert fetcher.limiter.status()['rate_limit_responses'] == 2
