from concurrent.futures import CancelledError
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import Mock

import pytest
import requests

from parallel_fetch import GlobalStartLimiter, ParallelFetcher
from scanner import WizzAYCFClient, WizzRateLimited, _retry_after_seconds
import wizz_rate_limit as limits


def response(status=429, retry='120'):
    result = requests.Response()
    result.status_code = status
    result.headers['Retry-After'] = retry
    return result


@pytest.fixture
def clock(monkeypatch):
    value = [100000.0]
    monkeypatch.setattr(limits.time, 'time', lambda: value[0])
    monkeypatch.setattr(limits.time, 'monotonic', lambda: value[0])
    monkeypatch.setattr(limits.time, 'sleep', lambda seconds: value.__setitem__(0, value[0] + seconds))
    return value


def test_retry_after_honours_long_numeric_and_date_delays():
    assert _retry_after_seconds(response()) == 120
    future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=180))
    assert 178 <= _retry_after_seconds(response(retry=future)) <= 180
    for invalid in ('garbage', 'nan', 'inf'):
        assert _retry_after_seconds(response(retry=invalid)) == 4


def test_waiter_aborts_when_another_worker_records_cooldown(monkeypatch, clock):
    limiter = GlobalStartLimiter(1)
    limiter.wait()
    waits = []

    def wait(seconds):
        waits.append(seconds)
        clock[0] += 0.5
        limits.record_rate_limit(120)

    monkeypatch.setattr(limiter._stopped, 'wait', wait)
    with pytest.raises(WizzRateLimited):
        limiter.wait()
    assert waits == [1]
    assert clock[0] == 100000.5
    assert limiter.status()['effective_request_interval'] == 5


def test_stopped_limiter_does_not_wait_for_retry_deadline(clock):
    limiter = GlobalStartLimiter()
    limits.record_rate_limit(3600)
    limiter.stop()
    with pytest.raises(CancelledError):
        limiter.wait()
    assert clock[0] == 100000


def test_first_rate_limit_stops_without_retry_and_next_client_never_sends(monkeypatch, clock):
    monkeypatch.setenv('AYCF_HTTP_ATTEMPTS', '5')
    client = WizzAYCFClient({})
    fetcher = ParallelFetcher(lambda: client, workers=4)
    fetcher._client()
    client.http.request = Mock(side_effect=[response(), response(200)])
    with pytest.raises(WizzRateLimited) as caught:
        client._request('GET', 'https://example.invalid')
    assert client.http.request.call_count == client.live_requests == 1
    assert caught.value.status['cooldown_until'] == 100900
    assert fetcher.limiter.status()['rate_limit_responses'] == 1
    assert fetcher.limiter.status()['effective_request_interval'] == 5
    assert clock[0] == 100000
    other = WizzAYCFClient({})
    other._throttle = Mock()
    other.http.request = Mock(return_value=response(200))
    with pytest.raises(WizzRateLimited):
        other._request('GET', 'https://example.invalid')
    other._throttle.assert_not_called()
    other.http.request.assert_not_called()
    assert other.live_requests == 0


def test_first_rate_limit_stops_parallel_workers(monkeypatch, clock):
    client = WizzAYCFClient({})
    fetcher = ParallelFetcher(lambda: client, workers=1)
    client.http.request = Mock(return_value=response())
    client.no_availability_responses = client.wallet_redirects = client.html_retries = 0
    client.check = lambda *args: client._request('GET', 'https://example.invalid')
    job = ('primary', 'A', 'B', None, ['A'], ['B'])
    with pytest.raises(WizzRateLimited):
        fetcher.run([job, job, job], lambda result: None)
    assert fetcher.limiter._stopped.is_set()
    assert client.http.request.call_count == 1
    assert fetcher.limiter.status()['rate_limit_responses'] == 1


def test_cooldown_recorded_during_local_throttle_prevents_send(clock):
    client = WizzAYCFClient({})
    client._throttle = lambda: limits.record_rate_limit(3600)
    client.http.request = Mock(return_value=response(200))
    with pytest.raises(WizzRateLimited):
        client._request('GET', 'https://example.invalid')
    client.http.request.assert_not_called()
    assert client.live_requests == 0


def test_success_from_inflight_request_does_not_clear_other_workers_cooldown(clock):
    client = WizzAYCFClient({})
    client._throttle = lambda: None

    def send(*args, **kwargs):
        limits.record_rate_limit(3600)
        return response(200)

    client.http.request = send
    assert client._request('GET', 'https://example.invalid').status_code == 200
    assert limits.rate_limit_status()['blocked']
    assert limits.rate_limit_status()['cooldown_until'] == 103600
    with pytest.raises(WizzRateLimited):
        client._request('GET', 'https://example.invalid')
    assert client.live_requests == 1


def test_normal_5xx_retry_is_still_spaced(clock):
    client = WizzAYCFClient({})
    client._throttle = lambda: None
    starts = []
    replies = iter([response(503), response(200)])

    def send(*args, **kwargs):
        starts.append(clock[0])
        return next(replies)

    client.http.request = send
    assert client._request('GET', 'https://example.invalid').status_code == 200
    assert starts == [100000, 100001.5]
    assert client.live_requests == 2


def test_new_fetcher_reports_retained_pace_and_later_restores_baseline(clock):
    status = limits.record_rate_limit(0)
    clock[0] = status['cooldown_until'] + 1
    status = limits.record_rate_limit(0)
    clock[0] = status['cooldown_until'] + 1
    fetcher = ParallelFetcher(lambda: WizzAYCFClient({}), start_interval=2)
    assert fetcher.limiter.status()['effective_request_interval'] == 10
    clock[0] = status['last_rate_limit_at'] + limits.QUIET_SECONDS + 1
    assert fetcher.limiter.status()['effective_request_interval'] == 2
