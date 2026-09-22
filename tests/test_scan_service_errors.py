"""Keep isolated 5xx failures pending, without bypassing outage/auth/429 stops."""
from datetime import date
from unittest.mock import Mock
import threading

import pytest
import requests

from morning_scan import verify_scan_requests
from parallel_fetch import ParallelFetcher, fetch_group
from scan_service_errors import ServiceFailureTracker
from scanner import WizzSessionExpired, WizzRequestRejected
from wizz_rate_limit import WizzRateLimited

DAY = date(2026, 9, 22)


def error(code=500):
    response = requests.Response()
    response.status_code = code
    return requests.HTTPError('Private URL and body must not appear in route diagnostics', response=response)


def jobs(count):
    return [('primary', f'Origin {i}', 'Budapest', DAY, [f'Origin {i}'], ['Budapest'],
             [(f'Origin {i}', 'Budapest')]) for i in range(count)]


def test_preflight_tries_another_route_after_exhausted_500(capsys):
    client = Mock()
    client.preflight.side_effect = [error(), {'ok': True, 'availability_verified': True}]
    assert verify_scan_requests(client, jobs(4))['ok']
    assert client.preflight.call_count == 2
    log = capsys.readouterr().out
    assert 'Origin 0 -> Budapest on 2026-09-22' in log
    assert 'Private URL' not in log


def test_preflight_service_failures_are_bounded_and_not_auth_failures():
    client = Mock()
    client.preflight.side_effect = error(503)
    with pytest.raises(requests.HTTPError, match='3 scoped preflight probes') as stopped:
        verify_scan_requests(client, jobs(20))
    assert stopped.value.response.status_code == 503
    assert client.preflight.call_count == 3


def test_preflight_mix_of_unknown_and_500_does_not_demand_auth_repair():
    client = Mock()
    client.preflight.side_effect = [{'ok': False}, error(), {'ok': False}]
    with pytest.raises(requests.HTTPError):
        verify_scan_requests(client, jobs(3))


@pytest.mark.parametrize('failure', [WizzRateLimited(), WizzSessionExpired('expired'),
                                   WizzRequestRejected('418'), error(400)])
def test_preflight_does_not_continue_after_auth_rate_limit_or_bad_request(failure):
    client = Mock()
    client.preflight.side_effect = failure
    with pytest.raises(type(failure)):
        verify_scan_requests(client, jobs(3))
    assert client.preflight.call_count == 1


def test_isolated_500_does_not_skip_other_airports_or_certify_empty():
    client = Mock()
    client.check.side_effect = [[], error(), []]
    pairs = [('Luton', 'Budapest'), ('Gatwick', 'Budapest'), ('Liverpool', 'Budapest')]
    flights, checked, unknown = fetch_group(client, pairs, DAY)
    assert flights == [] and checked == [pairs[0], pairs[2]]
    assert len(unknown) == 1 and 'HTTP 500 for Gatwick' in unknown[0]
    assert client.check.call_count == 3


def test_scattered_failure_threshold_and_latched_pause():
    tracker = ServiceFailureTracker()
    for _ in range(4):
        tracker.failure(error())
        tracker.success()
    with pytest.raises(requests.HTTPError, match='5 in the last 9'):
        tracker.failure(error())
    tracker.success()  # An in-flight success cannot reopen a stopped scan.
    with pytest.raises(requests.HTTPError):
        tracker.check()


def test_old_isolated_errors_age_out_of_failure_window():
    tracker = ServiceFailureTracker()
    for _ in range(20):
        tracker.failure(error())
        for _ in range(4):
            tracker.success()
    tracker.check()


@pytest.mark.parametrize('workers', [1, 3])
def test_outage_stops_queued_work_after_bounded_checks(workers):
    attempted = []
    lock = threading.Lock()
    class Client:
        live_requests = no_availability_responses = wallet_redirects = html_retries = 0
        def check(self, origin, destination, day):
            with lock:
                attempted.append(origin)
            self.live_requests += 1
            raise error()
    captured = []
    with pytest.raises(requests.HTTPError, match='Pausing scan'):
        ParallelFetcher(Client, workers=workers).run(jobs(100), captured.append)
    assert 3 <= len(attempted) <= 3 + workers - 1
    assert all(result['unknown'] and not result['checked_pairs'] for result in captured)


def test_isolated_service_error_does_not_stop_later_groups():
    class Client:
        live_requests = no_availability_responses = wallet_redirects = html_retries = 0
        def check(self, origin, destination, day):
            self.live_requests += 1
            if origin == 'Origin 1':
                raise error()
            return []
    captured = []
    ParallelFetcher(Client, workers=1).run(jobs(5), captured.append)
    assert len(captured) == 5
    assert sum(bool(result['unknown']) for result in captured) == 1
    assert sum(len(result['checked_pairs']) for result in captured) == 4
