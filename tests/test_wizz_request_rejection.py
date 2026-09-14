from contextlib import contextmanager
from unittest.mock import Mock

import pytest
import requests
from scanner import WizzAYCFClient, WizzRequestRejected
from termux import automated_morning


def test_http_418_is_not_retried_or_reported_as_expired():
    client = WizzAYCFClient({})
    client._throttle = lambda: None
    response = requests.Response()
    response.status_code = 418
    response.url = 'https://example.invalid/private-endpoint'
    client.http.request = Mock(return_value=response)
    with pytest.raises(WizzRequestRejected, match='HTTP 418') as error:
        client._request('POST', response.url)
    client.http.request.assert_called_once()
    assert 'private-endpoint' not in str(error.value)


def test_rejection_survives_runner_without_repair_or_completion(monkeypatch):
    @contextmanager
    def lock():
        yield True
    monkeypatch.setattr(automated_morning, 'single_scan_lock', lock)
    monkeypatch.setattr(automated_morning.tiered_morning, 'run', Mock(side_effect=WizzRequestRejected('HTTP 418')))
    refresh = Mock()
    monkeypatch.setattr(automated_morning, '_refresh', refresh)
    statuses = []
    monkeypatch.setattr(automated_morning, 'write_status', lambda state, *a, **k: statuses.append(state))
    result = automated_morning.run(force=True)
    assert result['state'] == 'request_rejected'
    assert result['http_status'] == 418
    assert statuses == ['running', 'request_rejected']
    refresh.assert_not_called()


def test_repair_rejection_does_not_rediscover_endpoint(monkeypatch):
    from termux import refresh_wizz_from_chrome as repair
    client = Mock()
    client.preflight.side_effect = WizzRequestRejected('HTTP 418')
    monkeypatch.setattr(repair, 'CapturedRequestWizzClient', lambda *a, **k: client)
    monkeypatch.setattr(repair, '_normalize_runtime_in_place', lambda r: False)
    monkeypatch.setattr(repair, 'apply_runtime', lambda *a: True)
    rediscover = Mock()
    monkeypatch.setattr(repair, '_rediscover_endpoint', rediscover)
    with pytest.raises(WizzRequestRejected):
        repair._validate_candidate({}, {})
    rediscover.assert_not_called()
