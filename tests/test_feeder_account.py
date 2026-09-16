import pytest
import requests

from feeder_account import ACCOUNT_ENDPOINT, check_serpapi_account
from feeder_provider import ProviderError


class AccountSession:
    def __init__(self, payload, status=200):
        self.payload, self.status_code, self.calls = payload, status, []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError('private-key-in-response-url', response=self)

    def json(self):
        return self.payload


def test_account_check_uses_free_endpoint_and_returns_only_safe_summary():
    session = AccountSession({'account_status': 'Active', 'total_searches_left': 218,
                              'api_key': 'private-key', 'account_email': 'private-email',
                              'plan_name': 'untrusted-plan-text'})
    result = check_serpapi_account(' private-key ', session=session)
    assert result['state'] == 'ready' and '218' in result['message']
    assert set(result) == {'state', 'message'}
    assert not any(secret in str(result) for secret in ('private-key', 'private-email', 'untrusted-plan-text'))
    assert len(session.calls) == 1
    url, kwargs = session.calls[0]
    assert url == ACCOUNT_ENDPOINT and kwargs['params'] == {'api_key': 'private-key'}
    assert 'engine' not in kwargs['params'] and kwargs['timeout'] == (10, 20)


def test_account_zero_quota_is_not_reported_ready():
    result = check_serpapi_account('key', session=AccountSession({'account_status': 'Active', 'total_searches_left': 0}))
    assert result['state'] == 'quota' and 'no searches remaining' in result['message']


@pytest.mark.parametrize('payload', [
    {}, [], {'error': 'private-key'},
    {'account_status': 'private-status', 'total_searches_left': 20},
    *[{'account_status': 'Active', 'total_searches_left': value}
      for value in (None, True, -1, 1.5, 'private-key', 10**13)],
])
def test_unknown_or_malformed_account_response_is_safe_failure(payload):
    with pytest.raises(ProviderError) as caught:
        check_serpapi_account('private-key', session=AccountSession(payload))
    assert 'private' not in caught.value.safe_message


@pytest.mark.parametrize('status,code', [(401, 'invalid_key'), (403, 'forbidden'), (429, 'quota'), (503, 'unavailable')])
def test_account_http_failures_preserve_safe_cause(status, code):
    session = AccountSession({'error': 'private-key'}, status=status)
    with pytest.raises(ProviderError) as caught:
        check_serpapi_account('private-key', session=session)
    assert caught.value.code == code
    assert 'private' not in str(caught.value)
    assert len(session.calls) == 1


def test_missing_key_never_calls_account_api():
    session = AccountSession({})
    with pytest.raises(ProviderError) as caught:
        check_serpapi_account(' ', session=session)
    assert caught.value.code == 'unconfigured' and session.calls == []
