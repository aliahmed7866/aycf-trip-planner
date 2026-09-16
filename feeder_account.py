"""Explicit, quota-free SerpApi account check with a strict output allowlist.

The account response includes credentials and personal details. Never return,
persist or log that response: only fixed messages and validated counts leave
this module. This check does not establish Google Flights coverage.
"""
from feeder_provider import ProviderError, _request_json


ACCOUNT_ENDPOINT = 'https://serpapi.com/account.json'


def check_serpapi_account(api_key, *, session=None):
    """Check the configured key once; no search request or automatic retry."""
    if not isinstance(api_key, str) or not api_key.strip():
        raise ProviderError('unconfigured')
    payload = _request_json(ACCOUNT_ENDPOINT, {'api_key': api_key.strip()},
                            session=session, timeout=(10, 20))
    if payload.get('error'):
        raise ProviderError('invalid_response')
    if payload.get('account_status') != 'Active':
        raise ProviderError('forbidden')
    remaining = payload.get('total_searches_left')
    if type(remaining) is not int or not 0 <= remaining <= 10**12:
        raise ProviderError('invalid_response')
    if remaining == 0:
        return {'state': 'quota', 'message': 'SerpApi key accepted, but your account has no searches remaining. Check your allowance in SerpApi. No flight search was requested.'}
    return {'state': 'ready', 'message': f'SerpApi key accepted. {remaining:,} account searches remaining. Flight results still depend on each route and date. No flight search was requested.'}
