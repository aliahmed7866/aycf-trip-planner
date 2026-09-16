"""Provider contract checks use fabricated responses, never live fare claims."""
from copy import deepcopy
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from feeder_provider import ProviderError, fetch_serpapi_offers


NOW = datetime(2026, 9, 16, 12, tzinfo=timezone.utc)


def offer(**changes):
    result = {
        'price': 29.99,
        'flights': [{
            'airline': 'Ryanair', 'flight_number': 'FR 123',
            'departure_airport': {'id': 'MAN', 'time': '2026-09-17 08:00'},
            'arrival_airport': {'id': 'BUD', 'time': '2026-09-17 11:30'},
        }],
    }
    result.update(changes)
    return result


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self

    def raise_for_status(self):
        pass

    def json(self):
        return deepcopy(self.payload)


def fetch(payload, **kwargs):
    return fetch_serpapi_offers('BUD', '2026-09-17', 'test-only-secret', session=FakeSession(payload), now=NOW, **kwargs)


def test_request_and_timezone_contract_and_safe_booking_link():
    session = FakeSession({'best_flights': [offer()], 'search_parameters': {'currency': 'GBP'}})
    rows = fetch_serpapi_offers('BUD', '2026-09-17', 'test-only-secret', session=session, now=NOW)
    assert len(session.calls) == len(rows) == 1
    url, request = session.calls[0]
    assert url == 'https://serpapi.com/search'
    assert request['timeout'] == 20
    params = request['params']
    assert params == {
        'engine': 'google_flights', 'api_key': 'test-only-secret',
        'departure_id': 'MAN', 'arrival_id': 'BUD', 'outbound_date': '2026-09-17',
        'type': 2, 'adults': 1, 'travel_class': 1, 'currency': 'GBP',
        'gl': 'uk', 'hl': 'en', 'stops': 1, 'max_price': 40,
        'show_hidden': 'true', 'deep_search': 'true',
    }
    row = rows[0]
    assert row['departure'] == '2026-09-17T08:00:00+01:00'
    assert row['arrival'] == '2026-09-17T11:30:00+02:00'
    assert row['observed_at'] == NOW.isoformat()
    assert row['source'] == 'Google Flights via SerpApi'
    assert row['origin'] == 'MAN' and row['destination'] == 'BUD'
    assert row['flight_number'] == 'FR123' and row['price_gbp'] == 29.99
    link = urlsplit(row['booking_url'])
    assert link.scheme == 'https' and link.netloc == 'www.google.com'
    assert parse_qs(link.query)['q'] == ['One way flights from MAN to BUD on 2026-09-17']
    assert 'secret' not in str(rows)


@pytest.mark.parametrize('timestamp', ['2026-09-15 14:30:00 UTC', '2026-09-15T16:30:00+02:00', '2026-09-15T14:30:00Z'])
def test_cached_observation_timestamp_is_preserved(timestamp):
    rows = fetch({'search_metadata': {'created_at': timestamp, 'status': 'Success'}, 'best_flights': [offer()]})
    assert rows[0]['observed_at'] == '2026-09-15T14:30:00+00:00'


def test_both_result_groups_airline_families_sort_and_deduplicate():
    easy = offer(price=19)
    easy['flights'][0].update(airline='easyJet Europe', flight_number='EC 124')
    buzz = offer(price=35)
    buzz['flights'][0].update(airline='Buzz', flight_number='RR 125')
    rows = fetch({'best_flights': [offer(), easy], 'other_flights': [offer(), buzz]})
    assert [(row['airline'], row['price_gbp']) for row in rows] == [('easyJet', 19), ('Ryanair', 29.99), ('Ryanair', 35)]


@pytest.mark.parametrize('price', [None, True, 0, -1, 40, 40.01, 'NaN', 'Infinity', '-Infinity', '', '£20', {}, []])
def test_reject_invalid_or_over_budget_price(price):
    assert fetch({'best_flights': [offer(price=price)]}) == []


@pytest.mark.parametrize('change', [
    ('departure_airport', {'id': 'LPL', 'time': '2026-09-17 08:00'}),
    ('arrival_airport', {'id': 'WAW', 'time': '2026-09-17 11:30'}),
    ('departure_airport', {'id': 'MAN', 'time': '2026-09-18 08:00'}),
    ('arrival_airport', {'id': 'BUD', 'time': '2026-09-17 07:00'}),
    ('arrival_airport', {'id': 'BUD', 'time': '2026-09-17'}),
    ('arrival_airport', {'id': 'BUD', 'time': 'bad-clock'}),
    ('airline', 'British Airways'),
    ('airline', 'Ryanair pretending'),
    ('flight_number', 'BA 123'),
    ('flight_number', ''),
])
def test_reject_unusable_flight_record(change):
    item = offer()
    item['flights'][0][change[0]] = change[1]
    assert fetch({'best_flights': [item]}) == []


def test_reject_malformed_multileg_and_conflicting_offer_currency():
    multileg = offer()
    multileg['flights'].append(deepcopy(multileg['flights'][0]))
    assert fetch({'other_flights': [None, 'bad', {}, offer(flights=None), offer(flights=[None]), multileg, offer(currency='EUR')]}) == []


@pytest.mark.parametrize('day,clock', [('2026-10-25', '01:30'), ('2026-03-29', '01:30')])
def test_reject_ambiguous_or_nonexistent_uk_clock(day, clock):
    item = offer()
    item['flights'][0]['departure_airport']['time'] = f'{day} {clock}'
    item['flights'][0]['arrival_airport']['time'] = f'{day} 05:00'
    assert fetch_serpapi_offers('BUD', day, 'key', session=FakeSession({'best_flights': [item]}), now=NOW) == []


def test_explicit_offset_resolves_dst_ambiguity():
    item = offer()
    item['flights'][0]['departure_airport']['time'] = '2026-10-25T01:30:00+01:00'
    item['flights'][0]['arrival_airport']['time'] = '2026-10-25T05:00:00+01:00'
    rows = fetch_serpapi_offers('BUD', '2026-10-25', 'key', session=FakeSession({'best_flights': [item]}), now=NOW)
    assert rows[0]['departure'] == '2026-10-25T01:30:00+01:00'


@pytest.mark.parametrize('payload', [
    {}, [], {'error': 'upstream-secret'}, {'best_flights': {}},
    {'search_metadata': {'status': 'Processing'}},
    {'best_flights': [offer()], 'currency': 'USD'},
    {'best_flights': [offer()], 'search_parameters': {'currency': 'EUR'}},
    {'best_flights': [offer()], 'search_metadata': {'created_at': '2026-09-16 12:00:00'}},
    {'best_flights': [offer()], 'search_metadata': {'created_at': 'bad'}},
    {'best_flights': [offer()], 'search_metadata': []},
])
def test_provider_errors_are_explicit_and_sanitized(payload):
    with pytest.raises(ProviderError) as caught:
        fetch(payload)
    assert 'secret' not in str(caught.value)


def test_successful_empty_search_is_not_a_provider_failure():
    assert fetch({'search_metadata': {'status': 'Success'}}) == []


def test_network_error_does_not_expose_key_or_retry():
    class BrokenSession(FakeSession):
        def raise_for_status(self):
            raise requests.HTTPError('https://serpapi.com/search?api_key=test-only-secret')
    session = BrokenSession(None)
    with pytest.raises(ProviderError) as caught:
        fetch_serpapi_offers('BUD', '2026-09-17', 'test-only-secret', session=session, now=NOW)
    assert len(session.calls) == 1
    assert 'secret' not in str(caught.value)
    assert caught.value.__suppress_context__


@pytest.mark.parametrize('hub,day,key', [('MIL', '2026-09-17', 'key'), ('BUD,MXP', '2026-09-17', 'key'), ('MAN', '2026-09-17', 'key'), ('BUD', '2026-02-30', 'key'), ('BUD', '20260917', 'key'), ('BUD', '2026-09-17', '')])
def test_invalid_search_never_calls_provider(hub, day, key):
    session = FakeSession(None)
    with pytest.raises(ProviderError):
        fetch_serpapi_offers(hub, day, key, session=session, now=NOW)
    assert session.calls == []
