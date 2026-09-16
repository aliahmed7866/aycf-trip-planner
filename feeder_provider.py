"""Optional, quota-controlled-by-caller Google Flights provider via SerpApi.

This adapter makes one request only. It never stores credentials, retries, or
scrapes Google/airline websites directly. Live access requires the user's key;
fixture tests do not establish current live route or fare coverage.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
from urllib.parse import urlencode

from dateutil import tz
import requests

from short_trips import airport_zone


UTC = timezone.utc
SOURCE = 'Google Flights via SerpApi'
ENDPOINT = 'https://serpapi.com/search'
MAN_ZONE = tz.gettz('Europe/London')
_CARRIERS = {
    'ryanair': ('Ryanair', {'FR', 'RK', 'RR', 'AL', 'OE'}),
    'ryanair uk': ('Ryanair', {'FR', 'RK'}),
    'malta air': ('Ryanair', {'FR', 'AL'}),
    'buzz': ('Ryanair', {'FR', 'RR'}),
    'lauda europe': ('Ryanair', {'FR', 'OE'}),
    'laudamotion': ('Ryanair', {'FR', 'OE'}),
    'easyjet': ('easyJet', {'U2', 'EC', 'DS'}),
    'easyjet uk': ('easyJet', {'U2'}),
    'easyjet europe': ('easyJet', {'U2', 'EC'}),
    'easyjet switzerland': ('easyJet', {'U2', 'DS'}),
}

PROVIDER_MESSAGES = {
    'invalid_key': 'SerpApi rejected the API key. Check the saved key and restart AYCF.',
    'forbidden': 'SerpApi denied access for this account. Check your SerpApi account status.',
    'quota': 'SerpApi has reached its search allowance or hourly limit. Check your account before trying again.',
    'invalid_request': 'SerpApi could not accept this request. Check the request details.',
    'unavailable': 'SerpApi could not complete the request. Please try later.',
    'timeout': 'SerpApi took too long to respond. Check the connection and try later.',
    'network': 'AYCF could not connect to SerpApi. Check the internet connection and try later.',
    'invalid_response': 'SerpApi returned an unexpected response. Please try later.',
    'incomplete': 'SerpApi has not completed this search. Please try later.',
    'unconfigured': 'Add a SerpApi key to enable live feeder searches.',
}
_EMPTY_RESULT_ERRORS = {
    "google flights hasn't returned any results for this query",
    "google hasn't returned any results for this query",
}


class ProviderError(Exception):
    """Safe to display: contains no response, URL, credentials or raw errors."""

    def __init__(self, code='unavailable'):
        # Legacy callers may pass arbitrary messages. Never retain or display
        # those values: request errors and provider responses can contain keys.
        self.code = code if isinstance(code, str) and code in PROVIDER_MESSAGES else 'unavailable'
        self.safe_message = PROVIDER_MESSAGES[self.code]
        super().__init__(self.safe_message)


def _http_error_code(status):
    return {
        400: 'invalid_request', 401: 'invalid_key',
        403: 'forbidden', 429: 'quota',
    }.get(status, 'unavailable')


def _request_json(endpoint, params, *, session=None, timeout=(10, 45)):
    """Request one JSON object, exposing only fixed, credential-free errors.

    A read timeout is deliberately longer than the connection timeout because
    deep flight searches can take longer. No failed request is retried here.
    Shared with the free Account API diagnostic; search semantics stay below.
    """
    try:
        response = (session or requests).get(endpoint, params=params, timeout=timeout)
        status = getattr(response, 'status_code', 200)
        if isinstance(status, int) and not 200 <= status < 300:
            raise ProviderError(_http_error_code(status))
        response.raise_for_status()
    except requests.Timeout:
        raise ProviderError('timeout') from None
    except requests.HTTPError as exc:
        status = getattr(exc.response, 'status_code', None)
        raise ProviderError(_http_error_code(status)) from None
    except requests.RequestException:
        raise ProviderError('network') from None
    try:
        payload = response.json()
    except (ValueError, TypeError):
        raise ProviderError('invalid_response') from None
    if not isinstance(payload, dict):
        raise ProviderError('invalid_response')
    return payload


def _observed_at(payload, now):
    metadata = payload.get('search_metadata', {})
    if not isinstance(metadata, dict):
        raise ProviderError('invalid_response')
    raw = metadata.get('created_at')
    if raw is None:
        return now.isoformat()
    if not isinstance(raw, str):
        raise ProviderError('invalid_response')
    try:
        # SerpApi documents a UTC suffix; ISO offset-bearing times are accepted.
        value = datetime.fromisoformat(raw.removesuffix(' UTC') + '+00:00') if raw.endswith(' UTC') else datetime.fromisoformat(raw)
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError
        return value.astimezone(UTC).isoformat()
    except (ValueError, OverflowError):
        raise ProviderError('invalid_response') from None


def _local_clock(raw, zone):
    if not isinstance(raw, str) or not re.match(r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}', raw) or zone is None:
        raise ValueError
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None:
        value = value.replace(tzinfo=zone)
        if not tz.datetime_exists(value) or tz.datetime_ambiguous(value):
            raise ValueError
    else:
        value = value.astimezone(zone)
    return value


def _price(offer):
    raw = offer.get('price')
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float, Decimal)):
        raise ValueError
    value = Decimal(str(raw))
    if not value.is_finite() or not Decimal('0') < value < Decimal('40'):
        raise ValueError
    if value != value.quantize(Decimal('.01')):
        raise ValueError
    if 'currency' in offer and offer['currency'] != 'GBP':
        raise ValueError
    return float(value)


def _normalize_offer(offer, hub, travel_date, observed_at, booking_url):
    if not isinstance(offer, dict):
        return None
    flights = offer.get('flights')
    if not isinstance(flights, list) or len(flights) != 1 or not isinstance(flights[0], dict):
        return None
    flight = flights[0]
    try:
        origin = flight['departure_airport']
        destination = flight['arrival_airport']
        if not isinstance(origin, dict) or not isinstance(destination, dict):
            return None
        if origin.get('id') != 'MAN' or destination.get('id') != hub:
            return None
        airline = _CARRIERS.get(str(flight.get('airline', '')).strip().casefold())
        number = re.fullmatch(r'([A-Z0-9]{2})\s*([0-9]{1,4})', str(flight.get('flight_number', '')).strip())
        if not airline or not number or number[1] not in airline[1]:
            return None
        departure = _local_clock(origin.get('time'), MAN_ZONE)
        arrival = _local_clock(destination.get('time'), airport_zone(hub))
        duration = arrival.astimezone(UTC) - departure.astimezone(UTC)
        if departure.date().isoformat() != travel_date or not timedelta(0) < duration <= timedelta(hours=18):
            return None
        return {
            'origin': 'MAN', 'destination': hub, 'airline': airline[0],
            'flight_number': number[1] + number[2],
            'departure': departure.isoformat(), 'arrival': arrival.isoformat(),
            'price_gbp': _price(offer), 'observed_at': observed_at,
            'source': SOURCE, 'booking_url': booking_url,
        }
    except (ValueError, TypeError, KeyError, InvalidOperation, OverflowError):
        return None


def fetch_serpapi_offers(hub: str, travel_date: str, api_key: str, *, session=None, now=None) -> list[dict]:
    """Fetch one MAN→exact-hub date, returning validated fares strictly below £40.

    Caller must enforce free-plan quotas before invoking. ``session`` supports
    fixture injection; ``now`` is an aware datetime used only when no provider
    observation time is supplied. Cached provider timestamps stay unchanged.
    A successful empty list means no matching *returned* offers, not proof that
    the route has no flights. Unsupported/malformed individual offers are omitted.
    """
    if not isinstance(hub, str) or not re.fullmatch(r'[A-Z]{3}', hub) or hub == 'MAN' or airport_zone(hub) is None:
        raise ProviderError('invalid_request')
    try:
        if not isinstance(travel_date, str) or date.fromisoformat(travel_date).isoformat() != travel_date:
            raise ValueError
    except (ValueError, TypeError):
        raise ProviderError('invalid_request') from None
    if not isinstance(api_key, str) or not api_key.strip():
        raise ProviderError('unconfigured')
    now = now or datetime.now(UTC)
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ProviderError('invalid_request')
    params = {
        'engine': 'google_flights', 'api_key': api_key.strip(),
        'departure_id': 'MAN', 'arrival_id': hub, 'outbound_date': travel_date,
        'type': 2, 'adults': 1, 'travel_class': 1, 'currency': 'GBP',
        'gl': 'uk', 'hl': 'en', 'stops': 1, 'max_price': 40,
        'show_hidden': 'true', 'deep_search': 'true',
    }
    payload = _request_json(ENDPOINT, params, session=session)
    metadata = payload.get('search_metadata', {})
    if not isinstance(metadata, dict):
        raise ProviderError('invalid_response')
    status = metadata.get('status')
    if status in ('Processing', 'Queued'):
        raise ProviderError('incomplete')
    if status == 'Error':
        raise ProviderError('unavailable')
    if status not in (None, 'Success'):
        raise ProviderError('invalid_response')
    if not any(field in payload for field in ('best_flights', 'other_flights')) and status != 'Success':
        raise ProviderError('invalid_response')
    parameters = payload.get('search_parameters', {})
    if not isinstance(parameters, dict) or parameters.get('currency', 'GBP') != 'GBP' or payload.get('currency', 'GBP') != 'GBP':
        raise ProviderError('invalid_response')
    observed_at = _observed_at(payload, now.astimezone(UTC))
    for field in ('best_flights', 'other_flights'):
        if not isinstance(payload.get(field, []), list):
            raise ProviderError('invalid_response')
    error = payload.get('error')
    if error:
        # SerpApi reports some completed, empty searches via an error field.
        # Accept only documented wording with explicit successful metadata and
        # empty result groups; unknown failures must not overwrite saved fares.
        empty_error = isinstance(error, str) and error.strip().replace('\u2019', "'").rstrip('.').casefold() in _EMPTY_RESULT_ERRORS
        if status != 'Success' or not empty_error:
            raise ProviderError('unavailable')
        if any(payload.get(field) for field in ('best_flights', 'other_flights')):
            raise ProviderError('invalid_response')
        return []
    booking_url = 'https://www.google.com/travel/flights?' + urlencode({
        'hl': 'en', 'curr': 'GBP', 'q': f'One way flights from MAN to {hub} on {travel_date}',
    })
    results, seen = [], set()
    for field in ('best_flights', 'other_flights'):
        offers = payload.get(field, [])
        for offer in offers:
            item = _normalize_offer(offer, hub, travel_date, observed_at, booking_url)
            if item is None:
                continue
            identity = (item['flight_number'], item['departure'], item['arrival'], item['price_gbp'])
            if identity not in seen:
                results.append(item)
                seen.add(identity)
    return sorted(results, key=lambda item: (item['price_gbp'], item['departure']))
