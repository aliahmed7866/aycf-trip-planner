"""Optional, quota-controlled-by-caller Google Flights provider via SerpApi.

This adapter makes one request only. It never stores credentials, retries, or
scrapes Google/airline websites directly. Live access requires the user's key;
fixture tests do not establish current live route or fare coverage.
"""
from datetime import date, datetime, timezone
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


class ProviderError(Exception):
    """Safe to display: contains no response, URL, credentials or raw errors."""


def _observed_at(payload, now):
    metadata = payload.get('search_metadata', {})
    if not isinstance(metadata, dict):
        raise ProviderError('The flight provider returned invalid search metadata.')
    raw = metadata.get('created_at')
    if raw is None:
        return now.isoformat()
    if not isinstance(raw, str):
        raise ProviderError('The flight provider returned an invalid observation time.')
    try:
        # SerpApi documents a UTC suffix; ISO offset-bearing times are accepted.
        value = datetime.fromisoformat(raw.removesuffix(' UTC') + '+00:00') if raw.endswith(' UTC') else datetime.fromisoformat(raw)
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError
        return value.astimezone(UTC).isoformat()
    except (ValueError, OverflowError):
        raise ProviderError('The flight provider returned an invalid observation time.') from None


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
        if departure.date().isoformat() != travel_date or arrival.astimezone(UTC) <= departure.astimezone(UTC):
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
        raise ProviderError('Choose a supported, exact destination airport.')
    try:
        if not isinstance(travel_date, str) or date.fromisoformat(travel_date).isoformat() != travel_date:
            raise ValueError
    except (ValueError, TypeError):
        raise ProviderError('Choose a valid departure date.') from None
    if not isinstance(api_key, str) or not api_key.strip():
        raise ProviderError('Add a SerpApi key to enable live feeder searches.')
    now = now or datetime.now(UTC)
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ProviderError('The observation time must include its timezone.')
    params = {
        'engine': 'google_flights', 'api_key': api_key.strip(),
        'departure_id': 'MAN', 'arrival_id': hub, 'outbound_date': travel_date,
        'type': 2, 'adults': 1, 'travel_class': 1, 'currency': 'GBP',
        'gl': 'uk', 'hl': 'en', 'stops': 1, 'max_price': 40,
        'show_hidden': 'true', 'deep_search': 'true',
    }
    try:
        response = (session or requests).get(ENDPOINT, params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError, TypeError):
        raise ProviderError('The flight provider could not complete this search. Please try later.') from None
    if not isinstance(payload, dict) or payload.get('error'):
        raise ProviderError('The flight provider could not complete this search. Please try later.')
    metadata = payload.get('search_metadata', {})
    if isinstance(metadata, dict) and metadata.get('status') not in (None, 'Success'):
        raise ProviderError('The flight provider has not completed this search.')
    if not any(field in payload for field in ('best_flights', 'other_flights')) and not (isinstance(metadata, dict) and metadata.get('status') == 'Success'):
        raise ProviderError('The flight provider returned invalid flight results.')
    parameters = payload.get('search_parameters', {})
    if not isinstance(parameters, dict) or parameters.get('currency', 'GBP') != 'GBP' or payload.get('currency', 'GBP') != 'GBP':
        raise ProviderError('The flight provider did not return GBP prices.')
    observed_at = _observed_at(payload, now.astimezone(UTC))
    booking_url = 'https://www.google.com/travel/flights?' + urlencode({
        'hl': 'en', 'curr': 'GBP', 'q': f'One way flights from MAN to {hub} on {travel_date}',
    })
    results, seen = [], set()
    for field in ('best_flights', 'other_flights'):
        offers = payload.get(field, [])
        if not isinstance(offers, list):
            raise ProviderError('The flight provider returned invalid flight results.')
        for offer in offers:
            item = _normalize_offer(offer, hub, travel_date, observed_at, booking_url)
            if item is None:
                continue
            identity = (item['flight_number'], item['departure'], item['arrival'], item['price_gbp'])
            if identity not in seen:
                results.append(item)
                seen.add(identity)
    return sorted(results, key=lambda item: (item['price_gbp'], item['departure']))
