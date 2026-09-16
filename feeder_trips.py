"""Join Manchester cash fares to observed AYCF flights without live requests.

Cash observations never enter the Wizz scan cache. All connection arithmetic
uses UTC, and a city-wide airport label cannot establish a self-transfer.
"""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import os
import re
from urllib.parse import urlsplit

from airport_catalog import airport_code, airport_labels, country_for
from scan_scope import (AIRPORT_GROUPS, concrete_route_allowed, endpoint_excluded,
                        endpoint_matches, journey_hubs, normalize_name, transit_scope)
from short_trips import UK_ZONE, _flight_time, airport_zone, local_datetime

UTC = timezone.utc
DEFAULT_COUNTRIES = ('Egypt', 'Saudi Arabia', 'Georgia', 'Armenia')


def _now(value=None):
    value = value or datetime.now(UTC)
    if value.tzinfo is None:
        raise ValueError('Current time must include a timezone.')
    return value.astimezone(UTC)


def _hours_setting(name, default):
    try:
        value = float(os.environ.get(name, default))
        return value if 0 < value <= 168 else default
    except (ValueError, TypeError):
        return default


def _timestamp(value, *, zone=UTC):
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).strip().replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        parsed = local_datetime(parsed.isoformat(), zone)
    return parsed.astimezone(UTC)


def _physical_code(value):
    name = str(value or '').strip()
    if normalize_name(name) in AIRPORT_GROUPS:
        return ''
    return airport_code(name)


def _offer_airport(value):
    code = str(value or '').strip().upper()
    if not re.fullmatch(r'[A-Z]{3}', code):
        raise ValueError('Use exact three-letter airport codes, such as MAN or WAW.')
    zone = UK_ZONE if code == 'MAN' else airport_zone(code)
    if zone is None:
        raise ValueError('The airport timezone is not available.')
    return code, zone


def normalize_offer(offer, now=None):
    """Validate a cash observation; prices remain decimal strings in GBP.

    Offset-free timestamps are interpreted in the named airport's timezone.
    Ambiguous/nonexistent local clocks are rejected. Freshness is checked when
    matching, so expired saved observations can still be displayed separately.
    """
    if not isinstance(offer, dict):
        raise ValueError('A fare must be an object.')
    current = _now(now)
    origin, origin_zone = _offer_airport(offer.get('origin'))
    destination, destination_zone = _offer_airport(offer.get('destination'))
    if origin != 'MAN' or destination == origin:
        raise ValueError('Feeder flights must depart Manchester (MAN) for another airport.')
    try:
        departure = _timestamp(offer.get('departure'), zone=origin_zone)
        arrival = _timestamp(offer.get('arrival'), zone=destination_zone)
        observed = _timestamp(offer.get('observed_at'))
        price = Decimal(str(offer.get('price_gbp', '')))
    except (ValueError, TypeError, InvalidOperation, OverflowError) as exc:
        raise ValueError('Enter valid flight times, observation time and a GBP fare.') from exc
    if arrival <= departure or arrival - departure > timedelta(hours=18):
        raise ValueError('Arrival must follow departure with a realistic flight duration.')
    if observed > current + timedelta(minutes=5):
        raise ValueError('The observation time cannot be in the future.')
    try:
        valid_price = price.is_finite() and 0 < price <= 10000 and price == price.quantize(Decimal('.01'))
    except InvalidOperation:
        valid_price = False
    if not valid_price:
        raise ValueError('Enter a positive GBP fare with at most two decimal places.')
    if str(offer.get('currency') or 'GBP').upper() != 'GBP':
        raise ValueError('Only fares quoted in GBP can be matched to the pound budget.')
    airline = {'ryanair': 'Ryanair', 'easyjet': 'easyJet', 'easy jet': 'easyJet'}.get(
        str(offer.get('airline') or '').strip().casefold())
    number = str(offer.get('flight_number') or '').strip().upper()
    if not airline or not number or len(number) > 24:
        raise ValueError('Choose Ryanair or easyJet and enter a flight number.')
    link = str(offer.get('booking_url') or '').strip()
    if link:
        parsed = urlsplit(link)
        if parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError('The booking link must be an ordinary http or https URL.')
    return {'origin': origin, 'destination': destination, 'airline': airline,
            'flight_number': number, 'departure': departure.isoformat(),
            'arrival': arrival.isoformat(), 'price_gbp': format(price, '.2f'),
            'currency': 'GBP', 'observed_at': observed.isoformat(),
            'source': str(offer.get('source') or 'manual')[:120], 'booking_url': link}


def feeder_opportunities(db, run_id, scope, countries=DEFAULT_COUNTRIES, now=None,
                         min_transfer_minutes=180, max_layover_minutes=1440,
                         max_wizz_legs=2, limit=80, work_limit=50000):
    """Discover target-bound cached Wizz paths for a paid Manchester feeder."""
    current = _now(now)
    min_transfer_minutes = max(120, int(min_transfer_minutes))
    max_layover_minutes = max(0, int(max_layover_minutes))
    max_wizz_legs = max(1, min(2, int(max_wizz_legs)))
    limit = None if limit is None else max(1, min(500, int(limit)))
    work_limit = max(1, int(work_limit))
    report = {'opportunities': [], 'incomplete': False, 'limited': False,
              'scan_partial': False, 'omitted_times': 0, 'stale_flights': 0,
              'flight_count': 0, 'window_start': None, 'window_end': None,
              'scope_unknown': scope is None, 'total': 0}
    run = db.get_pdf_run(run_id) if run_id else None
    if not run:
        report['incomplete'] = True
        return report
    report['scan_partial'] = not bool(run.get('scanned_at'))
    scope = scope or {}
    transit = transit_scope(scope)
    allowed_countries = set(countries)
    approved = journey_hubs(scope)
    report['window_start'] = str(run.get('departure_start') or '')[:10] or None
    report['window_end'] = str(run.get('departure_end') or '')[:10] or None
    if not report['window_start'] or not report['window_end']:
        report['incomplete'] = True
        return report
    with db.connect() as conn:
        rows = [dict(row) for row in conn.execute('''
            SELECT f.* FROM route_flights f JOIN route_checks c
              ON f.pdf_run_id=c.pdf_run_id AND f.origin=c.origin
             AND f.destination=c.destination AND f.travel_date=c.travel_date
            WHERE f.pdf_run_id=? ORDER BY f.departure, f.flight_code
        ''', (run_id,))]
    incoming, flights, seen = defaultdict(list), [], set()
    max_age = timedelta(hours=_hours_setting('AYCF_FEEDER_WIZZ_MAX_AGE_HOURS', 24))
    labels = airport_labels()
    for row in rows:
        origin_name = row.get('physical_origin') or row['origin']
        destination_name = row.get('physical_destination') or row['destination']
        origin, destination = _physical_code(origin_name), _physical_code(destination_name)
        if not origin or not destination or origin == destination:
            report['omitted_times'] += 1
            continue
        if not concrete_route_allowed(origin_name, destination_name, transit):
            continue
        if not report['window_start'] <= row['travel_date'] <= report['window_end']:
            continue
        try:
            fetched = _timestamp(row['fetched_at'])
            if fetched > current + timedelta(minutes=5) or current - fetched > max_age:
                report['stale_flights'] += 1
                continue
            dep_local = _flight_time(row, 'departure', origin_name)
            arr_local = _flight_time(row, 'arrival', destination_name)
            dep, arr = dep_local.astimezone(UTC), arr_local.astimezone(UTC)
            if dep_local.date().isoformat() != row['travel_date']:
                raise ValueError('Wrong departure day')
            if ':' in str(row.get('arrival_text') or '') and len(str(row['arrival_text'])) <= 8:
                previous = arr_local - timedelta(days=1)
                if previous.astimezone(UTC) > dep:
                    arr_local, arr = previous, previous.astimezone(UTC)
            if arr <= dep or arr - dep > timedelta(hours=18):
                raise ValueError('Invalid flight duration')
        except (ValueError, TypeError, OverflowError):
            report['omitted_times'] += 1
            continue
        if dep <= current + timedelta(minutes=min_transfer_minutes):
            continue
        signature = (origin, destination, row['flight_code'], dep, arr)
        if signature in seen:
            continue
        seen.add(signature)
        leg = {'origin': origin, 'destination': destination,
               'origin_name': labels.get(origin, origin_name),
               'destination_name': labels.get(destination, destination_name),
               'flight_number': row['flight_code'], 'departure': dep.isoformat(),
               'arrival': arr.isoformat(), 'departure_local': dep_local.isoformat(),
               'arrival_local': arr_local.isoformat(), 'fetched_at': fetched.isoformat()}
        flights.append(leg)
        incoming[destination].append(leg)
    report['flight_count'] = len(flights)
    candidates, signatures, work = [], set(), 0

    def add_path(legs):
        first, last = legs[0], legs[-1]
        if endpoint_excluded(last['destination'], scope):
            return
        # A hub excluded as a visit may still be an explicitly allowed transit.
        if endpoint_excluded(first['origin'], transit):
            return
        signature = tuple((f['origin'], f['destination'], f['flight_number'], f['departure']) for f in legs)
        if signature in signatures:
            return
        signatures.add(signature)
        departure = _timestamp(first['departure'])
        latest = departure - timedelta(minutes=min_transfer_minutes)
        earliest = max(current, departure - timedelta(minutes=max_layover_minutes, hours=18))
        first_day, last_day = earliest.astimezone(UK_ZONE).date(), latest.astimezone(UK_ZONE).date()
        dates = [(first_day + timedelta(days=offset)).isoformat()
                 for offset in range((last_day - first_day).days + 1)]
        if not dates:
            return
        candidates.append({'id': hashlib.sha256(repr(signature).encode()).hexdigest()[:16],
                           'hub': first['origin'], 'hub_name': first['origin_name'],
                           'feeder_dates': dates, 'destination': last['destination'],
                           'destination_name': last['destination_name'],
                           'country': country_for(last['destination']), 'legs': legs,
                           'first_departure': first['departure'], 'final_arrival': last['arrival'],
                           'wizz_legs': len(legs),
                           'oldest_checked': min(f['fetched_at'] for f in legs)})

    for final in flights:
        if country_for(final['destination']) not in allowed_countries:
            continue
        work += 1
        if work > work_limit:
            report['incomplete'] = True
            break
        add_path([final])
        if max_wizz_legs < 2 or not any(endpoint_matches(final['origin'], hub) for hub in approved):
            continue
        for previous in incoming.get(final['origin'], []):
            work += 1
            if work > work_limit:
                report['incomplete'] = True
                break
            if previous['origin'] == final['destination']:
                continue
            wait = (_timestamp(final['departure']) - _timestamp(previous['arrival'])).total_seconds() / 60
            if min_transfer_minutes <= wait <= max_layover_minutes:
                add_path([previous, final])
        if report['incomplete']:
            break
    candidates.sort(key=lambda item: (item['wizz_legs'], item['first_departure'], item['hub'], item['destination']))
    report['total'] = len(candidates)
    report['limited'] = limit is not None and len(candidates) > limit
    report['opportunities'] = candidates if limit is None else candidates[:limit]
    return report


def match_feeders(opportunities, offers, max_price_gbp=40, min_transfer_minutes=180,
                  max_layover_minutes=1440, now=None):
    """Return priced outbound journeys; fees in different currencies stay separate."""
    current = _now(now)
    ceiling = Decimal(str(max_price_gbp))
    if not ceiling.is_finite() or ceiling <= 0:
        raise ValueError('The feeder budget must be a positive GBP amount.')
    opportunities = opportunities.get('opportunities', []) if isinstance(opportunities, dict) else opportunities
    min_transfer_minutes = max(120, int(min_transfer_minutes))
    max_layover_minutes = max(0, int(max_layover_minutes))
    age_limit = timedelta(hours=_hours_setting('AYCF_FEEDER_FARE_MAX_AGE_HOURS', 6))
    wizz_age_limit = timedelta(hours=_hours_setting('AYCF_FEEDER_WIZZ_MAX_AGE_HOURS', 24))
    by_hub = defaultdict(list)
    for raw in offers:
        try:
            offer = normalize_offer(raw, now=current)
        except (ValueError, TypeError):
            continue
        if (Decimal(offer['price_gbp']) >= ceiling or _timestamp(offer['departure']) <= current
                or current - _timestamp(offer['observed_at']) > age_limit):
            continue
        by_hub[offer['destination']].append(offer)
    results, seen = [], set()
    for opportunity in opportunities:
        if any(leg.get('origin') == 'MAN' or leg.get('destination') == 'MAN'
               for leg in opportunity.get('legs', [])):
            continue
        try:
            first, last = _timestamp(opportunity['first_departure']), _timestamp(opportunity['final_arrival'])
            checked = _timestamp(opportunity['oldest_checked'])
        except (ValueError, KeyError, TypeError):
            continue
        if current - checked > wizz_age_limit or checked > current + timedelta(minutes=5):
            continue
        for offer in by_hub.get(opportunity['hub'], []):
            wait = (first - _timestamp(offer['arrival'])).total_seconds() / 60
            if not min_transfer_minutes <= wait <= max_layover_minutes:
                continue
            signature = (opportunity['id'], offer['airline'], offer['flight_number'], offer['departure'],
                         offer['arrival'], offer['price_gbp'])
            if signature in seen:
                continue
            seen.add(signature)
            duration = int((last - _timestamp(offer['departure'])).total_seconds() // 60)
            if duration <= 0:
                continue
            results.append(dict(opportunity, feeder=offer, feeder_price_gbp=offer['price_gbp'],
                                transfer_minutes=int(wait), total_minutes=duration,
                                aycf_fee_eur='9.99', aycf_fee_count=opportunity['wizz_legs'],
                                outbound_only=True))
    results.sort(key=lambda item: (Decimal(item['feeder_price_gbp']), item['wizz_legs'],
                                   item['total_minutes'], item['feeder']['departure']))
    return results
