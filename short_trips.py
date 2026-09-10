"""Pair released cached flights into UK round trips, without live requests.

All eligibility precedes the display limit. Airport-local clocks are converted
before arithmetic; unknown/ambiguous clocks are omitted instead of guessed.
"""
from collections import defaultdict
from datetime import datetime, time, timedelta, timezone
from heapq import nsmallest
from math import ceil
from functools import lru_cache

from dateutil import parser, tz
from airport_catalog import airport_code, country_for
from scan_scope import concrete_route_allowed, endpoint_key, endpoint_matches

UTC = timezone.utc
UK_ZONE = tz.gettz('Europe/London')
COUNTRY_ZONES = {
    'Albania': 'Europe/Tirane', 'Armenia': 'Asia/Yerevan', 'Austria': 'Europe/Vienna',
    'Azerbaijan': 'Asia/Baku', 'Belgium': 'Europe/Brussels', 'Bosnia and Herzegovina': 'Europe/Sarajevo',
    'Bulgaria': 'Europe/Sofia', 'Croatia': 'Europe/Zagreb', 'Cyprus': 'Asia/Nicosia',
    'Czechia': 'Europe/Prague', 'Denmark': 'Europe/Copenhagen', 'Egypt': 'Africa/Cairo',
    'Estonia': 'Europe/Tallinn', 'Finland': 'Europe/Helsinki', 'France': 'Europe/Paris',
    'Georgia': 'Asia/Tbilisi', 'Germany': 'Europe/Berlin', 'Greece': 'Europe/Athens',
    'Hungary': 'Europe/Budapest', 'Iceland': 'Atlantic/Reykjavik', 'Israel': 'Asia/Jerusalem',
    'Italy': 'Europe/Rome', 'Jordan': 'Asia/Amman', 'Kazakhstan': 'Asia/Almaty',
    'Kosovo': 'Europe/Belgrade', 'Kuwait': 'Asia/Kuwait', 'Latvia': 'Europe/Riga',
    'Lithuania': 'Europe/Vilnius', 'Malta': 'Europe/Malta', 'Moldova': 'Europe/Chisinau',
    'Montenegro': 'Europe/Podgorica', 'Morocco': 'Africa/Casablanca', 'Netherlands': 'Europe/Amsterdam',
    'North Macedonia': 'Europe/Skopje', 'Norway': 'Europe/Oslo', 'Poland': 'Europe/Warsaw',
    'Portugal': 'Europe/Lisbon', 'Romania': 'Europe/Bucharest', 'Saudi Arabia': 'Asia/Riyadh',
    'Serbia': 'Europe/Belgrade', 'Slovakia': 'Europe/Bratislava', 'Slovenia': 'Europe/Ljubljana',
    'Spain': 'Europe/Madrid', 'Sweden': 'Europe/Stockholm', 'Switzerland': 'Europe/Zurich',
    'Türkiye': 'Europe/Istanbul', 'United Arab Emirates': 'Asia/Dubai',
    'United Kingdom': 'Europe/London', 'Uzbekistan': 'Asia/Tashkent',
}


@lru_cache(maxsize=1024)
def airport_zone(name):
    code = airport_code(name)
    zone = ('Atlantic/Canary' if code in {'LPA', 'TFS', 'FUE', 'ACE'} else
            'Atlantic/Madeira' if code == 'FNC' else COUNTRY_ZONES.get(country_for(name)))
    return tz.gettz(zone) if zone else None


def local_datetime(value, zone):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        return parsed.astimezone(zone)
    parsed = parsed.replace(tzinfo=zone)
    if not tz.datetime_exists(parsed) or tz.datetime_ambiguous(parsed):
        raise ValueError('Choose an unambiguous local time outside the clock change.')
    return parsed


def _flight_time(row, field, airport):
    zone = airport_zone(airport)
    if zone is None:
        raise ValueError('Unknown airport timezone')
    raw = str(row.get(field + '_text') or '')
    # The scanner normalises offset-bearing input to naive UTC, but retains
    # the original text. Recover its offset before interpreting local clocks.
    if raw:
        original = parser.parse(raw, default=datetime(2000, 1, 1))
        if original.tzinfo is not None:
            return original.astimezone(zone)
    return local_datetime(row[field], zone)


def _matches(name, choices):
    return any(endpoint_matches(name, choice) for choice in choices)


def _daytime_minutes(arrival, departure):
    """09:00–21:00 local time after 90m arrival and 120m departure buffers."""
    start, end = arrival + timedelta(minutes=90), departure - timedelta(minutes=120)
    minutes = 0
    day = start.date()
    while day <= end.date():
        opening = datetime.combine(day, time(9), arrival.tzinfo)
        closing = datetime.combine(day, time(21), arrival.tzinfo)
        lo, hi = max(start, opening), min(end, closing)
        minutes += max(0, (hi.astimezone(UTC) - lo.astimezone(UTC)).total_seconds() / 60)
        day += timedelta(days=1)
    return int(minutes)


def released_short_trips(db, run_id, scope, origins, returns, *, leave_after=None,
                         return_by=None, destinations=(), min_stay_hours=24,
                         min_daytime_hours=8, max_stops=0, min_transfer_minutes=150,
                         max_layover_minutes=12 * 60, max_journey_minutes=0,
                         now=None, limit=100, work_limit=200000):
    now = (now or datetime.now(UTC)).astimezone(UTC)
    earliest = max(now, leave_after.astimezone(UTC)) if leave_after else now
    latest = return_by.astimezone(UTC) if return_by else None
    max_stops = max(0, min(2, int(max_stops)))
    min_stay_hours = max(24, int(min_stay_hours))
    min_transfer_minutes = max(120, int(min_transfer_minutes))
    report = {'trips': [], 'total': 0, 'limited': False, 'incomplete': False,
              'omitted_times': 0, 'flight_count': 0, 'window_start': None, 'window_end': None}
    run = db.get_pdf_run(run_id)
    if not run or not run.get('scanned_at'):
        return report
    # Restrict every leg to the current completed run AND a successful route check.
    with db.connect() as conn:
        rows = [dict(r) for r in conn.execute('''
            SELECT f.* FROM route_flights f JOIN route_checks c
              ON f.pdf_run_id=c.pdf_run_id AND f.origin=c.origin
             AND f.destination=c.destination AND f.travel_date=c.travel_date
            WHERE f.pdf_run_id=? ORDER BY f.departure, f.flight_code
        ''', (run_id,))]
    outgoing, incoming, seen = defaultdict(list), defaultdict(list), set()
    allowed_dates = (str(run.get('departure_start') or '')[:10], str(run.get('departure_end') or '')[:10])
    for row in rows:
        origin = row.get('physical_origin') or row['origin']
        destination = row.get('physical_destination') or row['destination']
        # A grouped airport cannot prove a physical return or connection.
        if not airport_code(origin) or not airport_code(destination):
            report['omitted_times'] += 1
            continue
        if not concrete_route_allowed(origin, destination, scope):
            continue
        if ((allowed_dates[0] and row['travel_date'] < allowed_dates[0]) or
                (allowed_dates[1] and row['travel_date'] > allowed_dates[1])):
            continue
        try:
            dep_local = _flight_time(row, 'departure', origin)
            arr_local = _flight_time(row, 'arrival', destination)
            dep, arr = dep_local.astimezone(UTC), arr_local.astimezone(UTC)
            if dep_local.date().isoformat() != row['travel_date']:
                raise ValueError('Departure does not match the checked travel date')
            # Time-only feeds can roll an arrival forward based on local clock
            # order even when a westbound same-day flight is valid. Correct only
            # that provable clock-only case, never full dated timestamps.
            if ':' in str(row.get('arrival_text') or '') and len(str(row['arrival_text'])) <= 8:
                previous = arr_local - timedelta(days=1)
                if previous.astimezone(UTC) > dep:
                    arr_local, arr = previous, previous.astimezone(UTC)
            if arr <= dep or arr - dep > timedelta(hours=18):
                raise ValueError('Invalid flight duration')
        except (ValueError, TypeError, OverflowError):
            report['omitted_times'] += 1
            continue
        if dep < earliest or (latest and arr > latest):
            continue
        sig = (endpoint_key(origin), endpoint_key(destination), row['flight_code'], dep, arr)
        if sig in seen:
            continue
        seen.add(sig)
        leg = dict(row, origin=origin, destination=destination, dep=dep, arr=arr,
                   dep_local=dep_local, arr_local=arr_local)
        outgoing[endpoint_key(origin)].append(leg)
        incoming[endpoint_key(destination)].append(leg)
    flights = [leg for legs in outgoing.values() for leg in legs]
    report['flight_count'] = len(flights)
    if not flights:
        return report
    report['window_start'] = min(f['dep_local'].date() for f in flights).isoformat()
    report['window_end'] = max(f['dep_local'].date() for f in flights).isoformat()
    hubs = scope.get('connection_hubs') or []
    preferred = scope.get('preferred_destinations') or []
    budget = [0]

    def spend():
        budget[0] += 1
        if budget[0] > work_limit:
            report['incomplete'] = True
            return False
        return True

    def journeys(reverse=False):
        adjacency = incoming if reverse else outgoing
        selections = returns if reverse else origins
        starts = [f for f in flights if _matches(f['destination' if reverse else 'origin'], selections)]
        def walk(legs, visited):
            if not spend():
                return
            first, last = legs[0], legs[-1]
            if max_journey_minutes and (last['arr'] - first['dep']).total_seconds() / 60 > max_journey_minutes:
                return
            endpoint = first['origin'] if reverse else last['destination']
            if country_for(endpoint) == 'United Kingdom':
                return
            if not destinations or _matches(endpoint, destinations):
                yield legs
            if len(legs) >= max_stops + 1 or not _matches(endpoint, hubs):
                return
            for nxt in adjacency.get(endpoint_key(endpoint), []):
                new_endpoint = nxt['origin'] if reverse else nxt['destination']
                if endpoint_key(new_endpoint) in visited:
                    continue
                wait = ((first['dep'] - nxt['arr']) if reverse else (nxt['dep'] - last['arr'])).total_seconds() / 60
                if wait < min_transfer_minutes or (max_layover_minutes and wait > max_layover_minutes):
                    continue
                yield from walk(([nxt] + legs) if reverse else (legs + [nxt]), visited | {endpoint_key(new_endpoint)})
                if report['incomplete']:
                    return
        for leg in starts:
            yield from walk([leg], {endpoint_key(leg['origin']), endpoint_key(leg['destination'])})
            if report['incomplete']:
                return

    return_options = defaultdict(list)
    for legs in journeys(reverse=True):
        return_options[endpoint_key(legs[0]['origin'])].append(legs)

    def public_leg(leg):
        return {key: leg[key] for key in ('origin', 'destination', 'flight_code', 'fetched_at')} | {
            'departure': leg['dep_local'].strftime('%a %d %b %H:%M'),
            'arrival': leg['arr_local'].strftime('%a %d %b %H:%M'),
        }

    def candidates():
        for out in journeys():
            destination = out[-1]['destination']
            for ret in return_options[endpoint_key(destination)]:
                if not spend():
                    return
                stay = (ret[0]['dep'] - out[-1]['arr']).total_seconds() / 3600
                if stay <= 24 or stay < min_stay_hours:
                    continue
                daylight = _daytime_minutes(out[-1]['arr_local'], ret[0]['dep_local'])
                if daylight < max(0, min_daytime_hours) * 60:
                    continue
                travel = ((out[-1]['arr'] - out[0]['dep']) + (ret[-1]['arr'] - ret[0]['dep'])).total_seconds() / 60
                report['total'] += 1
                yield {'destination': destination, 'origin': out[0]['origin'], 'return_airport': ret[-1]['destination'],
                       'stay_hours': round(stay, 1),
                       'stay_label': f'{ceil(stay * 60) // 60}h {ceil(stay * 60) % 60}m',
                       'daytime_hours': round(daylight / 60, 1),
                       'travel_hours': round(travel / 60, 1), 'stops': len(out) + len(ret) - 2,
                       'preferred': _matches(destination, preferred),
                       'different_airports': endpoint_key(out[0]['origin']) != endpoint_key(ret[-1]['destination']),
                       'outbound': [public_leg(f) for f in out], 'return_legs': [public_leg(f) for f in ret],
                       'last_checked': min(f['fetched_at'] for f in out + ret),
                       '_rank': (not _matches(destination, preferred), -daylight, len(out) + len(ret), travel, out[0]['dep'])}
    report['trips'] = nsmallest(max(1, limit), candidates(), key=lambda r: r['_rank'])
    report['limited'] = report['total'] > len(report['trips'])
    for row in report['trips']:
        del row['_rank']
    return report
