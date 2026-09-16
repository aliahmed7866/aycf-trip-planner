"""Choose useful Manchester fare checks from current, exact-airport AYCF paths.

History is advisory: it never creates availability. Repeated scans and reused
cache rows count once per actual observation day. City-wide history cannot
establish a physical airport's availability rate.
"""
from collections import defaultdict
from contextlib import closing
from datetime import date, datetime, time, timedelta
from pathlib import Path
import sqlite3

from feeder_trips import DEFAULT_COUNTRIES, UTC, _now, _physical_code, _timestamp
from route_history import history_db_path
from short_trips import UK_ZONE


def _history_evidence(path, now):
    """Read at most 60 days without creating or modifying a history database."""
    start = now.date() - timedelta(days=59)
    evidence = defaultdict(lambda: {'checked': set(), 'seen': set()})
    try:
        target = Path(path or history_db_path()).expanduser().resolve()
        if not target.is_file():
            return {}
        with closing(sqlite3.connect(target.as_uri() + '?mode=ro', uri=True, timeout=.25)) as conn:
            conn.row_factory = sqlite3.Row
            # Snapshot dates bound the work. fetched_at below prevents cache reuse
            # on a newer snapshot from becoming a new observation.
            params = ((start - timedelta(days=1)).isoformat(),
                      (now.date() + timedelta(days=1)).isoformat())
            checks = conn.execute('''
                SELECT r.origin, r.destination, r.flight_count, r.fetched_at, s.scanned_at
                  FROM route_appearances r JOIN snapshots s USING(snapshot_id)
                 WHERE substr(s.scanned_at,1,10) BETWEEN ? AND ?
            ''', params)
            for index, row in enumerate(checks):
                if index >= 250000:
                    return {}  # A truncated denominator must never inflate a rate.
                pair = (_physical_code(row['origin']), _physical_code(row['destination']))
                day = _observation_day(row['fetched_at'], row['scanned_at'], now, start)
                if all(pair) and day:
                    evidence[pair]['checked'].add(day)
                    if int(row['flight_count'] or 0) > 0:
                        evidence[pair]['seen'].add(day)
            flights = conn.execute('''
                SELECT f.origin, f.destination, f.physical_origin, f.physical_destination,
                       f.fetched_at, s.scanned_at
                  FROM flight_appearances f JOIN snapshots s USING(snapshot_id)
                 WHERE substr(s.scanned_at,1,10) BETWEEN ? AND ?
            ''', params)
            for index, row in enumerate(flights):
                if index >= 250000:
                    return {}
                pair = (_physical_code(row['physical_origin'] or row['origin']),
                        _physical_code(row['physical_destination'] or row['destination']))
                day = _observation_day(row['fetched_at'], row['scanned_at'], now, start)
                if all(pair) and day:
                    evidence[pair]['seen'].add(day)
    except (OSError, sqlite3.Error, ValueError, TypeError):
        return {}
    return dict(evidence)


def _observation_day(fetched, scanned, now, start):
    try:
        observed = _timestamp(fetched or scanned)
    except (ValueError, TypeError, OverflowError):
        return None
    if observed > now + timedelta(minutes=5) or observed.date() < start:
        return None
    return observed.date()


def _path_history(legs, evidence, now):
    """Use the weakest leg: stable first legs cannot hide an unproven onward leg."""
    routes = []
    recent = now.date() - timedelta(days=29)
    for leg in legs:
        raw = evidence.get((leg['origin'], leg['destination']), {})
        checked, seen = raw.get('checked', set()), raw.get('seen', set())
        recent_checked = {day for day in checked if day >= recent}
        window = 30 if len(recent_checked) >= 3 else 60
        if window == 30:
            checked = recent_checked
            seen = {day for day in seen if day >= recent}
        positive = len(checked & seen)
        rate = positive / len(checked) if checked else None
        stable = positive >= 3 and rate is not None and rate >= .6
        routes.append({'history_days': len(seen), 'checked_days': len(checked),
                       'history_rate': round(100 * rate, 1) if rate is not None else None,
                       'stable': stable, 'history_window_days': window})
    weakest = min(routes, key=lambda item: (item['stable'], item['checked_days'] >= 3,
                                           item['history_rate'] or 0, item['history_days']))
    result = dict(weakest)
    result['stable'] = all(item['stable'] for item in routes)
    result['stability_label'] = ('Stable recent history' if result['stable'] else
                                 'Variable history' if result['checked_days'] >= 3 else
                                 'Limited history')
    return result


def _minutes(report, key, default, minimum):
    try:
        return max(minimum, min(10080, int(report.get(key, default))))
    except (ValueError, TypeError, OverflowError):
        return default


def _eligible_dates(opportunity, report, now):
    """Prune broad core dates using a practical one-to-eight-hour feeder window.

    A query is still only a candidate: actual departure/arrival times must pass
    match_feeders before a priced connection is shown. All query dates are in
    Manchester time, including DST changes.
    """
    departure = _timestamp(opportunity['first_departure'])
    minimum = _minutes(report, 'min_transfer_minutes', 180, 120)
    maximum = _minutes(report, 'max_layover_minutes', 1440, 0)
    if maximum < minimum:
        return []
    earliest = max(now, departure - timedelta(minutes=maximum, hours=8))
    latest = departure - timedelta(minutes=minimum, hours=1)
    if latest <= now or earliest > latest:
        return []
    dates = []
    for value in sorted(set(opportunity.get('feeder_dates', []))):
        try:
            day = date.fromisoformat(value)
            beginning = datetime.combine(day, time.min, UK_ZONE).astimezone(UTC)
            end = datetime.combine(day + timedelta(days=1), time.min, UK_ZONE).astimezone(UTC)
        except (ValueError, TypeError, OverflowError):
            continue
        if beginning <= latest and end > earliest:
            dates.append(day.isoformat())
    return dates


def rank_feeder_targets(report, history_path=None, now=None):
    """Return one ranked request per current eligible exact hub/Manchester date.

    Optional report min_transfer_minutes/max_layover_minutes match the search
    constraints; defaults are three and 24 hours. A stable label needs at least
    three positive observation days and >=60% of exact-route checked days.
    Every leg must qualify. This is route recurrence, not a guarantee of seats,
    a timetable, or a probability of making a self-transfer.
    """
    now = _now(now)
    evidence = _history_evidence(history_path, now)
    grouped = defaultdict(dict)
    for opportunity in report.get('opportunities', []):
        try:
            hub = _physical_code(opportunity.get('hub'))
            legs = opportunity.get('legs') or []
            if (not hub or hub == 'MAN' or not 1 <= len(legs) <= 2
                    or opportunity.get('country') not in DEFAULT_COUNTRIES
                    or legs[0]['origin'] != hub
                    or legs[-1]['destination'] != opportunity.get('destination')
                    or any(_physical_code(leg['origin']) != leg['origin']
                           or _physical_code(leg['destination']) != leg['destination']
                           for leg in legs)):
                continue
            history = _path_history(legs, evidence, now)
            signature = tuple((leg['origin'], leg['destination'], leg['departure'],
                               leg.get('flight_number', '')) for leg in legs)
            for day in _eligible_dates(opportunity, report, now):
                grouped[(hub, day)][signature] = (opportunity, history)
        except (ValueError, TypeError, KeyError, OverflowError):
            continue
    targets = []
    for (hub, day), unique in grouped.items():
        items = list(unique.values())
        # Prefer a usable direct Wizz route, then evidence for its weakest leg.
        best, history = max(items, key=lambda pair: (
            len(pair[0]['legs']) == 1, pair[1]['stable'],
            pair[1]['history_rate'] or 0, pair[1]['history_days'],
            pair[0]['destination']))
        direct = sum(len(item['legs']) == 1 for item, _ in items)
        countries = sorted({item['country'] for item, _ in items})
        destinations = sorted({item.get('destination_name') or item['destination'] for item, _ in items})
        departure_day = _timestamp(best['first_departure']).astimezone(UK_ZONE).date()
        day_gap = (departure_day - date.fromisoformat(day)).days
        day_bonus = 20 if day_gap == 0 else 10 if day_gap == 1 else 0
        score = ((1000 if direct else 0) + (400 if history['stable'] else 0)
                 + 2 * (history['history_rate'] or 0) + min(history['history_days'], 20) * 5
                 + 50 * len(countries) + 5 * min(len(items), 10) + day_bonus)
        connection_text = f"{direct} direct AYCF option{'s' if direct != 1 else ''}" if direct else 'Two AYCF legs required'
        if history['checked_days'] >= 3:
            evidence_text = (f"{history['history_rate']:g}% of {history['checked_days']} checked days "
                             f"in the last {history['history_window_days']} days")
        else:
            evidence_text = 'Limited exact-airport history; current availability only'
        targets.append({'hub': hub, 'date': day, 'hub_name': best.get('hub_name') or hub,
                        'destinations': destinations, 'countries': countries,
                        **history, 'opportunity_count': len(items), 'direct_count': direct,
                        'reason': f"{connection_text}; {evidence_text}.", 'score': round(score, 1)})
    targets.sort(key=lambda item: (-item['score'], item['date'], item['hub']))
    return targets
