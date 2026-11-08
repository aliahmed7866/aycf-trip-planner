"""Direct-trip patterns from recently observed complete scan snapshots.

A pattern is evidence of past availability, not a prediction of inventory.
"""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
from contextlib import closing

from airport_catalog import airport_code, country_for
from route_history import history_db_path
from scan_scope import concrete_route_allowed, endpoint_excluded, endpoint_key, endpoint_matches
from short_trips import _flight_time, _daytime_minutes, local_datetime, UK_ZONE

UTC = timezone.utc


def historical_trip_patterns(scope, origins, returns, *, destinations=(), min_stay_hours=24,
                             min_daytime_hours=8, max_journey_minutes=0,
                             leave_after=None, return_by=None, now=None, path=None, limit=30):
    now = (now or datetime.now(UTC)).astimezone(UTC)
    cutoff = now - timedelta(days=28)
    report = {'patterns': [], 'latest_snapshot': None, 'observation_days': 0, 'error': False}
    target = Path(path or history_db_path()).expanduser()
    if not target.is_file():
        return report
    try:
        with closing(sqlite3.connect(target.resolve().as_uri() + '?mode=ro', uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            latest = conn.execute('SELECT MAX(scanned_at) FROM snapshots').fetchone()[0]
            report['latest_snapshot'] = latest
            # SQLite datetime() understands both UTC-naive and offset timestamps.
            snapshots = conn.execute('SELECT snapshot_id, scanned_at FROM snapshots WHERE datetime(scanned_at)>=datetime(?) AND datetime(scanned_at)<=datetime(?) ORDER BY scanned_at', (cutoff.isoformat(), now.isoformat())).fetchall()
            report['observation_days'] = len({str(s['scanned_at'])[:10] for s in snapshots})
            if not snapshots:
                return report
            rows = conn.execute('''SELECT f.*, s.scanned_at FROM flight_appearances f
                JOIN snapshots s ON s.snapshot_id=f.snapshot_id
                WHERE datetime(s.scanned_at)>=datetime(?) AND datetime(s.scanned_at)<=datetime(?)
                AND datetime(f.fetched_at)>=datetime(?) AND datetime(f.fetched_at)<=datetime(s.scanned_at)
                ORDER BY f.snapshot_id, f.departure''', (cutoff.isoformat(), now.isoformat(), cutoff.isoformat())).fetchall()
    except (sqlite3.Error, OSError):
        report['error'] = True
        return report

    groups = defaultdict(lambda: {'out': [], 'back': []})
    snapshot_weeks = {s['snapshot_id']: datetime.fromisoformat(s['scanned_at']).isocalendar()[:2] for s in snapshots}
    for raw in rows:
        row = dict(raw)
        origin = row.get('physical_origin') or row['origin']
        dest = row.get('physical_destination') or row['destination']
        if not airport_code(origin) or not airport_code(dest) or not concrete_route_allowed(origin, dest, scope):
            continue
        try:
            dep, arr = _flight_time(row, 'departure', origin), _flight_time(row, 'arrival', dest)
            if arr.astimezone(UTC) <= dep.astimezone(UTC) or arr.astimezone(UTC)-dep.astimezone(UTC) > timedelta(hours=18):
                continue
        except (ValueError, TypeError, OverflowError):
            continue
        if max_journey_minutes and (arr.astimezone(UTC)-dep.astimezone(UTC)).total_seconds() > max_journey_minutes * 60:
            continue
        leg = {'origin': origin, 'destination': dest, 'dep': dep, 'arr': arr, 'code': row['flight_code']}
        snap = row['snapshot_id']
        if any(endpoint_matches(origin, x) for x in origins) and country_for(dest) != 'United Kingdom' and not endpoint_excluded(dest, scope) and (not destinations or any(endpoint_matches(dest, x) for x in destinations)):
            groups[(snap, endpoint_key(dest))]['out'].append(leg)
        if any(endpoint_matches(dest, x) for x in returns) and country_for(origin) != 'United Kingdom' and not endpoint_excluded(origin, scope) and (not destinations or any(endpoint_matches(origin, x) for x in destinations)):
            groups[(snap, endpoint_key(origin))]['back'].append(leg)

    patterns = {}
    for (snapshot_id, _), legs in groups.items():
        for out in legs['out']:
            for back in legs['back']:
                stay = back['dep'].astimezone(UTC) - out['arr'].astimezone(UTC)
                if stay <= timedelta(hours=24) or stay < timedelta(hours=min_stay_hours) or stay > timedelta(days=7):
                    continue
                if _daytime_minutes(out['arr'], back['dep']) < min_daytime_hours * 60:
                    continue
                # Same actual trip seen in multiple scans counts only once.
                occurrence = (out['dep'].date().isoformat(), back['dep'].date().isoformat())
                key = (out['origin'], out['destination'], back['destination'], out['code'], back['code'],
                       out['dep'].weekday(), out['dep'].strftime('%H:%M'), out['arr'].strftime('%H:%M'),
                       (out['arr'].date()-out['dep'].date()).days, (back['arr'].date()-back['dep'].date()).days,
                       (back['dep'].date()-out['dep'].date()).days, back['dep'].strftime('%H:%M'), back['arr'].strftime('%H:%M'))
                entry = patterns.setdefault(key, {'out': out, 'back': back, 'occurrences': set(), 'snapshot_weeks': set()})
                entry['occurrences'].add(occurrence)
                entry['snapshot_weeks'].add(snapshot_weeks[snapshot_id])
    for entry in patterns.values():
        out, back = entry['out'], entry['back']
        # Only show recurring patterns; one observed trip is weak evidence.
        weeks = {datetime.fromisoformat(day).isocalendar()[:2] for day, _ in entry['occurrences']}
        if len(weeks) < 2 or len(entry['snapshot_weeks']) < 2:
            continue
        earliest = max(now, leave_after.astimezone(UTC)) if leave_after else now
        day = earliest.astimezone(UK_ZONE).date()
        offset = (out['dep'].weekday() - day.weekday()) % 7
        for extra in (offset, offset + 7):
            projected = day + timedelta(days=extra)
            try:
                shift = projected - out['dep'].date()
                departure, outbound_arrival, return_departure, uk_arrival = (
                    local_datetime(f"{stamp.date() + shift}T{stamp:%H:%M}", stamp.tzinfo)
                    for stamp in (out['dep'], out['arr'], back['dep'], back['arr'])
                )
                if departure.astimezone(UTC) < earliest:
                    continue
                if return_by and uk_arrival.astimezone(UTC) > return_by.astimezone(UTC):
                    continue
                out_utc, arrived_utc, return_utc, home_utc = (
                    stamp.astimezone(UTC) for stamp in (departure, outbound_arrival, return_departure, uk_arrival)
                )
                stay = return_utc - arrived_utc
                if stay <= timedelta(hours=24) or not timedelta(hours=min_stay_hours) <= stay <= timedelta(days=7):
                    continue
                durations = (arrived_utc - out_utc, home_utc - return_utc)
                max_duration = timedelta(minutes=min(max_journey_minutes or 1080, 1080))
                if any(not timedelta(0) < duration <= max_duration for duration in durations):
                    continue
                if _daytime_minutes(outbound_arrival, return_departure) < min_daytime_hours * 60:
                    continue
            except ValueError:
                continue
            report['patterns'].append({'origin': out['origin'], 'destination': out['destination'], 'return_airport': back['destination'],
                'outbound_code': out['code'], 'return_code': back['code'], 'outbound_day': departure.strftime('%a %d %b'),
                'outbound_time': out['dep'].strftime('%H:%M'), 'outbound_arrival': outbound_arrival.strftime('%a %d %b %H:%M'),
                'return_day': return_departure.strftime('%a %d %b'), 'return_time': back['dep'].strftime('%H:%M'),
                'return_arrival': uk_arrival.strftime('%a %d %b %H:%M'), 'weeks': min(len(weeks), len(entry['snapshot_weeks'])),
                'last_observed': max(d for d, _ in entry['occurrences']), 'sort_date': projected.isoformat()})
            break
    report['patterns'].sort(key=lambda p: (-p['weeks'], p['sort_date'], p['destination']))
    report['patterns'] = report['patterns'][:limit]
    return report
