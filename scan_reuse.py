"""Reuse verified airport coverage without renewing its observation timestamp."""
import json
from datetime import datetime, timezone

from airport_catalog import airport_code


def _pairs(values):
    return {(airport_code(a) or a, airport_code(b) or b) for a, b in values}


def reuse_check(db, run_id, origin, destination, day, requests, ttl_for_count):
    """Copy fresh, proven coverage from another scope of the same PDF release.

    Legacy rows without concrete request metadata, partial groups and changed
    airport coverage cannot establish a complete empty result. A narrower group
    can reuse a verified superset, filtering out its newly excluded airports.
    """
    wanted = _pairs(requests)
    if not wanted:
        return None
    with db.connect() as conn:
        # A current partial observation is newer evidence than another scope's
        # complete snapshot. Finish its pending work rather than overwriting it.
        if conn.execute('SELECT 1 FROM route_checks WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date=?',
                        (run_id, origin, destination, day.isoformat())).fetchone():
            return None
        candidates = conn.execute('''SELECT c.* FROM route_checks c
            JOIN pdf_runs old ON old.run_id=c.pdf_run_id
            JOIN pdf_runs current ON current.run_id=? AND current.generated_at=old.generated_at
            WHERE c.pdf_run_id<>? AND c.origin=? AND c.destination=? AND c.travel_date=?
            AND c.complete=1 AND c.request_pairs_json IS NOT NULL
            ORDER BY c.fetched_at DESC''', (run_id, run_id, origin, destination, day.isoformat())).fetchall()
        for candidate in candidates:
            try:
                if not wanted <= _pairs(json.loads(candidate['request_pairs_json'])):
                    continue
                fetched = datetime.fromisoformat(candidate['fetched_at'])
                if fetched.tzinfo is None:
                    fetched = fetched.replace(tzinfo=timezone.utc)
                age = (datetime.now(timezone.utc) - fetched).total_seconds()
                ttl = ttl_for_count(candidate['flight_count'])
                if ttl <= 0 or not 0 <= age <= ttl:
                    continue
            except (ValueError, TypeError):
                continue
            flights = conn.execute('''SELECT * FROM route_flights WHERE pdf_run_id=?
                AND origin=? AND destination=? AND travel_date=?''',
                (candidate['pdf_run_id'], origin, destination, day.isoformat())).fetchall()
            if len(flights) != candidate['flight_count'] or any(
                    not row['physical_origin'] or not row['physical_destination'] for row in flights):
                continue
            kept = [row for row in flights if _pairs([(row['physical_origin'], row['physical_destination'])]) <= wanted]
            # Replace this group's old partial inventory atomically. Never copy
            # rows from a different date/catalogue or rewrite fetched_at as now.
            conn.execute('DELETE FROM route_flights WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date=?',
                         (run_id, origin, destination, day.isoformat()))
            for row in kept:
                values = dict(row)
                values['pdf_run_id'] = run_id
                columns = list(values)
                conn.execute('INSERT INTO route_flights (' + ','.join(columns) + ') VALUES (' +
                             ','.join('?' for _ in columns) + ')', list(values.values()))
            conn.execute('''INSERT OR REPLACE INTO route_checks
                (pdf_run_id,origin,destination,travel_date,fetched_at,flight_count,complete,request_pairs_json)
                VALUES (?,?,?,?,?,?,1,?)''', (run_id, origin, destination, day.isoformat(),
                candidate['fetched_at'], len(kept), json.dumps(list(requests))))
            return len(kept)
    return None
