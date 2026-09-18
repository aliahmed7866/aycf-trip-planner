"""Carry observations across releases without certifying fresh scan coverage."""
from datetime import date, timedelta

from airport_catalog import airport_code
from scan_scope import route_requests


def preserve_inventory(db, run_id, routes, scope):
    if not run_id:
        return
    allowed = {
        (a, b): {(airport_code(x) or x, airport_code(y) or y)
                 for x, y in route_requests(a, b, scope)}
        for a, b in routes
    }
    with db.connect() as conn:
        # Serialize against scanner writes: never overwrite a new observation.
        conn.execute('BEGIN IMMEDIATE')
        candidates = conn.execute('''SELECT c.* FROM route_checks c
            WHERE c.pdf_run_id<>? AND c.travel_date>=?
              AND NOT EXISTS (SELECT 1 FROM route_checks n WHERE n.pdf_run_id=?
                AND n.origin=c.origin AND n.destination=c.destination
                AND n.travel_date=c.travel_date)
            ORDER BY c.fetched_at DESC, c.rowid DESC''',
            (run_id, date.today().isoformat(), run_id)).fetchall()
        seen = set()
        for check in candidates:
            group = (check['origin'], check['destination'], check['travel_date'])
            if group in seen:
                continue
            seen.add(group)
            wanted = allowed.get(group[:2], set())
            if not wanted:
                continue
            rows = conn.execute('''SELECT * FROM route_flights WHERE pdf_run_id=?
                AND origin=? AND destination=? AND travel_date=?''',
                (check['pdf_run_id'], *group)).fetchall()
            rows = [row for row in rows if row['physical_origin'] and row['physical_destination']
                    and (airport_code(row['physical_origin']) or row['physical_origin'],
                         airport_code(row['physical_destination']) or row['physical_destination']) in wanted]
            if not rows:
                continue
            for row in rows:
                values = dict(row)
                values['pdf_run_id'] = run_id
                columns = list(values)
                conn.execute('INSERT INTO route_flights (' + ','.join(columns) +
                             ') VALUES (' + ','.join('?' for _ in columns) + ')', list(values.values()))
            # Incomplete checks stay visible but cannot skip today's live scan.
            conn.execute('''INSERT INTO route_checks
                (pdf_run_id,origin,destination,travel_date,fetched_at,flight_count,complete)
                VALUES (?,?,?,?,?,?,0)''', (run_id, *group, check['fetched_at'], len(rows)))


def prepare_inventory(db, run_id, generated, frame, routes, scope, scope_id):
    """Make the new release readable even before a worker has started."""
    if not run_id:
        return None
    run = db.get_pdf_run(run_id)
    if not run:
        start = str(frame["availability_start"].iloc[0]) if "availability_start" in frame.columns else date.today().isoformat()
        end = str(frame["availability_end"].iloc[0]) if "availability_end" in frame.columns else (date.today() + timedelta(days=3)).isoformat()
        db.upsert_pdf_run(run_id, generated, start, end, len(routes), scope_id=scope_id, scope=scope)
        run = db.get_pdf_run(run_id)
    if not run.get("scanned_at"):
        preserve_inventory(db, run_id, routes, scope)
    return run
