"""Report scheduled inbound route/date checks separately from flight counts."""
from datetime import date, timedelta
from scan_scope import route_requests, endpoint_matches


def inbound_coverage(db, run_id, routes, scope, returns, today=None):
    report = {'expected': 0, 'checked': 0, 'empty': 0, 'positive': 0, 'missing': 0, 'days': []}
    run = db.get_pdf_run(run_id)
    if not run:
        return report
    try:
        start = max(date.fromisoformat(str(run['departure_start'])[:10]), today or date.today())
        end = date.fromisoformat(str(run['departure_end'])[:10])
    except (ValueError, TypeError, KeyError):
        return report
    pairs = {(a, b) for a, b in routes if any(
        endpoint_matches(physical_b, selected)
        for _, physical_b in route_requests(a, b, scope) for selected in returns)}
    with db.connect() as conn:
        checks = {(r['origin'], r['destination'], r['travel_date']): int(r['flight_count'])
                  for r in conn.execute('SELECT origin,destination,travel_date,flight_count FROM route_checks WHERE pdf_run_id=? AND complete=1', (run_id,))}
    day = start
    while day <= end:
        row = {'date': day.isoformat(), 'expected': len(pairs), 'checked': 0, 'empty': 0, 'positive': 0, 'missing': 0}
        for a, b in pairs:
            count = checks.get((a, b, day.isoformat()))
            if count is None:
                row['missing'] += 1
            else:
                row['checked'] += 1
                row['positive' if count > 0 else 'empty'] += 1
        for key in ('expected', 'checked', 'empty', 'positive', 'missing'):
            report[key] += row[key]
        report['days'].append(row)
        day += timedelta(days=1)
    return report
