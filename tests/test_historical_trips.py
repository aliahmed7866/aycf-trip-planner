from datetime import datetime, timezone
from route_history import _connect
from historical_trips import historical_trip_patterns

NOW = datetime(2026, 10, 30, 0, tzinfo=timezone.utc)

def populate(path):
    with _connect(str(path)) as db:
        for i, day in enumerate(('2026-10-16', '2026-10-23'), 1):
            db.execute('INSERT INTO snapshots VALUES(?,?,?,?,?)', (i, i, 'run', day + 'T07:00:00', day + 'T07:00:00'))
            return_day = '2026-10-18' if i == 1 else '2026-10-25'
            for origin, dest, date, code, dep, arr in [
                ('London Luton', 'Budapest', day, 'W1', day+'T08:00:00', day+'T11:30:00'),
                ('Budapest', 'Liverpool', return_day, 'W2', return_day+'T16:00:00', return_day+'T18:00:00')]:
                db.execute('INSERT INTO flight_appearances VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                    (i,'run',origin,dest,date,code,dep,arr,origin,dest,day+'T07:00:00'))

def test_patterns_require_independent_weeks_and_honor_choices(tmp_path):
    path = tmp_path/'history.db'; populate(path)
    result = historical_trip_patterns({}, ['London Luton'], ['Liverpool'], now=NOW, path=str(path))
    assert len(result['patterns']) == 1
    assert result['patterns'][0]['weeks'] == 2
    assert result['patterns'][0]['outbound_day'] == 'Fri 30 Oct'
    assert not historical_trip_patterns({}, ['Birmingham'], ['Liverpool'], now=NOW, path=str(path))['patterns']
    assert not historical_trip_patterns({}, ['London Luton'], ['Liverpool'], now=NOW, path=str(path), destinations=['Rome'])['patterns']
    assert not historical_trip_patterns({'excluded_countries':['Hungary']}, ['London Luton'], ['Liverpool'], now=NOW, path=str(path))['patterns']

def test_stale_or_missing_history_does_not_create_database(tmp_path):
    path = tmp_path/'history.db'; populate(path)
    result = historical_trip_patterns({}, ['London Luton'], ['Liverpool'], now=datetime(2026,12,1,tzinfo=timezone.utc), path=str(path))
    assert not result['patterns'] and result['observation_days'] == 0
    assert result['latest_snapshot'].startswith('2026-10-23')
    missing = tmp_path/'missing.db'
    assert not historical_trip_patterns({}, [], [], path=str(missing))['patterns']
    assert not missing.exists()

def test_duplicate_snapshot_not_second_week(tmp_path):
    path=tmp_path/'history.db'; populate(path)
    with _connect(str(path)) as db:
        db.execute('DELETE FROM flight_appearances WHERE snapshot_id=2')
        db.execute("""INSERT INTO flight_appearances
            SELECT 2, pdf_run_id, origin, destination, travel_date, flight_code,
                   departure, arrival, physical_origin, physical_destination, fetched_at
            FROM flight_appearances WHERE snapshot_id=1""")
    assert not historical_trip_patterns({}, ['London Luton'], ['Liverpool'], now=NOW, path=str(path))['patterns']

def test_projected_return_respects_exact_deadline(tmp_path):
    path = tmp_path / 'history.db'
    populate(path)
    inputs = dict(now=NOW, path=str(path))
    deadline = datetime(2026, 11, 1, 18, tzinfo=timezone.utc)
    result = historical_trip_patterns({}, ['London Luton'], ['Liverpool'], return_by=deadline, **inputs)
    assert result['patterns'][0]['return_arrival'] == 'Sun 01 Nov 18:00'
    early = deadline.replace(hour=17, minute=59)
    assert not historical_trip_patterns({}, ['London Luton'], ['Liverpool'], return_by=early, **inputs)['patterns']


def test_partial_scan_saves_only_same_day_positive_evidence(tmp_path):
    import sqlite3

    from cache_db import ScanCacheDB
    from route_history import snapshot_partial_run

    cache = ScanCacheDB(str(tmp_path / 'scan.db'))
    history = str(tmp_path / 'history.db')
    with cache.connect() as conn:
        conn.execute("""
            INSERT INTO scan_runs (id, pdf_run_id, started_at, completed_at, status)
            VALUES (4, 'pdf', '2026-10-23T08:00:00', '2026-10-23T09:00:00', 'partial')
        """)
        for code, fetched in [('W1', '2026-10-23T07:00:00'), ('W2', '2026-10-22T07:00:00'),
                              ('W3', '2026-10-23T10:00:00')]:
            conn.execute("""
                INSERT INTO route_flights
                (pdf_run_id, origin, destination, travel_date, flight_code, departure, arrival, fetched_at)
                VALUES ('pdf', 'LTN', 'BUD', '2026-10-24', ?, '2026-10-24T08:00:00',
                        '2026-10-24T11:00:00', ?)
            """, (code, fetched))
    assert snapshot_partial_run(cache, 4, history)['flights'] == 1
    assert snapshot_partial_run(cache, 4, history)['skipped']
    with sqlite3.connect(history) as conn:
        assert conn.execute('SELECT flight_code, fetched_at FROM flight_appearances').fetchall() == [
            ('W1', '2026-10-23T07:00:00')]
        assert conn.execute('SELECT COUNT(*) FROM route_appearances').fetchone()[0] == 0
