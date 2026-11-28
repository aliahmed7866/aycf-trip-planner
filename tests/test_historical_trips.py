from datetime import datetime, timezone
from short_trips import airport_zone, local_datetime
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
                db.execute('INSERT INTO flight_appearances (snapshot_id,pdf_run_id,origin,destination,travel_date,flight_code,departure,arrival,physical_origin,physical_destination,fetched_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                    (i,'run',origin,dest,date,code,local_datetime(dep, airport_zone(origin)).isoformat(),
                     local_datetime(arr, airport_zone(dest)).isoformat(),origin,dest,day+'T07:00:00'))

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


def test_archive_without_physical_airport_columns_uses_saved_endpoints(tmp_path):
    path = tmp_path / 'history.db'
    populate(path)
    with _connect(str(path)) as conn:
        conn.execute('ALTER TABLE flight_appearances DROP COLUMN physical_origin')
        conn.execute('ALTER TABLE flight_appearances DROP COLUMN physical_destination')
    report = historical_trip_patterns({}, ['London Luton'], ['Liverpool'], now=NOW, path=path)
    assert [trip['destination'] for trip in report['patterns']] == ['Budapest']

def test_duplicate_snapshot_not_second_week(tmp_path):
    path=tmp_path/'history.db'; populate(path)
    with _connect(str(path)) as db:
        db.execute('DELETE FROM flight_appearances WHERE snapshot_id=2')
        db.execute("""INSERT INTO flight_appearances (snapshot_id,pdf_run_id,origin,destination,travel_date,flight_code,departure,arrival,physical_origin,physical_destination,fetched_at)
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


def test_new_snapshots_preserve_same_instants_as_live_flights(tmp_path):
    from cache_db import ScanCacheDB
    from scanner import Flight, _parse_dt
    from route_history import snapshot_partial_run, snapshot_latest_run
    from short_trips import released_short_trips

    path = str(tmp_path / 'history.db')
    cache = ScanCacheDB(str(tmp_path / 'live.db'))
    for i, (day, back) in enumerate([('2026-11-07', '2026-11-09'),
                                             ('2026-11-14', '2026-11-16')]):
        cache.upsert_pdf_run(day, day, day, back, 2)
        observed = day + 'T06:00:00'
        scan = cache.start_scan(day)
        for origin, dest, code, dep, arr in [
            ('Liverpool', 'Budapest', 'W1', day+'T10:00:00Z', day+'T14:00:00+01:00'),
            ('Budapest', 'Liverpool', 'W2', back+'T15:00:00+01:00', back+'T17:00:00Z'),
        ]:
            local_date = datetime.fromisoformat(dep).date()
            flight = Flight(origin, dest, code, _parse_dt(str(local_date), dep),
                            _parse_dt(str(local_date), arr), dep, arr)
            cache.replace_route_check(day, origin, dest, local_date, [flight], observed_at=observed)
        status = 'partial' if i == 0 else 'completed'
        cache.finish_scan(scan, status, 2, 2, 2)
        with cache.connect() as conn:
            conn.execute('UPDATE scan_runs SET completed_at=? WHERE id=?', (day+'T06:05:00', scan))
        if i == 0:
            assert snapshot_partial_run(cache, scan, path)['flights'] == 2
        else:
            cache.mark_pdf_scanned(day)
            assert snapshot_latest_run(cache, path)['flights'] == 2
        now = datetime.fromisoformat(observed).replace(tzinfo=timezone.utc)
        live = released_short_trips(cache, day, {}, ['Liverpool'], ['Liverpool'], now=now)
        assert live['trips'][0]['return_legs'][0]['departure'].endswith('15:00')
    result = historical_trip_patterns({}, ['Liverpool'], ['Liverpool'],
                                     now=datetime(2026, 11, 20, tzinfo=timezone.utc), path=path)
    assert result['patterns'][0]['return_time'] == '15:00'
    assert result['uncertain_times'] == 0
    with _connect(path) as conn:
        row = conn.execute("SELECT departure_utc,departure_text FROM flight_appearances WHERE flight_code='W2' ORDER BY departure LIMIT 1").fetchone()
        assert tuple(row) == ('2026-11-09T14:00:00+00:00', '2026-11-09T15:00:00+01:00')


def test_legacy_database_is_readable_before_and_after_schema_upgrade(tmp_path):
    from contextlib import closing
    import sqlite3
    path = tmp_path / 'legacy.db'
    populate(path)
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("UPDATE flight_appearances SET departure=substr(departure,1,19), arrival=substr(arrival,1,19)")
        for column in ('departure_text', 'arrival_text', 'departure_utc', 'arrival_utc'):
            conn.execute('ALTER TABLE flight_appearances DROP COLUMN ' + column)
    def check():
        report = historical_trip_patterns({}, ['London Luton'], ['Liverpool'], now=NOW, path=path)
        assert not report['error'] and not report['patterns']
        assert report['uncertain_times'] == 4 and report['observation_days'] == 2
    check()
    with closing(_connect(str(path))) as conn:
        assert conn.execute("SELECT count(*) FROM flight_appearances").fetchone()[0] == 4
    check()
