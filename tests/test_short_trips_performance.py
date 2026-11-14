"""Guard expensive work by counts, not machine-dependent wall-clock limits."""
from datetime import datetime, timezone
from unittest.mock import patch

from cache_db import ScanCacheDB
from historical_trips import historical_trip_patterns
from route_history import _connect
from scanner import Flight
from short_trips import released_short_trips

NOW = datetime(2026, 10, 30, tzinfo=timezone.utc)


def history(path):
    with _connect(str(path)) as conn:
        for i, day in enumerate((16, 23), 1):
            stamp = f'2026-10-{day}T07:00:00'
            conn.execute('INSERT INTO snapshots VALUES(?,?,?,?,?)', (i, i, 'run', stamp, stamp))
            for a, b, dep, arr in [
                ('London Luton', 'Budapest', f'2026-10-{day}T08:00', f'2026-10-{day}T11:30'),
                ('Budapest', 'Liverpool', f'2026-10-{day+2}T16:00', f'2026-10-{day+2}T18:00'),
            ]:
                conn.execute('INSERT INTO flight_appearances VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                             (i, 'run', a, b, dep[:10], 'W1', dep, arr, a, b, stamp))


def test_large_unrelated_history_is_filtered_before_clock_parsing(tmp_path):
    import historical_trips as module
    path = tmp_path / 'history.db'
    history(path)
    expected = historical_trip_patterns({}, ['London'], ['Liverpool'], now=NOW, path=path)
    assert expected['patterns']
    with _connect(str(path)) as conn:
        # Match the user's ~30,000 flight history records, on other UK routes.
        conn.executemany('INSERT INTO flight_appearances VALUES(?,?,?,?,?,?,?,?,?,?,?)', [
            (1, 'run', 'Birmingham', 'Budapest', '2026-10-16', f'OTHER{i}',
             '2026-10-16T08:00', '2026-10-16T11:30', 'Birmingham', 'Budapest', '2026-10-16T07:00')
            for i in range(30000)
        ])
    with patch.object(module, '_flight_time', wraps=module._flight_time) as parse:
        actual = historical_trip_patterns({}, ['London'], ['Liverpool'], now=NOW, path=path)
    assert actual == expected
    assert parse.call_count == 8  # Only the four relevant legs need clock parsing.
    # Filter changes must not reuse cached suggestions from a previous request.
    assert not historical_trip_patterns({'excluded_countries': ['Hungary']},
        ['London'], ['Liverpool'], now=NOW, path=path)['patterns']


def test_one_observation_week_does_not_read_flight_history(tmp_path):
    import historical_trips as module
    path = tmp_path / 'history.db'
    history(path)
    with _connect(str(path)) as conn:
        conn.execute('DELETE FROM snapshots WHERE snapshot_id=1')
    statements = []
    connect = module.sqlite3.connect
    def traced(*args, **kwargs):
        conn = connect(*args, **kwargs)
        conn.set_trace_callback(statements.append)
        return conn
    with patch.object(module.sqlite3, 'connect', side_effect=traced):
        result = historical_trip_patterns({}, ['London'], ['Liverpool'], now=NOW, path=path)
    assert result['observation_days'] == 1
    assert result['latest_snapshot'] == '2026-10-23T07:00:00'
    assert result['patterns'] == [] and not result['error']
    assert not any('flight_appearances' in sql for sql in statements)


def test_dense_results_reuse_daytime_work_and_keep_full_ranking(tmp_path):
    import short_trips as module
    db = ScanCacheDB(str(tmp_path / 'live.db'))
    db.upsert_pdf_run('run', '2026-10-30', '2026-10-30', '2026-11-02', 2)
    for a, b, dep, arr in [
        ('London Luton', 'Budapest', datetime(2026, 10, 30, 8), datetime(2026, 10, 30, 11)),
        ('Budapest', 'Liverpool', datetime(2026, 11, 1, 16), datetime(2026, 11, 1, 18)),
    ]:
        db.replace_route_check('run', a, b, dep.date(),
            [Flight(a, b, f'W{i:03}', dep, arr, '', '') for i in range(80)])
    kwargs = dict(now=NOW, limit=7)
    with patch.object(module, '_daytime_minutes', wraps=module._daytime_minutes) as daytime:
        result = released_short_trips(db, 'run', {}, ['London Luton'], ['Liverpool'], **kwargs)
    assert result['total'] == 6400 and result['limited'] and not result['incomplete']
    assert daytime.call_count == 1
    full = released_short_trips(db, 'run', {}, ['London Luton'], ['Liverpool'], now=NOW, limit=6400)
    assert result['trips'] == full['trips'][:7]
    assert result['trips'][0]['outbound'][0]['departure'] == 'Fri 30 Oct 08:00'
    db.replace_route_check('run', 'Budapest', 'Liverpool', datetime(2026, 11, 1).date(), [])
    assert released_short_trips(db, 'run', {}, ['London Luton'], ['Liverpool'], **kwargs)['total'] == 0


def test_equal_instants_in_different_zones_keep_local_daytime(tmp_path):
    db = ScanCacheDB(str(tmp_path / 'live.db'))
    db.upsert_pdf_run('run', '2026-09-11', '2026-09-11', '2026-09-14', 4)
    for dest, arrival, back in [
        ('Budapest', '2026-09-11T11:00', '2026-09-12T23:00'),
        ('Kutaisi', '2026-09-11T13:00', '2026-09-13T01:00'),
    ]:
        for a, b, departure, landing in [
            ('London Luton', dest, '2026-09-11T06:00', arrival),
            (dest, 'Liverpool', back, '2026-09-13T02:00'),
        ]:
            dep, arr = datetime.fromisoformat(departure), datetime.fromisoformat(landing)
            db.replace_route_check('run', a, b, dep.date(), [Flight(a, b, 'W1', dep, arr, '', '')])
    result = released_short_trips(db, 'run', {}, ['London Luton'], ['Liverpool'],
                                  now=datetime(2026, 9, 11, tzinfo=timezone.utc))
    assert {r['destination']: r['daytime_hours'] for r in result['trips']} == {
        'Budapest': 20.5, 'Kutaisi': 18.5}
