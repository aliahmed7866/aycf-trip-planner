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
        db.execute('DELETE FROM snapshots WHERE snapshot_id=2')
    assert not historical_trip_patterns({}, ['London Luton'], ['Liverpool'], now=NOW, path=str(path))['patterns']
