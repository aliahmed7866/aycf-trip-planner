from datetime import date, datetime, time, timedelta

from cache_db import ScanCacheDB
from scanner import Flight
from scan_inventory import preserve_inventory
from search_cache import scan_readiness


def test_rollover_preserves_until_successful_refresh(tmp_path):
    db = ScanCacheDB(str(tmp_path / 'cache.sqlite'))
    day = date.today() + timedelta(days=1)
    scope = {'origins': ['London Luton'], 'destination_mode': 'all'}
    routes = [('London Luton', 'Budapest')]
    for run in ('old', 'new'):
        db.upsert_pdf_run(run, run, day.isoformat(), day.isoformat(), 1)
    dep = datetime.combine(day, time(12))
    flight = Flight('London Luton', 'Budapest', 'W123', dep, dep + timedelta(hours=2), '12:00', '14:00', '2h')
    db.replace_route_check('old', *routes[0], day, [flight])
    with db.connect() as conn:
        conn.execute("UPDATE route_flights SET fetched_at='2026-01-01T12:00:00'")
    preserve_inventory(db, 'new', routes, scope)
    assert db.get_flights(*routes[0], day, 'new') == [flight]
    assert not db.route_checked('new', *routes[0], day)
    assert scan_readiness(db, 'new', db.get_pdf_run('new')) == {'ready': False, 'usable': True, 'partial': True}
    with db.connect() as conn:
        assert conn.execute("SELECT fetched_at FROM route_flights WHERE pdf_run_id='new'").fetchone()[0] == '2026-01-01T12:00:00'
    # A failed/unknown request and repeated page loads must preserve inventory.
    db.replace_route_check('new', *routes[0], day, [], complete=False)
    preserve_inventory(db, 'new', routes, scope)
    assert db.get_flights(*routes[0], day, 'new') == [flight]
    # A successful empty response must remove it and prevent resurrection.
    db.replace_route_check('new', *routes[0], day, [])
    preserve_inventory(db, 'new', routes, scope)
    assert db.get_flights(*routes[0], day, 'new') == []


def test_rollover_respects_exclusions_past_dates_and_newer_empty(tmp_path):
    db = ScanCacheDB(str(tmp_path / 'cache.sqlite'))
    day = date.today() + timedelta(days=1)
    route = ('London Luton', 'Budapest')
    dep = datetime.combine(day, time(12))
    flight = Flight(*route, 'W123', dep, dep + timedelta(hours=2), '', '', '')
    db.replace_route_check('old', *route, day, [flight])
    db.replace_route_check('old', *route, date.today() - timedelta(days=1), [flight])
    scope = {'origins': ['London Luton'], 'destination_mode': 'all', 'excluded_airports': ['Budapest']}
    preserve_inventory(db, 'excluded', [route], scope)
    assert db.get_flights(*route, day, 'excluded') is None
    scope.pop('excluded_airports')
    preserve_inventory(db, 'next', [route], scope)
    assert db.get_flights(*route, date.today() - timedelta(days=1), 'next') is None
    db.replace_route_check('newer', *route, day, [])
    preserve_inventory(db, 'latest', [route], scope)
    assert db.get_flights(*route, day, 'latest') is None


def test_web_shows_previous_flights_before_new_scan_starts(tmp_path, monkeypatch):
    import pandas as pd
    import app as web
    from types import SimpleNamespace
    db = ScanCacheDB(str(tmp_path / 'web.sqlite'))
    day = date.today() + timedelta(days=1)
    dep = datetime.combine(day, time(12))
    db.upsert_pdf_run('previous', '2026-01-01', day.isoformat(), day.isoformat(), 1)
    db.replace_route_check('previous', 'London Luton', 'Budapest', day,
                           [Flight('London Luton', 'Budapest', 'W123', dep, dep + timedelta(hours=2), '', '', '')])
    frame = pd.DataFrame([{'departure_from': 'London Luton', 'departure_to': 'Budapest',
                           'data_generated': date.today().isoformat(),
                           'availability_start': date.today().isoformat(),
                           'availability_end': (day + timedelta(days=1)).isoformat()}])
    graph = SimpleNamespace(latest_frame=lambda: frame, cities=lambda: ['London Luton', 'Budapest'])
    monkeypatch.setattr(web, '_cache_dir', lambda: str(tmp_path))
    monkeypatch.setattr(web, 'update_data_if_needed', lambda **kw: SimpleNamespace(data_dir=str(tmp_path)))
    monkeypatch.setattr(web, 'CurrentRouteGraph', lambda path: graph)
    monkeypatch.setattr(web, 'ScanCacheDB', lambda: db)
    monkeypatch.setattr(web, 'load_scope', lambda: {'origins': ['London Luton'], 'destination_mode': 'all'})
    monkeypatch.setattr(web, 'scan_scope_with_preferences', lambda scope: scope)
    monkeypatch.setattr(web, 'scan_run_id', lambda *args: 'new')
    monkeypatch.setattr(web, '_vault_or_none', lambda: None)
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path))
    app = web.create_app()
    app.testing = True
    response = app.test_client().get('/flights')
    assert response.status_code == 200
    assert b'W123' in response.data
    assert b'Awaiting refresh' in response.data and b'Last checked:' in response.data
    assert not db.route_checked('new', 'London Luton', 'Budapest', day)
    with db.connect() as conn:
        assert conn.execute('SELECT COUNT(*) FROM scan_runs').fetchone()[0] == 0
