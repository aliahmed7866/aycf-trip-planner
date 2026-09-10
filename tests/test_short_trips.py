from datetime import datetime, timezone
from pathlib import Path

import pytest
from flask import Flask

from cache_db import ScanCacheDB
from scanner import Flight
from short_trips import released_short_trips, local_datetime, UK_ZONE
from short_trips_blueprint import create_short_trips_blueprint

NOW = datetime(2026, 9, 11, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(tmp_path):
    cache = ScanCacheDB(str(tmp_path / 'flights.sqlite3'))
    cache.upsert_pdf_run('current', '2026-09-11T00:00:00', '2026-09-11', '2026-09-14', 20)
    cache.mark_pdf_scanned('current')
    return cache


def add(db, origin, destination, departure, arrival, code='W1', run='current', logical=None, texts=None):
    dep, arr = datetime.fromisoformat(departure), datetime.fromisoformat(arrival)
    db.replace_route_check(run, *(logical or (origin, destination)), dep.date(), [
        Flight(origin, destination, code, dep, arr, *(texts or ('', '')))])


def outward(db, destination='Budapest', arrival='2026-09-11T11:00:00'):
    add(db, 'London Luton', destination, '2026-09-11T08:00:00', arrival, logical=('London', destination))


def home(db, departure='2026-09-12T15:00:00', arrival='2026-09-12T17:00:00', destination='Budapest', airport='Liverpool'):
    add(db, destination, airport, departure, arrival, 'W2')


def search(db, **kwargs):
    kwargs.setdefault('scope', {})
    return released_short_trips(db, 'current', origins=['London Luton'], returns=['Liverpool'], now=NOW, **kwargs)


def test_complete_trip_pairs_different_uk_airports(db):
    outward(db)
    home(db)
    result = search(db)
    trip = result['trips'][0]
    assert trip['stay_hours'] == 28
    assert trip['daytime_hours'] == 12.5
    assert trip['different_airports']
    assert trip['outbound'][0]['origin'] == 'London Luton'
    assert trip['return_airport'] == 'Liverpool'
    assert result['total'] == 1


@pytest.mark.parametrize('departure,arrival', [
    ('2026-09-12T07:00:00', '2026-09-12T09:00:00'),
    ('2026-09-12T11:00:00', '2026-09-12T13:00:00'),
])
def test_overnight_and_exactly_24h_rejected(db, departure, arrival):
    outward(db)
    home(db, departure, arrival)
    assert search(db)['trips'] == []


def test_more_than_24_not_rounded_down(db):
    outward(db)
    home(db, '2026-09-12T11:01:00', '2026-09-12T13:01:00')
    assert search(db)['total'] == 1
    assert search(db, min_stay_hours=30)['total'] == 0


def test_daytime_filter_is_distinct_from_stay(db):
    outward(db, arrival='2026-09-11T23:00:00')
    home(db, '2026-09-13T00:30:00', '2026-09-13T02:00:00')
    assert search(db)['trips'][0]['daytime_hours'] == 12
    assert search(db, min_daytime_hours=16)['total'] == 0


def test_monday_morning_deadline_and_any_weekday(db):
    outward(db)
    home(db, '2026-09-14T06:00:00', '2026-09-14T08:00:00')
    assert search(db, return_by=local_datetime('2026-09-14T08:00', UK_ZONE))['total'] == 1
    assert search(db, return_by=local_datetime('2026-09-14T07:59', UK_ZONE))['total'] == 0
    assert search(db, leave_after=local_datetime('2026-09-11T08:01', UK_ZONE))['total'] == 0


def test_only_current_completed_released_flights(db):
    outward(db)
    db.upsert_pdf_run('old', '2026-09-10', '2026-09-10', '2026-09-14', 2)
    db.mark_pdf_scanned('old')
    add(db, 'Budapest', 'Liverpool', '2026-09-12T15:00', '2026-09-12T17:00', run='old')
    assert search(db)['total'] == 0
    home(db, '2026-09-15T15:00', '2026-09-15T17:00')
    assert search(db)['total'] == 0
    home(db)
    with db.connect() as conn:
        conn.execute("UPDATE pdf_runs SET scanned_at=NULL WHERE run_id='current'")
    assert search(db)['total'] == 0


def test_past_flights_and_unchecked_rows_excluded(db):
    outward(db)
    home(db)
    assert search(db, leave_after=local_datetime('2026-09-11T09:00', UK_ZONE))['total'] == 0
    with db.connect() as conn:
        conn.execute("DELETE FROM route_checks WHERE origin='Budapest'")
    assert search(db)['total'] == 0


@pytest.mark.parametrize('scope', [
    {'excluded_countries': ['Hungary']}, {'excluded_airports': ['BUD']},
    {'excluded_routes': [['Budapest', 'Liverpool']]},
    {'destination_mode': 'exclude', 'destinations': ['Budapest']},
])
def test_exclusions_apply_to_every_leg(db, scope):
    outward(db)
    home(db)
    assert search(db, scope=scope)['total'] == 0


def test_exact_airport_selection_and_ambiguous_london(db):
    add(db, 'London Gatwick', 'Budapest', '2026-09-11T08:00', '2026-09-11T11:00', logical=('London', 'Budapest'))
    home(db)
    assert search(db)['total'] == 0
    add(db, 'London', 'Budapest', '2026-09-11T08:00', '2026-09-11T11:00')
    assert search(db)['total'] == 0
    assert search(db)['omitted_times'] == 1


def test_connections_with_same_airport_and_approved_hubs(db):
    outward(db)
    add(db, 'Budapest', 'Rome', '2026-09-11T14:00', '2026-09-11T16:00', 'W3')
    home(db, destination='Rome', departure='2026-09-13T17:00', arrival='2026-09-13T19:00')
    assert search(db, destinations=['Rome'])['total'] == 0
    assert search(db, destinations=['Rome'], max_stops=1)['total'] == 0
    result = search(db, destinations=['Rome'], max_stops=1, scope={'connection_hubs': ['Budapest']})
    assert result['trips'][0]['stops'] == 1
    assert result['trips'][0]['stay_hours'] == 49
    assert search(db, destinations=['Rome'], max_stops=1, scope={'connection_hubs': ['Budapest']}, max_layover_minutes=150)['total'] == 0
    assert search(db, destinations=['Rome'], max_stops=1, scope={'connection_hubs': ['Budapest']}, max_journey_minutes=300)['total'] == 0


def test_return_connection_and_cross_airport_rejected(db):
    outward(db, 'Rome', '2026-09-11T12:00')
    add(db, 'Rome', 'Bucharest Otopeni', '2026-09-13T12:30', '2026-09-13T15:30', 'W3')
    home(db, destination='Bucharest Otopeni', departure='2026-09-13T18:00', arrival='2026-09-13T20:00')
    assert search(db, max_stops=1, scope={'connection_hubs': ['Bucharest']})['total'] == 1
    with db.connect() as conn:
        conn.execute("UPDATE route_flights SET physical_origin='Bucharest Baneasa' WHERE origin='Bucharest Otopeni'")
    assert search(db, max_stops=1, scope={'connection_hubs': ['Bucharest']})['total'] == 0


def test_preferred_sort_and_no_limit_before_pairing(db):
    outward(db)
    home(db)
    outward(db, 'Rome', '2026-09-11T12:00')
    home(db, destination='Rome')
    result = search(db, scope={'preferred_destinations': ['Rome']}, limit=1)
    assert result['total'] == 2
    assert result['limited']
    assert result['trips'][0]['destination'] == 'Rome'
    assert search(db, work_limit=1)['incomplete']


def test_offset_timestamps_recovered_and_unknown_zone_omitted(db):
    add(db, 'London Luton', 'Budapest', '2026-09-11T07:00', '2026-09-11T09:00',
        texts=('2026-09-11T08:00:00+01:00', '2026-09-11T11:00:00+02:00'))
    home(db)
    assert search(db)['trips'][0]['stay_hours'] == 28
    assert '08:00' in search(db)['trips'][0]['outbound'][0]['departure']
    with db.connect() as conn:
        conn.execute("UPDATE route_flights SET physical_destination='ZZZ' WHERE origin='London Luton'")
    assert search(db)['total'] == 0
    assert search(db)['omitted_times'] == 1


def test_dst_clock_change_is_real_elapsed_time(db):
    with db.connect() as conn:
        conn.execute("UPDATE pdf_runs SET departure_start='2026-10-23', departure_end='2026-10-26'")
    add(db, 'London Luton', 'Budapest', '2026-10-24T08:00', '2026-10-24T11:00')
    add(db, 'Budapest', 'Liverpool', '2026-10-25T11:00', '2026-10-25T13:00')
    assert search(db)['trips'][0]['stay_hours'] == 25
    with pytest.raises(ValueError):
        local_datetime('2026-10-25T01:30', UK_ZONE)


@pytest.fixture
def web(db):
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'templates'))
    app.secret_key = 'test'
    app.add_url_rule('/', 'index', lambda: 'planner')
    app.add_url_rule('/flights', 'all_flights', lambda: 'flights')
    app.jinja_env.globals['csrf_token'] = lambda: 'csrf'
    ctx = {'origins': ['London Luton', 'Liverpool'], 'pairs': [('London', 'Budapest'), ('Budapest', 'Liverpool')],
           'ready': True, 'run_id': 'current', 'scope': {'origins': ['London Luton', 'Liverpool']}}
    app.register_blueprint(create_short_trips_blueprint(lambda: ctx, db, lambda: request_token()))
    def request_token():
        from flask import request
        return request.form.get('csrf_token') == 'csrf'
    return app, ctx


def test_form_get_validation_and_rendered_results(web, db, monkeypatch):
    import short_trips_blueprint as module
    real = module.released_short_trips
    monkeypatch.setattr(module, 'released_short_trips', lambda *a, **kw: real(*a, now=NOW, **kw))
    app, ctx = web
    outward(db)
    home(db)
    with app.test_client() as client:
        response = client.get('/short-trips')
        assert response.status_code == 200
        assert b'Land in the UK by' in response.data
        assert client.post('/short-trips').status_code == 400
        response = client.post('/short-trips', data={'csrf_token': 'csrf', 'origins': 'London Luton', 'returns': 'Liverpool'})
        assert response.status_code == 200
        assert b'1 complete trip found' in response.data
        assert b'28h 0m at destination' in response.data
        assert b'Different UK return airport' in response.data
        for extra in ({'origins': 'Budapest'}, {'destination': 'Fake'}, {'max_stops': 'nan'}, {'min_stay_hours': '12'}, {'return_by': 'bad'}):
            assert client.post('/short-trips', data={'csrf_token': 'csrf', 'origins': 'London Luton', 'returns': 'Liverpool', **extra}).status_code == 400
        ctx['ready'] = False
        assert client.post('/short-trips', data={'csrf_token': 'csrf', 'origins': 'London Luton', 'returns': 'Liverpool'}).status_code == 400


def test_two_stops_each_way_remain_complete_trips(db):
    outward(db)
    add(db, 'Budapest', 'Rome', '2026-09-11T14:00', '2026-09-11T16:00', 'W3')
    add(db, 'Rome', 'Athens', '2026-09-11T19:00', '2026-09-11T22:00', 'W4')
    add(db, 'Athens', 'Rome', '2026-09-13T09:00', '2026-09-13T10:00', 'W5')
    add(db, 'Rome', 'Budapest', '2026-09-13T13:00', '2026-09-13T15:00', 'W6')
    home(db, '2026-09-13T18:00', '2026-09-13T20:00')
    result = search(db, destinations=['Athens'], max_stops=2, scope={'connection_hubs': ['Budapest', 'Rome']})
    assert result['total'] == 1
    assert result['trips'][0]['stops'] == 4
    assert result['trips'][0]['stay_hours'] == 35
    assert search(db, destinations=['Athens'], max_stops=1, scope={'connection_hubs': ['Budapest', 'Rome']})['total'] == 0


@pytest.mark.parametrize('width', [390, 1280])
def test_short_trip_browser(web, db, monkeypatch, tmp_path, width):
    playwright = pytest.importorskip('playwright.sync_api')
    import threading
    import os
    from werkzeug.serving import make_server
    import short_trips_blueprint as module
    real = module.released_short_trips
    monkeypatch.setattr(module, 'released_short_trips', lambda *a, **kw: real(*a, now=NOW, **kw))
    outward(db)
    home(db)
    app, ctx = web
    server = make_server('127.0.0.1', 0, app)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch()
            page = browser.new_page(viewport={'width': width, 'height': 844})
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.goto(f'http://127.0.0.1:{server.server_port}/short-trips')
            page.locator('[name="origins"][value="Liverpool"]').uncheck()
            page.locator('[name="returns"][value="London Luton"]').uncheck()
            page.get_by_role('button', name='Find complete trips').click()
            playwright.expect(page.get_by_role('heading', name='1 complete trip found')).to_be_visible()
            playwright.expect(page.get_by_text('28h 0m at destination')).to_be_visible()
            assert page.locator('[name="origins"][value="London Luton"]').is_checked()
            assert not page.locator('[name="returns"][value="London Luton"]').is_checked()
            assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
            artifacts = Path(os.environ.get('BROWSER_ARTIFACT_DIR', str(tmp_path)))
            artifacts.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(artifacts / f'short-trips-{width}.png'), full_page=True)
            page.locator('#min-stay').select_option('36')
            page.get_by_role('button', name='Find complete trips').click()
            playwright.expect(page.get_by_role('heading', name='No complete trip fits these choices yet.')).to_be_visible()
            assert not errors
            browser.close()
    finally:
        server.shutdown()
        worker.join(timeout=5)
