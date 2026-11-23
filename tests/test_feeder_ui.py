from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, session
import pytest

import feeder_blueprint as web
from feeder_store import FeederStore
from navigation import navigation_context

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def page_app(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    day = (now + timedelta(days=1)).date().isoformat()
    from cache_db import ScanCacheDB
    from scanner import Flight
    db = ScanCacheDB(str(tmp_path / 'flights.sqlite3'))
    return_day = (now + timedelta(days=2)).date().isoformat()
    db.upsert_pdf_run('current', now.isoformat(), day, return_day, 3)
    for origin, destination, travel, number, dep, arr in [
        ('Liverpool', 'Budapest', day, 'W100', '06:00', '10:00'),
        ('Budapest', 'Kutaisi', day, 'W6123', '16:00', '22:00'),
        ('Kutaisi', 'Liverpool', return_day, 'W300', '22:00', '23:30'),
    ]:
        departure, arrival = (datetime.fromisoformat(travel + 'T' + time) for time in (dep, arr))
        db.replace_route_check('current', origin, destination, departure.date(),
                               [Flight(origin, destination, number, departure, arrival, '', '')])
    db.mark_pdf_scanned('current')
    monkeypatch.delenv('AYCF_SERPAPI_KEY', raising=False)
    store = FeederStore(tmp_path / 'feeders.sqlite3')
    app = Flask(__name__, template_folder=str(ROOT / 'templates'), static_folder=str(ROOT / 'static'))
    app.secret_key = 'test'
    app.config['TESTING'] = True
    app.context_processor(navigation_context)
    app.add_url_rule('/', 'index', lambda: 'planner')
    app.add_url_rule('/short-trips', 'short_trips.page', lambda: 'returns')
    def token():
        session['csrf_token'] = 'valid'
        return 'valid'
    app.jinja_env.globals['csrf_token'] = token
    from flask import request
    app.register_blueprint(web.create_feeder_blueprint(lambda: {'run_id': 'current', 'scope': {'connection_hubs': ['Budapest']}},
                           db, lambda: request.form.get('csrf_token') == session.get('csrf_token') == 'valid', store))
    app.config.update(FEEDER_TEST_STORE=store, FEEDER_TEST_DAY=day)
    return app


def test_get_requires_no_provider_and_discloses_scope(page_app, monkeypatch):
    store = page_app.config['FEEDER_TEST_STORE']
    monkeypatch.setattr(store, 'refresh', lambda *a: pytest.fail('GET must not search'))
    monkeypatch.setattr(web, 'check_serpapi_account', lambda *a: pytest.fail('GET must not check account'))
    result = page_app.test_client().get('/feeder-trips')
    assert result.status_code == 200
    text = result.get_data(as_text=True)
    assert 'Find a cash flight to meet your pass flight' in text and 'W6123' in text
    assert 'manual fare checks work without a key' in text
    assert 'Check fares · up to 1 request' not in text
    assert 'Budapest BUD' in text and 'Kutaisi' in text
    assert 'aria-current="page">Manchester connections' in text


def test_account_check_requires_csrf_and_spends_no_fare_allowance(page_app, monkeypatch):
    store = page_app.config['FEEDER_TEST_STORE']
    calls = []
    monkeypatch.setenv('AYCF_SERPAPI_KEY', 'private-test-key')
    monkeypatch.setattr(store, 'refresh', lambda *a: pytest.fail('Account check must not search fares'))
    monkeypatch.setattr(web, 'check_serpapi_account', lambda key: calls.append(key) or {
        'state': 'ready', 'message': 'SerpApi key accepted. 218 account searches remaining.'})
    client = page_app.test_client()
    assert 'Test SerpApi connection' in client.get('/feeder-trips').get_data(as_text=True)
    assert calls == []
    assert client.post('/feeder-trips', data={'action': 'test_connection'}).status_code == 400
    assert calls == []
    response = client.post('/feeder-trips', data={'action': 'test_connection', 'csrf_token': 'valid'}, follow_redirects=True)
    assert response.status_code == 200 and calls == ['private-test-key']
    text = response.get_data(as_text=True)
    assert '218 account searches remaining' in text and 'private-test-key' not in text
    assert store.usage()['usage_24h'] == 0 and store.list_offers() == []


@pytest.mark.parametrize('failure,expected', [
    (web.ProviderError('invalid_key'), 'key'),
    (RuntimeError('private-key-in-url'), 'The account check could not complete.'),
])
def test_account_check_errors_are_safe_in_page(page_app, monkeypatch, failure, expected):
    def fail(key):
        raise failure
    monkeypatch.setattr(web, 'check_serpapi_account', fail)
    client = page_app.test_client()
    client.get('/feeder-trips')
    response = client.post('/feeder-trips', data={'action': 'test_connection', 'csrf_token': 'valid'}, follow_redirects=True)
    assert response.status_code == 200
    assert expected in response.get_data(as_text=True)
    assert 'private-key-in-url' not in response.get_data(as_text=True)


def test_refresh_requires_csrf_key_and_current_candidate(page_app, monkeypatch):
    client = page_app.test_client()
    client.get('/feeder-trips')
    store = page_app.config['FEEDER_TEST_STORE']
    calls = []
    monkeypatch.setattr(store, 'refresh', lambda *a: calls.append(a) or {'message': 'checked'})
    form = dict(action='refresh', hub='BUD', date=page_app.config['FEEDER_TEST_DAY'])
    assert client.post('/feeder-trips', data=form).status_code == 400
    form['csrf_token'] = 'valid'
    assert client.post('/feeder-trips', data=form).status_code == 400
    monkeypatch.setenv('AYCF_SERPAPI_KEY', 'private-test-key')
    assert client.post('/feeder-trips', data=dict(form, hub='WMI')).status_code == 400
    assert calls == []
    response = client.post('/feeder-trips', data=form)
    assert response.status_code == 302 and len(calls) == 1
    assert 'private-test-key' not in client.get('/feeder-trips').get_data(as_text=True)


def test_automatic_controls_persist_and_require_csrf_without_provider_calls(page_app, monkeypatch):
    from feeder_automation import automation_status
    store = page_app.config['FEEDER_TEST_STORE']
    monkeypatch.setattr(store, 'refresh', lambda *a: pytest.fail('Settings must not search'))
    client = page_app.test_client()
    client.get('/feeder-trips')
    assert automation_status(store)['enabled']
    assert client.post('/feeder-trips', data={'action': 'auto_pause'}).status_code == 400
    assert automation_status(store)['enabled']
    paused = client.post('/feeder-trips', data={'action': 'auto_pause', 'csrf_token': 'valid'}, follow_redirects=True)
    assert paused.status_code == 200
    assert 'Resume automatic checks' in paused.get_data(as_text=True)
    assert not automation_status(FeederStore(store.path))['enabled']
    resumed = client.post('/feeder-trips', data={'action': 'auto_resume', 'csrf_token': 'valid'}, follow_redirects=True)
    assert resumed.status_code == 200
    assert 'Pause automatic checks' in resumed.get_data(as_text=True)
    assert automation_status(store)['enabled']


def manual_form(day):
    return dict(action='add', csrf_token='valid', hub='BUD', airline='Ryanair', flight_number='FR1234',
                departure=day+'T07:00', arrival=day+'T11:00', price_gbp='29.99', checked_now='yes')


def test_manual_fare_forms_priced_trip_and_delete_is_protected(page_app):
    client = page_app.test_client()
    client.get('/feeder-trips')
    response = client.post('/feeder-trips', data=manual_form(page_app.config['FEEDER_TEST_DAY']), follow_redirects=True)
    assert response.status_code == 200
    text = response.get_data(as_text=True)
    assert '1 priced connection' in text and '£29.99' in text and 'Manually checked' in text
    assert 'airport local times' in text
    store = page_app.config['FEEDER_TEST_STORE']
    offer = store.list_offers()[0]
    assert client.post('/feeder-trips', data=dict(action='delete', offer_id=offer['id'])).status_code == 400
    assert len(store.list_offers()) == 1
    assert client.post('/feeder-trips', data=dict(action='delete', offer_id=offer['id'], csrf_token='valid')).status_code == 302
    assert store.list_offers() == []


@pytest.mark.parametrize('change', [{'checked_now': ''}, {'price_gbp': 'NaN'}, {'arrival': 'broken'},
                                    {'departure': '2020-01-01T07:00'}, {'hub': 'WMI'}])
def test_bad_manual_quotes_do_not_write(page_app, change):
    client = page_app.test_client()
    client.get('/feeder-trips')
    assert client.post('/feeder-trips', data=manual_form(page_app.config['FEEDER_TEST_DAY']) | change).status_code == 400
    assert page_app.config['FEEDER_TEST_STORE'].list_offers() == []


@pytest.mark.parametrize('width', [390, 1280])
def test_feeder_browser(page_app, width, tmp_path, monkeypatch):
    playwright = pytest.importorskip('playwright.sync_api')
    import os
    import threading
    from werkzeug.serving import make_server
    monkeypatch.setenv('AYCF_SERPAPI_KEY', 'browser-test-key')
    monkeypatch.setattr(web, 'check_serpapi_account', lambda key: {
        'state': 'ready', 'message': 'SerpApi key accepted. 218 account searches remaining.'})
    server = make_server('127.0.0.1', 0, page_app)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        with playwright.sync_playwright() as p:
            browser = p.chromium.launch()
            try:
                page = browser.new_page(viewport={'width': width, 'height': 1000})
                page.goto(f'http://127.0.0.1:{server.server_port}/feeder-trips')
                page.get_by_role('button', name='Test SerpApi connection', exact=True).click()
                playwright.expect(page.get_by_text('SerpApi key accepted. 218 account searches remaining.', exact=True)).to_be_visible()
                page.get_by_role('button', name='Pause automatic checks', exact=True).click()
                playwright.expect(page.get_by_role('button', name='Resume automatic checks', exact=True)).to_be_visible()
                page.get_by_role('button', name='Resume automatic checks', exact=True).click()
                monkeypatch.delenv('AYCF_SERPAPI_KEY')
                page.reload()
                playwright.expect(page.get_by_role('heading', name='Start with a pass flight')).to_be_visible()
                day = page_app.config['FEEDER_TEST_DAY']
                target = page.locator('article').filter(has_text='MAN → Budapest BUD').filter(
                    has_text='Leave Manchester on ' + day)
                target.get_by_role('link', name='View flights and save a fare').click()
                playwright.expect(page.locator('#selected-hub')).to_contain_text('Direct pass return to LPL')
                playwright.expect(page.locator('#selected-hub')).to_contain_text('W6123')
                page.get_by_label('Flight number', exact=True).fill('FR1234')
                page.get_by_label('Departure · Manchester local time', exact=True).fill('07:00')
                page.get_by_label('Arrival · hub local time', exact=True).fill(page_app.config['FEEDER_TEST_DAY']+'T11:00')
                page.get_by_label('Fare in GBP · less than £40', exact=True).fill('29.99')
                page.get_by_text('I have just checked this fare and these flight times.', exact=True).click()
                page.get_by_role('button', name='Save fare and match', exact=True).click()
                playwright.expect(page.get_by_role('heading', name='1 priced connection', exact=True)).to_be_visible()
                assert page_app.config['FEEDER_TEST_STORE'].usage()['usage_24h'] == 0
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                if os.environ.get('BROWSER_ARTIFACT_DIR'):
                    path = Path(os.environ['BROWSER_ARTIFACT_DIR'])
                    path.mkdir(exist_ok=True, parents=True)
                    page.screenshot(path=str(path / f'feeder-connections-{width}.png'), full_page=True)
            finally:
                browser.close()
    finally:
        server.shutdown()
        worker.join(timeout=5)


def test_target_workflow_uses_route_and_date_without_key(page_app):
    client = page_app.test_client()
    day = page_app.config['FEEDER_TEST_DAY']
    url = '/feeder-trips?hub=BUD&day=' + day
    page = client.get(url).get_data(as_text=True)
    assert 'Check MAN → BUD fares' in page and 'name="departure_time"' in page
    assert 'W6123' in page and 'Direct pass return to LPL' in page and 'W300' in page
    man_only = client.get(url + '&airports=man').get_data(as_text=True)
    assert 'No direct pass return' in man_only and 'Start with a pass flight' not in man_only
    form = manual_form(day)
    del form['departure']
    result = client.post(url, data=dict(form, departure_time='07:00'), follow_redirects=True)
    assert '1 priced connection' in result.get_data(as_text=True)
    assert page_app.config['FEEDER_TEST_STORE'].usage()['usage_24h'] == 0


def test_over_budget_quote_and_too_late_arrival_have_distinct_outcomes(page_app):
    client = page_app.test_client()
    day = page_app.config['FEEDER_TEST_DAY']
    client.get('/feeder-trips')
    form = manual_form(day)
    assert client.post('/feeder-trips', data=dict(form, price_gbp='40')).status_code == 400
    assert not page_app.config['FEEDER_TEST_STORE'].list_offers()
    result = client.post('/feeder-trips', data=dict(form, arrival=day+'T17:59'), follow_redirects=True)
    assert 'Its arrival misses' in result.get_data(as_text=True)
    assert '0 priced connections' in result.get_data(as_text=True)
