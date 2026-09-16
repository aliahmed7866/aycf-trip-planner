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
    first = day + 'T16:00:00+00:00'
    report = {'opportunities': [{
        'id': 'one', 'hub': 'BUD', 'hub_name': 'Budapest', 'feeder_dates': [day],
        'destination': 'KUT', 'destination_name': 'Kutaisi', 'country': 'Georgia',
        'first_departure': first, 'final_arrival': day + 'T19:00:00+00:00',
        'oldest_checked': now.isoformat(), 'wizz_legs': 1,
        'legs': [{'origin': 'BUD', 'destination': 'KUT', 'origin_name': 'Budapest',
                  'destination_name': 'Kutaisi', 'flight_number': 'W6123',
                  'departure': first, 'arrival': day + 'T19:00:00+00:00',
                  'departure_local': day + 'T18:00:00+02:00',
                  'arrival_local': day + 'T23:00:00+04:00', 'fetched_at': now.isoformat()}],
    }], 'incomplete': False, 'limited': False, 'scan_partial': False,
        'omitted_times': 0, 'stale_flights': 0}
    monkeypatch.setattr(web, 'feeder_opportunities', lambda *a, **kw: report)
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
    app.register_blueprint(web.create_feeder_blueprint(lambda: {'run_id': 'current', 'scope': {}},
                           object(), lambda: request.form.get('csrf_token') == session.get('csrf_token') == 'valid', store))
    app.config.update(FEEDER_TEST_STORE=store, FEEDER_TEST_DAY=day)
    return app


def test_get_requires_no_provider_and_discloses_scope(page_app, monkeypatch):
    store = page_app.config['FEEDER_TEST_STORE']
    monkeypatch.setattr(store, 'refresh', lambda *a: pytest.fail('GET must not search'))
    monkeypatch.setattr(web, 'check_serpapi_account', lambda *a: pytest.fail('GET must not check account'))
    result = page_app.test_client().get('/feeder-trips')
    assert result.status_code == 200
    text = result.get_data(as_text=True)
    assert 'Outbound opportunities' in text and 'Return flights are not checked' in text
    assert 'Free API search is not connected' in text
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
                page.get_by_text('Save a fare you have checked', exact=True).click()
                page.get_by_label('Flight number', exact=True).fill('FR1234')
                page.get_by_label('Departure · Manchester local time', exact=True).fill(page_app.config['FEEDER_TEST_DAY']+'T07:00')
                page.get_by_label('Arrival · hub local time', exact=True).fill(page_app.config['FEEDER_TEST_DAY']+'T11:00')
                page.get_by_label('Fare in GBP · less than £40', exact=True).fill('29.99')
                page.get_by_text('I have just checked this fare and these flight times.', exact=True).click()
                page.get_by_role('button', name='Save fare and match', exact=True).click()
                playwright.expect(page.get_by_role('heading', name='1 priced connection', exact=True)).to_be_visible()
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
