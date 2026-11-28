from datetime import date
from pathlib import Path

from flask import Flask, render_template

from navigation import navigation_context
from search_cache import SearchCache
from itinerary_search import cached_scan_itineraries, endpoint_matches


class Cache:
    def __init__(self):
        self.calls = []

    def get_pdf_run(self, run_id):
        return {'departure_start': '2026-09-11', 'departure_end': '2026-09-13'}

    def get_flights(self, a, b, day, run_id):
        self.calls.append((a, b, day))
        return [] if b == 'Rome' else None

    def checked_routes(self, run_id):
        return set()


class Graph:
    def edges_for_day(self, day):
        return {('Liverpool', 'Budapest'), ('Budapest', 'Rome'), ('Budapest', 'Athens')}


def test_missing_lookups_unique_and_outside_release_window_not_counted():
    db = Cache()
    cache = SearchCache(db, 'run')
    for _ in range(2):
        rows, misses = cached_scan_itineraries(Graph(), cache, 'Liverpool', None, date(2026, 9, 12),
                                               pdf_run_id='run', days=4, max_stops=2)
        assert rows == []
        assert misses == 2
    assert len(db.calls) == 2
    assert {call[2] for call in db.calls} == {date(2026, 9, 12), date(2026, 9, 13)}


def test_checked_zero_flights_not_reported_missing():
    cache = SearchCache(Cache(), 'run')
    assert cache.get_flights('Liverpool', 'Rome', date(2026, 9, 12), 'run') == []
    assert cache.missing == set()


def test_excluded_routes_not_searched_or_counted_missing():
    db = Cache()
    rows, misses = cached_scan_itineraries(Graph(), db, 'Liverpool', None, date(2026, 9, 12),
                                          pdf_run_id='run', scope={'excluded_airports': ['Budapest']})
    assert rows == [] and misses == 0 and db.calls == []


def test_physical_airport_aliases_match_without_conflating_london_airports():
    assert endpoint_matches('LTN', ['London Luton'])
    assert endpoint_matches('Bucharest Otopeni', ['Bucharest'])
    assert not endpoint_matches('London', ['London Luton'])
    assert not endpoint_matches('LGW', ['London Luton'])


def test_navigation_registered_routes_active_results_and_more_links():
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'templates'))
    app.secret_key = 'test'
    app.context_processor(navigation_context)
    for path, endpoint in [('/', 'index'), ('/scan', 'multi_search.scan'), ('/short-trips', 'short_trips.page'),
                           ('/flights', 'all_flights'), ('/settings', 'scan_settings.page'), ('/system', 'system_health.page')]:
        app.add_url_rule(path, endpoint, lambda: render_template('base.html'))
    with app.test_client() as client:
        html = client.get('/scan').get_data(as_text=True)
        assert 'aria-current="page">Search' in html
        assert 'href="/short-trips"' in html
        assert 'nav-more-menu' in html and 'Scan settings' in html and 'System status' in html
        assert 'Watches' not in html
        html = client.get('/short-trips').get_data(as_text=True)
        assert 'aria-current="page">Short trips' in html
        assert 'Skip to content' in html


def test_navigation_browser(tmp_path):
    import pytest
    playwright = pytest.importorskip('playwright.sync_api')
    from werkzeug.serving import make_server
    import threading
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'templates'))
    app.secret_key = 'test'
    app.context_processor(navigation_context)
    for path, endpoint in [('/', 'index'), ('/short-trips', 'short_trips.page'), ('/flights', 'all_flights'),
                           ('/watches', 'watches.watchlist'), ('/settings', 'scan_settings.page'),
                           ('/system', 'system_health.page'), ('/places', 'places.page'), ('/stability', 'stability.page')]:
        app.add_url_rule(path, endpoint, lambda: render_template('base.html'))
    server = make_server('127.0.0.1', 0, app)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch()
            page = browser.new_page()
            for width in (360, 390, 820, 1280):
                page.set_viewport_size({'width': width, 'height': 844})
                page.goto(f'http://127.0.0.1:{server.server_port}/')
                if width < 1024:
                    nav = page.get_by_role('navigation', name='Mobile navigation')
                    nav.get_by_role('link', name='Short trips').click()
                    playwright.expect(nav.get_by_role('link', name='Short trips')).to_have_attribute('aria-current', 'page')
                    page.locator('.nav-more summary').click()
                    playwright.expect(nav.get_by_role('link', name='Scan settings')).to_be_visible()
                    page.keyboard.press('Escape')
                    assert page.locator('.nav-more').get_attribute('open') is None
                    page.locator('.nav-more summary').click()
                    nav.get_by_role('link', name='System status').click()
                    page.locator('.nav-more summary').click()
                    playwright.expect(nav.get_by_role('link', name='System status')).to_have_attribute('aria-current', 'page')
                else:
                    playwright.expect(page.get_by_role('navigation', name='Main navigation')).to_be_visible()
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            browser.close()
    finally:
        server.shutdown()
        worker.join(timeout=5)


def test_multi_airport_request_reuses_network_and_respects_changed_exclusions():
    from unittest.mock import Mock
    graph = Graph()
    graph.edges_for_day = Mock(wraps=graph.edges_for_day)
    db = Cache()
    db.checked_routes = Mock(wraps=db.checked_routes)
    cache = SearchCache(db, 'run')
    scope = {}
    for city in ('Rome', 'Athens'):
        cached_scan_itineraries(graph, cache, 'Liverpool', city, date(2026, 9, 12),
                                days=2, pdf_run_id='run', scope=scope)
    assert graph.edges_for_day.call_count == 2 and db.checked_routes.call_count == 1
    scope['excluded_airports'] = ['Budapest']
    assert 'Budapest' not in cache.adjacency(graph, date(2026, 9, 12), scope).get('Liverpool', [])
    assert 'Budapest' in cache.adjacency(graph, date(2026, 9, 12), {}).get('Liverpool', [])


def test_planner_browser_add_remove_and_submit_typed_destination(tmp_path, monkeypatch):
    import pytest
    import threading
    from datetime import datetime, time
    from werkzeug.serving import make_server
    from app import create_app
    from cache_db import ScanCacheDB
    from scanner import Flight
    from termux.multi_search import bp as multi_search
    playwright = pytest.importorskip('playwright.sync_api')

    day = date.today()
    direct = tmp_path / 'direct-data'
    direct.mkdir()
    (direct / 'routes.csv').write_text('departure_from,departure_to,data_generated\n' +
        ''.join(f'{a},{b},{day}T07:00:00\n' for a, b in [
            ('Liverpool', 'Budapest'), ('Budapest', 'Liverpool'), ('Liverpool', 'Athens')]))
    monkeypatch.setenv('AYCF_CACHE_DIR', str(tmp_path))
    app = create_app()
    app.register_blueprint(multi_search)
    app.jinja_env.globals['multi_search_enabled'] = True
    assert app.test_client().get('/').status_code == 200
    db = ScanCacheDB()
    run = db.latest_pdf_run()['run_id']
    dep = datetime.combine(day, time(12))
    arr = datetime.combine(day, time(15))
    db.replace_route_check(run, 'Liverpool', 'Budapest', day, [
        Flight('Liverpool', 'Budapest', 'W6123', dep, arr, '12:00', '15:00')])
    server = make_server('127.0.0.1', 0, app)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch()
            page = browser.new_page(viewport={'width': 390, 'height': 844})
            base = f'http://127.0.0.1:{server.server_port}'
            page.goto(base)
            playwright.expect(page.locator('main').get_by_role('link', name='Manchester connections', exact=False)).to_have_attribute('href', '/feeder-trips')
            field = page.get_by_label('Go to', exact=True)
            field.fill('Athens')
            field.press('Enter')
            page.get_by_role('button', name='Remove Athens').click()
            field.fill('wrong city')
            page.get_by_role('button', name='Find routes').click()
            assert page.url == base + '/' and not field.evaluate('node => node.validity.valid')
            field.fill('budapest')
            page.get_by_role('button', name='Find routes').click()
            playwright.expect(page.locator('h1')).to_contain_text('Any scanned origin')
            playwright.expect(page.locator('.itinerary-card').first).to_contain_text('W6123')
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            browser.close()
    finally:
        server.shutdown()
        worker.join(timeout=5)
