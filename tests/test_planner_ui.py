"""Exercise the actual shared assets and offline first-launch flows."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import os
import threading

import pandas as pd
import pytest
from werkzeug.serving import make_server

from airport_catalog import airport_code
from cache_db import ScanCacheDB
from scanner import Flight


@pytest.fixture
def planner_app(tmp_path, monkeypatch):
    import app as web
    from scan_scope import default_scope
    now = datetime.now(timezone.utc)
    start = (now + timedelta(days=1)).date()
    end = start + timedelta(days=3)
    frame = pd.DataFrame([dict(departure_from=a, departure_to=b, data_generated=now.isoformat(),
                              departure_start=str(start), departure_end=str(end)) for a, b in [
                                  ('Liverpool', 'Budapest'), ('Budapest', 'Liverpool'),
                                  ('London Luton', 'Budapest')]])
    graph = SimpleNamespace(latest_frame=lambda: frame, cities=lambda: ['Liverpool', 'London Luton', 'Budapest'])
    db = ScanCacheDB(str(tmp_path / 'scan.sqlite'))
    scope = dict(default_scope(), origins=['Liverpool', 'London Luton'])
    monkeypatch.setattr(web, 'CurrentRouteGraph', lambda _: graph)
    monkeypatch.setattr(web, '_cache_dir', lambda: str(tmp_path))
    monkeypatch.setattr(web, 'update_data_if_needed', lambda **_: SimpleNamespace(data_dir=str(tmp_path)))
    monkeypatch.setattr(web, 'ScanCacheDB', lambda: db)
    monkeypatch.setattr(web, 'load_scope', lambda: scope)
    monkeypatch.setattr(web, 'scan_scope_with_preferences', lambda value: value)
    monkeypatch.setattr(web, 'scan_run_id', lambda *args: 'current')
    monkeypatch.setattr(web, '_vault_or_none', lambda: None)
    app = web.create_app()
    app.testing = True
    app.jinja_env.globals['android_app'] = True
    app.config['TEST_DB'] = db
    app.config['TEST_START'] = start
    return app


def seed(app):
    start = app.config['TEST_START']
    db = app.config['TEST_DB']
    # Initialise the current run through the real planner, then save two checks.
    app.test_client().get('/')
    for origin, dest, day, dep, arr in [
        ('Liverpool', 'Budapest', start, '08:00', '11:00'),
        ('Budapest', 'Liverpool', start + timedelta(days=2), '19:00', '21:00'),
    ]:
        db.replace_route_check('current', origin, dest, day, [Flight(origin, dest, 'W1234',
            datetime.fromisoformat(f'{day}T{dep}'), datetime.fromisoformat(f'{day}T{arr}'), '', '')])


@pytest.mark.parametrize('width', [320, 390, 820, 1280])
def test_planner_browser_search_empty_state_shortcuts_and_preferences(planner_app, width, tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    server = make_server('127.0.0.1', 0, planner_app, threaded=True)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    address = f'http://127.0.0.1:{server.server_port}'
    try:
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch()
            context = browser.new_context(viewport={'width': width, 'height': 844},
                                          permissions=['clipboard-read', 'clipboard-write'])
            page = context.new_page()
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.goto(address + '/flights')
            playwright.expect(page.get_by_text('No completed cache for this scope.')).to_be_visible()
            assert not errors  # Previously crashed when the filter form was absent.
            seed(planner_app)
            page.goto(address)
            playwright.expect(page.get_by_role('navigation', name='Mobile navigation')).to_be_visible()
            field = page.get_by_label('Go to', exact=True)
            field.fill('BUD')
            field.press('ArrowDown')
            page.keyboard.press('Enter')
            playwright.expect(page.get_by_role('button', name='Remove Budapest')).to_be_visible()
            assert page.locator('[name=destinations]').input_value() == 'Budapest'
            page.get_by_role('button', name='Remove Budapest').click()
            page.get_by_role('button', name='Find routes').click()
            playwright.expect(page.locator('#search-error')).to_contain_text('Choose a starting airport')
            assert not page.get_by_role('button', name='Find routes').is_disabled()
            field.fill('zzzzzz')
            playwright.expect(page.locator('#destination-options')).to_contain_text('No matching destination')
            field.press('Escape')
            playwright.expect(field).to_have_attribute('aria-expanded', 'false')
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            page.goto(address)
            shots = Path(os.environ.get('BROWSER_ARTIFACT_DIR', tmp_path))
            shots.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(shots / f'planner-{width}.png'), full_page=True)
            page.goto(address + '/short-trips')
            page.get_by_role('button', name='This weekend', exact=True).click()
            assert page.locator('#return-by').input_value().endswith('T09:00')
            playwright.expect(page.locator('#preset-note')).to_contain_text('will not appear yet')
            page.get_by_role('button', name='Any day', exact=True).click()
            assert page.locator('#leave-after').input_value() == ''
            page.locator('[name=origins][value="London Luton"]').uncheck()
            page.get_by_role('button', name='Use departure airports for return').click()
            assert not page.locator('[name=returns][value="London Luton"]').is_checked()
            page.get_by_role('button', name='Find complete trips').click()
            playwright.expect(page.get_by_role('heading', name='1 complete trip found')).to_be_visible()
            assert not page.locator('#trip-search-panel').get_attribute('open')
            page.get_by_role('button', name='Copy itinerary').click()
            playwright.expect(page.locator('#app-feedback')).to_contain_text('Trip copied')
            copied = page.evaluate('navigator.clipboard.readText()')
            assert 'Liverpool → Budapest' in copied and 'Budapest → Liverpool' in copied
            assert 'recheck every seat' in copied
            page.get_by_text('Flight details · 2 flights', exact=True).click()
            playwright.expect(page.get_by_role('heading', name='Outbound', exact=True)).to_be_visible()
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            page.evaluate('document.activeElement.blur(); window.scrollTo(0, 0)')
            page.screenshot(path=str(shots / f'getaway-{width}.png'), full_page=True)
            page.goto(address + '/short-trips')
            assert page.locator('[name=origins][value="Liverpool"]').is_checked()
            assert not page.locator('[name=origins][value="London Luton"]').is_checked()
            assert page.locator('#leave-after').input_value() == ''
            assert not errors
            browser.close()
    finally:
        server.shutdown()
        worker.join(timeout=5)
