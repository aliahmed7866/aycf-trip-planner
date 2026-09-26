"""Exercise Hub polling and phone controls against a local fixture, without providers."""
import os
from pathlib import Path
import threading

import pytest
from werkzeug.serving import make_server
from tests.test_hub_live_status import workspace

playwright = pytest.importorskip('playwright.sync_api')


@pytest.fixture
def hub_server(workspace):
    app, rows = workspace
    server = make_server('127.0.0.1', 0, app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', rows
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.mark.parametrize('width', [360, 390, 1280])
def test_live_mobile_controls_filters_and_recovery(hub_server, width):
    url, rows = hub_server
    with playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch()
        page = browser.new_page(viewport={'width': width, 'height': 844})
        errors = []
        navigations = []
        page.on('framenavigated', lambda frame: navigations.append(frame.url) if frame.parent_frame is None else None)
        page.on('pageerror', lambda error: errors.append(str(error)))
        expect = playwright.expect
        try:
            page.goto(url + '/manage')
            page.wait_for_function('Boolean(navigator.serviceWorker.controller)')
            assert len(navigations) == 1
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            if width < 760:
                nav = page.get_by_role('navigation', name='Hub navigation').bounding_box()
                assert nav and nav['y'] > 700 and nav['y'] + nav['height'] <= 844
            page.get_by_role('button', name='Needs attention', exact=True).click()
            expect(page.locator('#no-apps')).to_be_visible()
            rows[0]['update_status'] = dict(state='deferred', message='deferred scan-active 2026-09-17')
            page.get_by_role('button', name='Refresh status', exact=True).click()
            expect(page.locator('#app-aycf')).to_be_visible()
            expect(page.locator('#app-places')).to_be_hidden()
            expect(page.locator('#app-aycf')).to_contain_text('Update waiting')
            page.get_by_role('button', name='All apps', exact=True).click()
            page.get_by_role('searchbox').fill('aycf')
            page.locator('#app-aycf summary').click()
            page.get_by_role('searchbox').focus()
            rows[0]['update_status'] = dict(state='success', message='Latest changes installed.')
            page.get_by_role('button', name='Refresh status', exact=True).click()
            expect(page.locator('#app-aycf')).to_contain_text('Update installed')
            assert page.locator('#app-aycf details').evaluate('(item) => item.open')
            expect(page.get_by_role('searchbox')).to_have_value('aycf')
            expect(page.locator('#app-places')).to_be_hidden()
            page.route('**/workspace-status?*', lambda route: route.abort())
            page.get_by_role('button', name='Refresh status', exact=True).click()
            expect(page.locator('#connection-status')).to_contain_text('Reconnecting')
            expect(page.locator('#app-aycf')).to_be_visible()
            page.unroute('**/workspace-status?*')
            page.get_by_role('button', name='Refresh status', exact=True).click()
            expect(page.locator('#connection-status')).to_contain_text('Live')
            rows[0]['update_status'] = dict(state='running', message='Installing latest changes.')
            page.get_by_role('button', name='Refresh status', exact=True).click()
            expect(page.locator('#app-aycf').get_by_role('button', name='Updating…')).to_be_disabled()
            expect(page.locator('#app-aycf').get_by_role('button', name='Restart', exact=True)).to_be_disabled()
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            destination = Path(os.environ.get('BROWSER_ARTIFACT_DIR', 'browser-artifacts'))
            destination.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(destination / f'hub-live-manage-{width}.png'), full_page=True)
            status_requests = []
            page.on('request', lambda request: status_requests.append(request.url) if '/workspace-status' in request.url else None)
            page.goto(url)
            status_requests.clear()
            expect(page.locator('#app-aycf')).not_to_contain_text('Installing update')
            expect(page.get_by_role('link', name='Open My Places', exact=True)).to_have_attribute('href', 'http://127.0.0.1:8094')
            expect(page.locator('.metrics, .filter-row, .badge, #connection-status')).to_have_count(0)
            page.clock.install()
            page.clock.run_for(25000)
            assert not status_requests
            page.get_by_role('searchbox').fill('places')
            expect(page.locator('#app-places')).to_be_visible()
            expect(page.locator('#app-aycf')).to_be_hidden()
            page.get_by_role('searchbox').fill('')
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            page.screenshot(path=str(destination / f'hub-live-apps-{width}.png'), full_page=True)
            assert not errors
        finally:
            browser.close()


def test_focused_card_still_refreshes_health_and_controls(hub_server):
    url, rows = hub_server
    with playwright.sync_playwright() as runtime:
        browser = runtime.chromium.launch()
        page = browser.new_page(viewport={'width': 390, 'height': 844})
        expect = playwright.expect
        try:
            page.goto(url + '/manage')
            summary = page.locator('#app-aycf summary')
            summary.focus()
            rows[0].update(state='stopped', health_text='Connection refused', service_text='down: aycf')
            rows[0]['update_status'] = dict(state='running', message='Installing latest changes.')
            page.evaluate("document.getElementById('refresh-status').click()")
            expect(page.locator('#app-aycf .badge')).to_have_text('Stopped')
            expect(page.locator('#app-aycf .service-facts')).to_contain_text('Connection refused')
            expect(page.locator('#app-aycf')).to_have_attribute('data-state', 'stopped')
            expect(page.locator('#app-aycf')).to_contain_text('Installing update')
            expect(page.locator('#app-aycf').get_by_role('button', name='Restart', exact=True)).to_be_disabled()
            expect(page.locator('#running-count')).to_have_text('0')
            expect(summary).to_be_focused()
        finally:
            browser.close()
