"""Local-only log export and 429 response diagnostics."""
import io
import json
from pathlib import Path
import sqlite3
from unittest.mock import Mock
import zipfile

import pytest
import requests

from diagnostic_text import response_details, redact_text
from scanner import WizzAYCFClient
from termux import diagnostic_downloads as downloads, health_ui
import wizz_rate_limit as limits


def response(body=b'', content_type='application/json'):
    result = requests.Response()
    result.status_code = 429
    result._content = body
    result.headers.update({'Content-Type': content_type, 'Retry-After': '120',
                           'Set-Cookie': 'session=never-export-header'})
    return result


def test_429_capture_records_provider_body_and_preserves_cooldown(monkeypatch):
    client = WizzAYCFClient({})
    client._throttle = lambda: None
    client.dynamic_url = 'https://example.invalid/availability?key=never-export-url'
    result = response(json.dumps({'message': 'Too many requests', 'code': 'RATE_LIMITED',
                                 'access_token': 'never-export-token',
                                 'account': {'password': 'never-export-password'}}).encode())
    client.http.request = Mock(return_value=result)
    with pytest.raises(limits.WizzRateLimited) as error:
        client._request('POST', client.dynamic_url)
    event = error.value.status['last_limit']
    assert event['retry_after_seconds'] == 120
    assert event['policy_wait_seconds'] == 900
    assert event['deadline_source'] == 'policy'
    assert 'Too many requests' in event['response']['body_preview']
    assert event['response']['http_status'] == 429
    assert 'never-export' not in json.dumps(event)
    assert b'never-export' not in limits._path().read_bytes()
    assert limits.rate_limit_events() == [event]
    client.http.request.assert_called_once()


def test_auth_capture_and_html_never_include_scripts_or_form_values():
    from termux.refresh_wizz_from_chrome import _auth_request
    result = response(b'<h1>Too Many Requests</h1><script>token="secret-script"</script>'
                      b'<input value="secret-input"><p>Retry later</p>', 'text/html; charset=utf-8')
    with pytest.raises(limits.WizzRateLimited):
        _auth_request(Mock(return_value=result), 'https://example.invalid/login')
    preview = limits.rate_limit_events()[0]['response']['body_preview']
    assert preview == 'Too Many Requests\nRetry later'
    assert b'secret-' not in limits._path().read_bytes()


def test_preview_is_bounded_and_unknown_or_incomplete_bodies_are_omitted():
    captured = response_details(response(b'a ' * 20000, 'text/plain'))
    assert captured['preview_truncated'] and len(captured['body_preview']) <= 4096
    assert captured['body_bytes'] == 40000
    assert 'omitted' in response_details(response(b'{"token":"partial', 'application/json'))['body_preview']
    assert 'omitted' in response_details(response(b'private bytes', 'application/octet-stream'))['body_preview']


def test_diagnostic_failure_cannot_prevent_cooldown(monkeypatch):
    monkeypatch.setattr('diagnostic_text.response_details', Mock(side_effect=ValueError('malformed body')))
    state = limits.record_rate_limit(response=response())
    assert state['blocked'] and state['last_limit']['response']['http_status'] == 429


def test_previous_schema_migrates_on_write_and_keeps_existing_event():
    limits.record_rate_limit()
    with sqlite3.connect(limits._path()) as db:
        db.execute('ALTER TABLE rate_limit_events DROP COLUMN response_json')
    before = limits._path().read_bytes()
    assert 'response' not in limits.rate_limit_events()[0]
    assert before == limits._path().read_bytes()
    deadline = limits.rate_limit_status()['cooldown_until']
    state = limits.record_rate_limit(response=response(b'{"message":"Too many requests"}'))
    assert state['cooldown_until'] == deadline
    assert len(limits.rate_limit_events()) == 2
    assert limits.rate_limit_events()[0]['response']['http_status'] == 429


@pytest.fixture
def export_client(monkeypatch, tmp_path):
    import app as base_app
    monkeypatch.setenv('AYCF_CACHE_DIR', str(tmp_path))
    monkeypatch.setenv('AYCF_DB_PATH', str(tmp_path / 'scan.sqlite3'))
    (tmp_path / 'direct-data').mkdir()
    (tmp_path / 'direct-data' / 'catalog.csv').write_text('graph mocked')
    monkeypatch.setattr(base_app, 'CurrentRouteGraph', Mock())
    monkeypatch.setattr(requests.sessions.Session, 'request', Mock(side_effect=AssertionError('No network')))
    monkeypatch.setenv('AYCF_APP_PASSWORD', 'test-password')
    monkeypatch.setattr(health_ui, 'LOG_DIR', tmp_path)
    monkeypatch.setattr(downloads, 'installed_revision', lambda _: 'abc1234567')
    app = base_app.create_app()
    app.register_blueprint(health_ui.bp)
    return app.test_client(), tmp_path


def test_downloads_require_same_login_as_planner(export_client):
    client, root = export_client
    (root / 'auth-repair.log').write_text('auth log')
    for path in ('/system/logs/auth/download', '/system/diagnostics/download'):
        result = client.get(path, environ_base={'REMOTE_ADDR': '192.0.2.1'})
        assert result.status_code in (302, 401)
        assert b'auth log' not in result.data
    with client.session_transaction() as session:
        session['aycf_authenticated'] = True
    assert client.get('/system/logs/auth/download', environ_base={'REMOTE_ADDR': '192.0.2.1'}).status_code == 200


def test_individual_log_is_text_attachment_redacted_and_allowlisted(export_client):
    client, root = export_client
    (root / 'manual-morning.log').write_text('scan saved\nAuthorization: Bearer private-auth\n'
        'SERPAPI_API_KEY=private-key\nURL https://example.invalid/path?api_key=private-url\n'
        'email user@example.invalid\npassword: private-password\nHTTP 429\n')
    result = client.get('/system/logs/scan/download')
    assert result.status_code == 200
    assert result.mimetype == 'text/plain'
    assert 'attachment;' in result.headers['Content-Disposition']
    assert result.headers['Cache-Control'] == 'no-store'
    assert result.headers['X-Content-Type-Options'] == 'nosniff'
    assert b'HTTP 429' in result.data and b'scan saved' in result.data
    assert b'private-' not in result.data and b'user@example' not in result.data
    for key in ('auth', 'unknown', '..%2Fsecret', 'wizz-runtime.json'):
        assert client.get(f'/system/logs/{key}/download').status_code == 404


def test_export_caps_logs_and_refuses_symlinks(export_client, monkeypatch):
    client, root = export_client
    monkeypatch.setattr(downloads, 'MAX_LOG_BYTES', 64)
    (root / 'supervisor.log').write_text('old secret prefix\n' + 'entry\n' * 50)
    result = client.get('/system/logs/supervisor/download')
    assert b'TRUNCATED' in result.data and b'old secret prefix' not in result.data
    (root / 'secret').write_text('never-export')
    (root / 'auth-repair.log').symlink_to(root / 'secret')
    assert client.get('/system/logs/auth/download').status_code == 404


def test_zip_has_timestamps_version_and_429_evidence_without_network_or_mutation(export_client, monkeypatch):
    client, root = export_client
    (root / 'supervisor.log').write_text('supervisor wake\n')
    limits.record_rate_limit(120, operation='availability', retry_after_kind='seconds',
                             response=response(b'{"message":"Too many requests"}'))
    before = limits._path().read_bytes()
    monkeypatch.setattr(requests.sessions.Session, 'request', Mock(side_effect=AssertionError('No network')))
    result = client.get('/system/diagnostics/download')
    assert result.status_code == 200 and result.mimetype == 'application/zip'
    assert result.headers['Cache-Control'] == 'no-store'
    with zipfile.ZipFile(io.BytesIO(result.data)) as archive:
        assert set(archive.namelist()) == {'supervisor.log.txt', 'manual-morning.log.txt', 'auth-repair.log.txt', 'diagnostics.json'}
        report = json.loads(archive.read('diagnostics.json'))
        assert report['installed_revision'] == 'abc1234567'
        assert report['logs']['supervisor']['updated_at']
        assert not report['logs']['auth']['exists']
        assert report['rate_limit_events'][0]['response']['http_status'] == 429
        assert report['rate_limit_events'][0]['deadline_source'] == 'policy'
    assert limits._path().read_bytes() == before


def test_browser_download_links_and_live_log_refresh(monkeypatch, tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    import threading
    from werkzeug.serving import make_server
    from tests.test_rate_limit_status_ui import health_app
    monkeypatch.setattr(health_ui, 'LOG_DIR', tmp_path)
    monkeypatch.setattr(downloads, 'installed_revision', lambda _: 'abc1234567')
    (tmp_path / 'manual-morning.log').write_text('old scan entry\n')
    server = make_server('127.0.0.1', 0, health_app(), threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with playwright.sync_playwright() as runtime:
            browser = runtime.chromium.launch()
            page = browser.new_page(viewport={'width': 390, 'height': 844})
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.clock.install()
            page.goto(f'http://127.0.0.1:{server.server_port}/system')
            with page.expect_download() as download:
                page.get_by_role('link', name='Download diagnostics ZIP').click()
            assert download.value.suggested_filename.endswith('.zip')
            with zipfile.ZipFile(download.value.path()) as archive:
                assert 'diagnostics.json' in archive.namelist()
            page.locator('details').filter(has=page.locator('[data-log-age="scan"]')).locator('summary').click()
            with page.expect_download() as download:
                page.get_by_role('link', name='Download manual scan log').click()
            assert 'old scan entry' in Path(download.value.path()).read_text()
            assert not errors
            (tmp_path / 'manual-morning.log').write_text('new scan entry\n')
            page.clock.run_for(11000)
            playwright.expect(page.locator('[data-log-lines="scan"]')).to_contain_text('new scan entry')
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            commands = []
            monkeypatch.setattr(health_ui, '_csrf_ok', lambda: True)
            monkeypatch.setattr(health_ui, '_spawn', lambda *args: commands.append(args))
            with page.expect_navigation():
                page.get_by_role('button', name='Clear pending work & start fresh').click()
            assert commands[0][1][-1] == 'fresh'
            assert commands[0][2] == 'manual-morning.log'
            assert not errors
            browser.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)
