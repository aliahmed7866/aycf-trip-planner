from datetime import datetime, timezone
import json
from pathlib import Path
import re
from unittest.mock import Mock

from flask import Flask
import pytest

from termux import admin_hub, health_ui, run_state

NOW = datetime(2026, 9, 17, 12, tzinfo=timezone.utc).timestamp()


@pytest.fixture
def state(monkeypatch):
    scan = {'state': 'rate_limited', 'http_status': 429, 'updated_at': NOW,
            'retry_at': '2026-09-17T12:30:00+00:00', 'cooldown_until': NOW + 1800,
            'effective_request_interval': 4}
    supervisor = {'state': 'rate_limited', 'health_ok': True, 'updated_at': NOW,
                  'cooldown_until': NOW + 1800, 'effective_request_interval': 4}
    shared = {'blocked': True, 'cooldown_until': NOW + 1800, 'effective_request_interval': 4}
    monkeypatch.setattr(health_ui.time, 'time', lambda: NOW)
    monkeypatch.setattr(run_state, 'read_status', lambda: dict(scan))
    monkeypatch.setattr(health_ui, '_json_file', lambda name: dict(supervisor) if name.startswith('supervisor') else
                        {'ok': True, 'state': 'verified', 'updated_at': NOW})
    monkeypatch.setattr(health_ui, '_shared_rate_limit_status', lambda: dict(shared))
    monkeypatch.setattr(health_ui, '_current_logs', lambda: {})
    return scan, supervisor, shared


def test_rate_limited_is_paused_with_deadline_and_pacing_not_expired_auth(state):
    snapshot = health_ui._snapshot()
    assert not snapshot['ok']
    assert snapshot['status_label'] == 'Wizz scan paused'
    assert snapshot['rate_limit']['blocked']
    assert snapshot['rate_limit']['retry_label'] == '17 Sep 2026, 12:30:00 UTC'
    assert snapshot['rate_limit']['effective_request_interval'] == 4
    assert 'at least 4s' in snapshot['guidance']
    assert 'Saved flights remain searchable' in snapshot['guidance']
    assert 'encrypted session is retained' in snapshot['guidance']
    assert snapshot['browser_bridge']['state'] == 'not_needed'
    assert 'expired' not in snapshot['browser_bridge']['detail'].lower()


def test_shared_block_is_visible_while_scan_still_reports_running(state):
    scan, supervisor, shared = state
    scan.clear()
    scan.update(state='running', updated_at=NOW + 1)
    assert health_ui._snapshot()['rate_limit']['blocked']
    assert health_ui._snapshot()['status_label'] == 'Wizz scan paused'


def test_expired_deadline_unblocks_controls_even_if_saved_state_is_unchanged(state, monkeypatch):
    monkeypatch.setattr(health_ui.time, 'time', lambda: NOW + 1801)
    snapshot = health_ui._snapshot()
    assert not snapshot['rate_limit']['blocked']
    assert snapshot['status_label'] == 'Cooldown finished'
    assert 'next supervisor wake' in snapshot['guidance']
    assert snapshot['rate_limit']['remaining_seconds'] == 0


def test_new_completed_scan_supersedes_old_pause_notice(state):
    scan, supervisor, shared = state
    scan.clear()
    scan.update(state='complete', updated_at=NOW + 1)
    shared.update(blocked=False, cooldown_until=0)
    snapshot = health_ui._snapshot()
    assert snapshot['ok']
    assert not snapshot['rate_limit']['notice']
    assert snapshot['status_label'] == 'All systems operational'


def test_new_auth_failure_is_not_hidden_by_old_expired_pause(state):
    scan, supervisor, shared = state
    scan.clear()
    scan.update(state='auth_failed', updated_at=NOW + 1)
    shared.update(blocked=False, cooldown_until=0)
    result = health_ui._snapshot()
    assert not result['rate_limit']['notice']
    assert result['status_label'] == 'Attention required'


def test_current_shared_pacing_supersedes_old_scan_interval(state):
    scan, supervisor, shared = state
    scan['effective_request_interval'] = 30
    shared['effective_request_interval'] = 5
    assert health_ui._snapshot()['rate_limit']['effective_request_interval'] == 5


def test_invalid_saved_retry_fields_do_not_crash_or_invent_a_deadline(state):
    scan, supervisor, shared = state
    for record in state:
        record.update(cooldown_until='nan', retry_at='invalid', effective_request_interval='inf')
    shared['blocked'] = False
    result = health_ui._snapshot()['rate_limit']
    assert not result['blocked']
    assert result['retry_at'] is None and result['effective_request_interval'] is None
    assert 'not available' in result['guidance']


def health_app():
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'templates'))
    app.secret_key = 'testing'
    app.jinja_env.globals['csrf_token'] = lambda: 'token'
    app.register_blueprint(health_ui.bp)
    for path, endpoint in [('/', 'index'), ('/flights', 'all_flights'), ('/watches', 'watches.watchlist')]:
        app.add_url_rule(path, endpoint, lambda: '')
    return app


def test_system_html_and_json_use_local_status_and_keep_cached_navigation(state, monkeypatch):
    network = Mock(side_effect=AssertionError('Status GET must not contact the network'))
    spawn = Mock(side_effect=AssertionError('Status GET must not launch processes'))
    monkeypatch.setattr('requests.sessions.Session.request', network)
    monkeypatch.setattr(health_ui.subprocess, 'Popen', spawn)
    client = health_app().test_client()
    html = client.get('/system').get_data(as_text=True)
    assert 'Wizz scan paused' in html and '17 Sep 2026, 12:30:00 UTC' in html
    buttons = re.findall(r'<button\b[^>]*data-wizz-network-action[^>]*>', html)
    assert len(buttons) == 3 and all('disabled' in button for button in buttons)
    reset_form = html.split('id="fresh-scan-form"', 1)[1].split('</form>', 1)[0]
    assert 'disabled' not in reset_form and 'data-wizz-network-action' not in reset_form
    assert 'href="/flights"' in html and 'Browse cached flights' in html
    response = client.get('/system/status.json')
    assert response.status_code == 200 and response.headers['Cache-Control'] == 'no-store'
    assert response.json['rate_limit']['blocked']
    network.assert_not_called()
    spawn.assert_not_called()


@pytest.mark.parametrize('path', ['/system/run-scan', '/system/repair-auth', '/system/check-now'])
def test_manual_controls_cannot_restart_wizz_requests_during_cooldown(state, monkeypatch, path):
    spawn = Mock()
    monkeypatch.setattr(health_ui.subprocess, 'Popen', spawn)
    client = health_app().test_client()
    with client.session_transaction() as session:
        session['csrf_token'] = 'token'
    response = client.post(path, data={'csrf_token': 'token'})
    assert response.status_code == 302
    spawn.assert_not_called()


def test_expired_cooldown_reenables_manual_controls(state, monkeypatch):
    monkeypatch.setattr(health_ui.time, 'time', lambda: NOW + 1801)
    client = health_app().test_client()
    html = client.get('/system').get_data(as_text=True)
    buttons = re.findall(r'<button\b[^>]*data-wizz-network-action[^>]*>', html)
    assert len(buttons) == 3 and all('disabled' not in button for button in buttons)


def test_hub_shows_scan_pause_without_marking_running_planner_offline(state, monkeypatch, tmp_path):
    scan, supervisor, _ = state
    (tmp_path / 'scan-status.json').write_text(json.dumps(scan))
    (tmp_path / 'supervisor-status.json').write_text(json.dumps(supervisor))
    monkeypatch.setattr(admin_hub, 'STATE_DIR', tmp_path)
    managed = {'id': 'aycf', 'name': 'AYCF Trip Planner', 'description': 'AYCF', 'icon': 'A',
               'state': 'running', 'available': True, 'port': 8080, 'health_text': 'OK',
               'service_text': 'Running', 'open_url': 'http://127.0.0.1:8080',
               'update_ready': False, 'update_status': {}, 'command_details': {},
               'actions': [{'id': 'morning_scan', 'label': 'Run morning scan', 'command': ['python', 'termux/runtime.py', 'morning']}]}
    monkeypatch.setattr(admin_hub, '_load_registry', lambda: [dict(managed)])
    monkeypatch.setattr(admin_hub, 'app_status', lambda item: item)
    monkeypatch.setattr(admin_hub, '_trusted_local_request', lambda: True)
    monkeypatch.setattr(admin_hub, '_system_status', lambda: {'uptime': '1d', 'storage': '10GB', 'storage_pct': '50%'})
    client = admin_hub.create_app().test_client()
    launcher = client.get('/').get_data(as_text=True)
    assert 'Wizz scan paused' not in launcher
    assert 'http://127.0.0.1:8080' in launcher
    html = client.get('/manage').get_data(as_text=True)
    assert 'Wizz scan paused' in html and '17 Sep 2026, 12:30:00 UTC' in html
    assert 'badge running' in html and 'http://127.0.0.1:8080' in html
    assert re.search(r'<button class="ghost"\s+disabled>Run morning scan', html)
    start = Mock()
    monkeypatch.setattr(admin_hub, '_start_command', start)
    with client.session_transaction() as session:
        token = session['csrf_token']
    response = client.post('/apps/aycf/action/morning_scan', data={'csrf_token': token})
    assert response.status_code == 302
    start.assert_not_called()
