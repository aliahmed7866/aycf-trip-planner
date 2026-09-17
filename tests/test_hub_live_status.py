import subprocess
from unittest.mock import Mock

import pytest
from termux import admin_hub as hub


@pytest.fixture
def workspace(monkeypatch, tmp_path):
    rows = [dict(id='aycf', name='AYCF Trip Planner', state='running', available=True,
                 description='Flight planning', icon='A', open_url='http://127.0.0.1:8080',
                 working_dir=str(tmp_path), port=8080, health_text='HTTP 200', service_text='Running',
                 update_ready=True, update_branch='deploy/termux', update_status={},
                 command_details={'start': 'python app.py'}, actions=[]),
            dict(id='places', name='My Places', state='stopped', available=True,
                 description='Your travel map', icon='P', open_url='http://127.0.0.1:8094',
                 working_dir=str(tmp_path), port=8094, health_text='Not running', service_text='Stopped',
                 update_ready=True, update_branch='main', update_status={}, command_details={}, actions=[])]
    monkeypatch.setattr(hub, '_load_registry', lambda: [dict(row) for row in rows])
    monkeypatch.setattr(hub, 'app_status', lambda item: item)
    monkeypatch.setattr(hub, '_aycf_rate_limit', lambda: {})
    monkeypatch.setattr(hub, '_trusted_local_request', lambda: True)
    return hub.create_app(), rows


def test_status_has_no_store_and_updates_without_page_reload(workspace):
    app, rows = workspace
    client = app.test_client()
    rows[0]['update_status'] = dict(state='deferred', message='deferred scan-active 2026-09-17')
    response = client.get('/workspace-status?section=manage')
    data = response.get_json()
    assert response.headers['Cache-Control'] == 'no-store'
    assert data['totals'] == dict(total=2, running=1, attention=1)
    assert not data['busy']
    assert 'Update waiting' in data['cards']
    assert 'A scan is running. Try the update again after it finishes.' in data['cards']
    assert 'deferred scan-active' not in data['cards']
    assert data['csrf']
    assert 'Refresh' not in client.get('/manage').headers
    rows[0]['update_status'] = dict(state='success', message='Latest changes installed.')
    data = client.get('/workspace-status?section=manage').get_json()
    assert data['totals']['attention'] == 0
    assert 'Update installed' in data['cards']


def test_status_requires_auth_and_never_runs_mutations(workspace, monkeypatch):
    app, _ = workspace
    mutate = Mock(side_effect=AssertionError('Status must only read'))
    for name in ('start_app', 'stop_app', 'restart_app', 'start_update', '_start_command'):
        monkeypatch.setattr(hub, name, mutate)
    client = app.test_client()
    assert client.get('/workspace-status').status_code == 200
    monkeypatch.setattr(hub, '_trusted_local_request', lambda: False)
    assert client.get('/workspace-status').status_code == 401
    mutate.assert_not_called()


def test_cooldown_keeps_planner_online_but_counts_attention(workspace, monkeypatch):
    app, _ = workspace
    monkeypatch.setattr(hub, '_aycf_rate_limit', lambda: dict(notice=True, blocked=True,
                        label='Wizz scan paused', guidance='Resume after cooldown.'))
    data = app.test_client().get('/workspace-status?section=manage').get_json()
    assert data['totals'] == dict(total=2, running=1, attention=1)
    assert 'Wizz scan paused' in data['cards']
    assert 'badge running' in data['cards']


def test_installed_revision_is_local_and_missing_git_is_tolerated(workspace, monkeypatch, tmp_path):
    _, rows = workspace
    (tmp_path / '.git').mkdir()
    output = Mock(return_value='abcdef0123\n')
    monkeypatch.setattr(hub.subprocess, 'check_output', output)
    assert hub._present_app(rows[0])['revision'] == 'abcdef0123'
    assert output.call_args.args[0] == ['git', '--no-pager', 'rev-parse', '--short=10', 'HEAD']
    output.side_effect = subprocess.TimeoutExpired('git', 2)
    assert hub._present_app(rows[0])['revision'] == ''


def test_status_escapes_messages_and_only_accepts_known_section(workspace):
    app, rows = workspace
    rows[0]['update_status'] = dict(state='error', message='<script>alert(1)</script>')
    data = app.test_client().get('/workspace-status?section=manage').get_json()
    assert '<script>' not in data['cards']
    assert '&lt;script&gt;' in data['cards']
    launcher = app.test_client().get('/workspace-status?section=../../other').get_json()
    assert 'app-tile' in launcher['cards']
    assert 'alert(1)' not in launcher['cards']
    assert launcher['totals'] == {'total': 2}
    assert not launcher['busy']


def test_apps_is_direct_launcher_without_status_probes(workspace, monkeypatch):
    app, rows = workspace
    rows[0]['update_status'] = dict(state='running', message='Installing latest changes.')
    probe = Mock(side_effect=AssertionError('Launcher must not inspect app health'))
    for name in ('app_status', '_system_status', '_aycf_rate_limit'):
        monkeypatch.setattr(hub, name, probe)
    client = app.test_client()
    html = client.get('/').get_data(as_text=True)
    assert 'href="http://127.0.0.1:8094" aria-label="Open My Places"' in html
    assert 'href="http://127.0.0.1:8080" aria-label="Open AYCF Trip Planner"' in html
    for text in ('Installing latest changes.', 'Workspace health', 'Needs attention',
                 'Refresh status', 'Pull latest', 'connection-status', 'data-status-url', 'badge running'):
        assert text not in html
    assert 'href="/manage"' in html
    assert 'No apps match your search.' in html
    client.get('/workspace-status')
    probe.assert_not_called()


def test_missing_launcher_link_does_not_redirect_to_manage(workspace):
    app, rows = workspace
    rows[1]['open_url'] = ''
    cards = app.test_client().get('/workspace-status').get_json()['cards']
    assert 'No app link configured' in cards
    assert 'href="/manage' not in cards


def test_custom_action_returns_to_manage(workspace, monkeypatch):
    app, rows = workspace
    rows[1]['actions'] = [dict(id='sync', label='Sync places', command=['places', 'sync'])]
    start = Mock(return_value=123)
    monkeypatch.setattr(hub, '_start_command', start)
    client = app.test_client()
    client.get('/manage')
    with client.session_transaction() as session:
        token = session['csrf_token']
    response = client.post('/apps/places/action/sync', data={'csrf_token': token})
    assert response.location == '/manage'
    start.assert_called_once()
