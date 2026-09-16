"""Automatic fares must follow the installed worker and current saved scope."""
from contextlib import nullcontext
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
from unittest.mock import Mock, patch

import pytest
import requests

from cache_db import ScanCacheDB
from termux import automated_morning, feeder_refresh
from termux.env_loader import load_termux_env
from termux.run_state import process_lock

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    # The loader writes os.environ directly, so restore newly loaded keys too.
    with patch.dict(os.environ):
        for name in tuple(os.environ):
            if name.startswith('AYCF_'):
                monkeypatch.delenv(name)
        paths = {name: tmp_path / name for name in ('config', 'state', 'cache')}
        for path in paths.values():
            path.mkdir()
        for name, value in paths.items():
            monkeypatch.setenv('AYCF_' + name.upper() + '_DIR', str(value))
        # None of these trigger tests may consume a provider request or fetch a PDF.
        network = Mock(side_effect=AssertionError('Unexpected external request'))
        monkeypatch.setattr(requests.sessions.Session, 'request', network)
        yield paths
        network.assert_not_called()


def saved_scan(paths):
    directory = paths['cache'] / 'direct-data'
    directory.mkdir()
    (directory / 'current.csv').write_text(
        'departure_from,departure_to,data_generated,availability_start,availability_end\n'
        'Liverpool,Budapest,2026-09-16T06:00:00,2026-09-16,2026-09-19\n'
        'Budapest,Kutaisi,2026-09-16T06:00:00,2026-09-16,2026-09-19\n',
        encoding='utf-8',
    )
    (paths['config'] / 'scan_scope.json').write_text(json.dumps({
        'origins': ['Liverpool'], 'destination_mode': 'all',
        'connection_hubs': ['Budapest'], 'connection_budget': 0,
    }))
    # Resolve the identity from the real local catalogue, then persist that scan.
    probe = Mock()
    probe.get_pdf_run.return_value = {'scanned_at': '2026-09-16T07:00:00'}
    ctx = feeder_refresh.local_scope_context(probe)
    assert ctx is not None
    db = ScanCacheDB(str(paths['state'] / 'custom-aycf.sqlite3'))
    db.upsert_pdf_run(ctx['run_id'], '2026-09-16T06:00:00', '2026-09-16', '2026-09-19', 2)
    db.mark_pdf_scanned(ctx['run_id'])
    return db, ctx


def test_worker_loads_private_env_and_uses_custom_database(isolated, monkeypatch, capsys):
    import feeder_automation
    import feeder_trips
    from watch_service import add_watch
    add_watch('Budapest', 'Kutaisi', any_date=True)
    db, ctx = saved_scan(isolated)
    for name in ('AYCF_STATE_DIR', 'AYCF_CACHE_DIR', 'AYCF_TERMUX_DB_PATH', 'AYCF_SERPAPI_KEY'):
        monkeypatch.delenv(name, raising=False)
    key = 'example-private-test-token'
    values = {'AYCF_STATE_DIR': isolated['state'], 'AYCF_CACHE_DIR': isolated['cache'],
              'AYCF_TERMUX_DB_PATH': db.path, 'AYCF_SERPAPI_KEY': key}
    (isolated['config'] / 'env').write_text(''.join(
        f'export {name}={shlex.quote(str(value))}\n' for name, value in values.items()))
    report = {'opportunities': [], 'scan_partial': False}
    opportunities = Mock(return_value=report)
    automatic = Mock(return_value={'state': 'idle'})
    monkeypatch.setattr(feeder_trips, 'feeder_opportunities', opportunities)
    monkeypatch.setattr(feeder_automation, 'run_auto_checks', automatic)

    assert feeder_refresh.run() == {'state': 'idle'}
    called_db, called_id, scope = opportunities.call_args.args
    assert Path(called_db.path) == Path(db.path)
    assert called_id == ctx['run_id']
    assert scope['origins'] == ['Liverpool']
    assert scope['watch_routes'] == [('Budapest', 'Kutaisi')]
    store, used_report, supplied_key = automatic.call_args.args
    assert store.path == isolated['state'] / 'feeder_quotes.sqlite3'
    assert used_report is report and supplied_key == key
    assert key not in capsys.readouterr().out


@pytest.mark.parametrize('missing', ['key', 'database', 'catalogue'])
def test_worker_missing_inputs_never_search_or_download(isolated, monkeypatch, missing):
    import feeder_automation
    automatic = Mock()
    monkeypatch.setattr(feeder_automation, 'run_auto_checks', automatic)
    if missing != 'key':
        monkeypatch.setenv('AYCF_SERPAPI_KEY', 'example-test-key')
    if missing == 'catalogue':
        path = isolated['state'] / 'aycf.sqlite3'
        ScanCacheDB(str(path))
    result = feeder_refresh.run()
    assert result['state'] == ('unconfigured' if missing == 'key' else 'waiting')
    automatic.assert_not_called()
    assert not (isolated['cache'] / 'direct-data').exists()


def test_current_catalogue_ignores_newer_unrelated_database_run(isolated):
    db, ctx = saved_scan(isolated)
    db.upsert_pdf_run('unrelated-latest', '2026-09-17T06:00:00', '2026-09-17', '2026-09-20', 99)
    db.mark_pdf_scanned('unrelated-latest')
    assert feeder_refresh.local_scope_context(db)['run_id'] == ctx['run_id']


@pytest.mark.parametrize('change', ['preferences', 'expected_run'])
def test_old_scope_or_scan_hook_cannot_spend_fare_requests(isolated, monkeypatch, change):
    import feeder_automation
    db, ctx = saved_scan(isolated)
    monkeypatch.setenv('AYCF_SERPAPI_KEY', 'example-test-key')
    monkeypatch.setenv('AYCF_TERMUX_DB_PATH', db.path)
    automatic = Mock()
    monkeypatch.setattr(feeder_automation, 'run_auto_checks', automatic)
    expected = ctx['run_id']
    if change == 'preferences':
        (isolated['config'] / 'recommendation_preferences.json').write_text(
            json.dumps({'destinations': ['Kutaisi']}))
    else:
        expected = 'previous-scan'
    assert feeder_refresh.run(expected_run_id=expected)['state'] == 'waiting'
    automatic.assert_not_called()


def test_active_scan_lock_blocks_worker_before_catalogue_or_fare_work(isolated, monkeypatch):
    db, _ = saved_scan(isolated)
    monkeypatch.setenv('AYCF_SERPAPI_KEY', 'example-test-key')
    monkeypatch.setenv('AYCF_TERMUX_DB_PATH', db.path)
    context = Mock(side_effect=AssertionError('Active scans must be left alone'))
    monkeypatch.setattr(feeder_refresh, 'local_scope_context', context)
    with process_lock(isolated['state'] / 'scan.lock') as acquired:
        assert acquired
        assert feeder_refresh.run()['state'] == 'scan_busy'
    context.assert_not_called()


@pytest.mark.parametrize('state', ['complete', 'already_current', 'partial'])
@pytest.mark.parametrize('fare_error', [False, True])
def test_post_scan_hook_preserves_outcome_and_passes_owned_lock(monkeypatch, state, fare_error, capsys):
    result = {'ok': state != 'partial', 'pdf_run_id': 'just-scanned'}
    if state == 'partial':
        result.update(state='partial', reason='One check pending', unknown_checks=1)
    elif state == 'already_current':
        result.update(state='already_current', skipped=True)
    original = dict(result)
    monkeypatch.setattr(automated_morning, 'single_scan_lock', lambda: nullcontext(True))
    monkeypatch.setattr(automated_morning, '_run_once', lambda **kwargs: result)
    for method in ('_snapshot_history_after_scan', '_refresh_stability_after_scan', '_check_watches_after_scan'):
        monkeypatch.setattr(automated_morning, method, lambda: {})
    status = Mock()
    monkeypatch.setattr(automated_morning, 'write_status', status)
    refresh = Mock(return_value={'state': 'checked'})
    if fare_error:
        refresh.side_effect = RuntimeError('private-provider-exception')
    monkeypatch.setattr(feeder_refresh, 'run', refresh)

    returned = automated_morning.run()
    refresh.assert_called_once_with(scan_lock_owned=True, expected_run_id='just-scanned')
    assert all(returned[name] == value for name, value in original.items())
    assert returned['feeders']['state'] == ('error' if fare_error else 'checked')
    assert status.call_args.args[0] == ('partial' if state == 'partial' else 'complete')
    assert 'private-provider-exception' not in capsys.readouterr().out


@pytest.mark.parametrize('state', ['wizz_authentication_required', 'wizz_service_unavailable',
                                   'request_rejected', 'already_running'])
def test_failed_or_duplicate_scan_does_not_call_post_scan_fare_hook(monkeypatch, state):
    result = {'ok': False, 'state': state, 'pdf_run_id': 'old-run'}
    monkeypatch.setattr(automated_morning, 'single_scan_lock', lambda: nullcontext(True))
    monkeypatch.setattr(automated_morning, '_run_once', lambda **kwargs: result)
    monkeypatch.setattr(automated_morning, 'write_status', Mock())
    refresh = Mock()
    monkeypatch.setattr(feeder_refresh, 'run', refresh)
    assert automated_morning.run() is result
    refresh.assert_not_called()


@pytest.mark.parametrize('supervisor_rc', [0, 23])
def test_existing_gate_runs_fares_with_private_env_after_supervisor(tmp_path, supervisor_rc):
    root = tmp_path / 'custom-checkout'
    (root / 'termux').mkdir(parents=True)
    shutil.copyfile(ROOT / 'termux/morning-gate.sh', root / 'termux/morning-gate.sh')
    config = tmp_path / 'config'
    config.mkdir()
    (config / 'env').write_text("export AYCF_SERPAPI_KEY='example-not-a-real-key'\n")
    binary = tmp_path / 'bin'
    binary.mkdir()
    stub = binary / 'python'
    stub.write_text('#!/bin/sh\n'
                    'printf "%s|%s|%s\\n" "$PWD" "$1" "${AYCF_SERPAPI_KEY:+configured}" >> "$AYCF_TEST_CAPTURE"\n'
                    'if [ "$1" = termux/supervisor.py ]; then exit "$AYCF_TEST_SUPERVISOR_RC"; fi\n')
    stub.chmod(0o700)
    wake = binary / 'termux-wake-lock'
    wake.write_text('#!/bin/sh\nexit 0\n')
    wake.chmod(0o700)
    capture = tmp_path / 'calls'
    env = {name: value for name, value in os.environ.items() if not name.startswith('AYCF_')}
    env.update(PATH=str(binary) + ':' + os.environ['PATH'], AYCF_CONFIG_DIR=str(config),
               AYCF_STATE_DIR=str(tmp_path / 'state'), AYCF_TEST_CAPTURE=str(capture),
               AYCF_TEST_SUPERVISOR_RC=str(supervisor_rc))
    process = subprocess.run(['bash', str(root / 'termux/morning-gate.sh')], env=env, capture_output=True, text=True)
    assert process.returncode == supervisor_rc
    assert capture.read_text().splitlines() == [
        f'{root}|termux/supervisor.py|configured', f'{root}|termux/feeder_refresh.py|configured']
    assert 'example-not-a-real-key' not in process.stdout + process.stderr


@pytest.mark.parametrize('explicit', [False, True])
def test_private_env_last_saved_key_wins_without_replacing_explicit_value(isolated, monkeypatch, explicit):
    old, updated, inherited = 'example-previous', 'example-updated', 'example-inherited'
    env_file = isolated['config'] / 'env'
    env_file.write_text(f"export AYCF_SERPAPI_KEY='{old}'\nexport AYCF_SERPAPI_KEY='{updated}'\n")
    if explicit:
        monkeypatch.setenv('AYCF_SERPAPI_KEY', inherited)
    load_termux_env()
    assert os.environ['AYCF_SERPAPI_KEY'] == (inherited if explicit else updated)
