import json
import zipfile
from contextlib import closing

import pytest

from android.transfer import export_backup, restore_backup
from cache_db import ScanCacheDB
from feeder_store import FeederStore
from route_history import _connect
from watch_service import add_watch, list_watches


def test_transfer_preserves_planner_data_only(tmp_path, monkeypatch):
    state = tmp_path / 'state'
    config = tmp_path / 'config'
    config.mkdir()
    state.mkdir()
    monkeypatch.setenv('AYCF_STATE_DIR', str(state))
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(config))
    monkeypatch.setenv('AYCF_TERMUX_DB_PATH', str(state / 'aycf.sqlite3'))
    monkeypatch.setenv('AYCF_CACHE_DIR', str(state / 'cache'))
    ScanCacheDB(str(state / 'aycf.sqlite3'))
    add_watch('London Luton', 'Tirana', any_date=True, path=str(state / 'aycf_watches.sqlite3'))
    with closing(_connect(str(state / 'aycf_route_history.sqlite3'))):
        pass
    FeederStore(state / 'feeder_quotes.sqlite3')
    (config / 'scan_scope.json').write_text(json.dumps({'origins': ['London Luton']}))
    (config / 'env').write_text('private')
    (state / 'wizz_session.enc').write_text('private')
    archive = export_backup(tmp_path / 'transfer.zip')
    with zipfile.ZipFile(archive) as check:
        assert set(check.namelist()) == {'state/aycf.sqlite3', 'state/aycf_watches.sqlite3',
                'state/aycf_route_history.sqlite3', 'state/feeder_quotes.sqlite3',
                'config/scan_scope.json', 'manifest.json'}
    restore_backup(archive, tmp_path / 'planner')
    assert list_watches(str(tmp_path / 'planner/state/aycf_watches.sqlite3'))[0]['destination'] == 'Tirana'
    assert json.loads((tmp_path / 'planner/config/scan_scope.json').read_text())['origins'] == ['London Luton']
    with pytest.raises(ValueError, match='already exists'):
        restore_backup(archive, tmp_path / 'planner')
    with zipfile.ZipFile(archive, 'a') as invalid:
        invalid.writestr('../outside', 'bad')
    with pytest.raises(ValueError):
        restore_backup(archive, tmp_path / 'broken')
    assert not (tmp_path / 'broken').exists()
    assert not (tmp_path / 'outside').exists()
