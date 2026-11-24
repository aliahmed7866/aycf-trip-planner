"""Export AYCF planner data from Termux, or restore it into a new Android install."""
import argparse
from contextlib import closing
import csv
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import zipfile

DBS = {
    'aycf.sqlite3': 'pdf_runs',
    'aycf_watches.sqlite3': 'flight_watches',
    'aycf_route_history.sqlite3': 'snapshots',
    'feeder_quotes.sqlite3': 'offers',
}
CONFIG = ('scan_scope.json', 'recommendation_preferences.json', 'airport_routes.json', 'trip_preferences.json')
CATALOG = 'cache/direct-data/catalogue.csv'
LIMIT = 2 * 1024**3


def export_backup(target):
    state = Path(os.getenv('AYCF_STATE_DIR', Path.home() / '.local/share/aycf'))
    config = Path(os.getenv('AYCF_CONFIG_DIR', Path.home() / '.config/aycf'))
    cache = Path(os.getenv('AYCF_CACHE_DIR', state / 'cache'))
    target = Path(target).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target.parent) as temp:
        stage = Path(temp)
        files = {}
        for name in DBS:
            source = Path(os.getenv('AYCF_TERMUX_DB_PATH', state / name)) if name == 'aycf.sqlite3' else state / name
            if source.is_file():
                copy = stage / name
                with closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True)) as src, closing(sqlite3.connect(copy)) as dst:
                    src.backup(dst)
                files['state/' + name] = copy
        for name in CONFIG:
            if (config / name).is_file():
                files['config/' + name] = config / name
        snapshots = list((cache / 'direct-data').glob('*.csv'))
        if snapshots:
            files[CATALOG] = max(snapshots, key=lambda path: path.stat().st_mtime)
        if 'state/aycf.sqlite3' not in files:
            raise ValueError('No AYCF flight database found. Run this from your AYCF Termux installation.')
        with zipfile.ZipFile(stage / 'transfer.zip', 'w', zipfile.ZIP_DEFLATED) as archive:
            hashes = {}
            for name, source in files.items():
                digest = hashlib.sha256()
                with source.open('rb') as src, archive.open(name, 'w') as dst:
                    for block in iter(lambda: src.read(1024 * 1024), b''):
                        dst.write(block)
                        digest.update(block)
                hashes[name] = digest.hexdigest()
            archive.writestr('manifest.json', json.dumps({'format': 1, 'sha256': hashes}))
        os.replace(stage / 'transfer.zip', target)
    return target


def restore_backup(source, target):
    """Publish a complete validated directory. Only callable before app setup."""
    target = Path(target)
    if target.exists():
        raise ValueError('AYCF data already exists on this installation.')
    allowed = {'state/' + name for name in DBS} | {'config/' + name for name in CONFIG} | {CATALOG}
    with tempfile.TemporaryDirectory(dir=target.parent) as temp, zipfile.ZipFile(source) as archive:
        stage = Path(temp) / 'planner'
        names = archive.namelist()
        if (len(names) != len(set(names)) or 'manifest.json' not in names
                or 'state/aycf.sqlite3' not in names or set(names) - allowed - {'manifest.json'}
                or sum(item.file_size for item in archive.infolist()) > LIMIT
                or archive.getinfo('manifest.json').file_size > 16384):
            raise ValueError('Choose a ZIP created by the AYCF transfer script.')
        manifest = json.loads(archive.read('manifest.json'))
        hashes = manifest.get('sha256', {})
        if manifest.get('format') != 1 or set(hashes) != set(names) - {'manifest.json'}:
            raise ValueError('Unsupported or incomplete AYCF transfer.')
        for name in hashes:
            out = stage / name
            out.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            with archive.open(name) as src, out.open('xb') as dst:
                for block in iter(lambda: src.read(1024 * 1024), b''):
                    dst.write(block)
                    digest.update(block)
            if digest.hexdigest() != hashes[name]:
                raise ValueError('The transfer failed its integrity check.')
            if name.startswith('state/'):
                with closing(sqlite3.connect(out.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                    db.execute('PRAGMA trusted_schema=OFF')
                    if db.execute('PRAGMA quick_check').fetchone() != ('ok',):
                        raise ValueError('Damaged database in transfer.')
                    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (DBS[out.name],)).fetchone():
                        raise ValueError('Unexpected database format for ' + out.name)
            elif name.startswith('config/'):
                if out.stat().st_size > 16 * 1024**2 or not isinstance(json.loads(out.read_text()), dict):
                    raise ValueError('Invalid planner preferences in transfer.')
            else:
                with out.open(encoding='utf-8-sig') as handle:
                    if not {'departure_from', 'departure_to', 'data_generated'}.issubset(next(csv.reader(handle), [])):
                        raise ValueError('Invalid route catalogue.')
        stage.rename(target)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Copy AYCF flights, history, watches and preferences to Android.')
    parser.add_argument('output', nargs='?', default='~/storage/downloads/AYCF-transfer.zip')
    print(export_backup(parser.parse_args().output))
