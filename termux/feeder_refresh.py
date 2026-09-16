"""Bounded automatic feeder checks from the current local AYCF cache."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def local_scope_context(db):
    """Reproduce the planner's current identity without downloading a catalogue."""
    from recommendation_preferences import scan_scope_with_preferences
    from scan_scope import load_scope, scan_plan, scan_window, scan_run_id
    from scanner import CurrentRouteGraph

    directory = Path(os.environ.get('AYCF_CACHE_DIR', str(ROOT / 'cache'))) / 'direct-data'
    if not directory.is_dir() or not any(directory.glob('*.csv')):
        return None
    frame = CurrentRouteGraph(str(directory)).latest_frame()
    if frame.empty or not {'departure_from', 'departure_to', 'data_generated'}.issubset(frame.columns):
        return None
    scope = scan_scope_with_preferences(load_scope())
    pairs = sorted(set(zip(frame['departure_from'], frame['departure_to'])))
    plan = scan_plan(pairs, scope, days=scan_window(frame)['days'])
    generated = str(frame['data_generated'].iloc[0]).strip()
    run_id = scan_run_id(generated, scope, plan['routes'])
    if not run_id or not db.get_pdf_run(run_id):
        return None
    return {'run_id': run_id, 'scope': scope}


def run(*, scan_lock_owned=False, expected_run_id=None):
    # The standalone script and Python post-scan hook both use the same private
    # environment as the shell launchers. Do this before importing state paths.
    from termux.env_loader import load_termux_env
    load_termux_env()
    key = os.environ.get('AYCF_SERPAPI_KEY', '').strip()
    if not key:
        return {'state': 'unconfigured', 'message': 'Automatic fare checks need AYCF_SERPAPI_KEY.'}
    state = Path(os.environ.get('AYCF_STATE_DIR', str(Path.home() / '.local/share/aycf')))
    path = Path(os.environ.get('AYCF_TERMUX_DB_PATH', str(state / 'aycf.sqlite3'))).expanduser()
    if not path.is_file():
        return {'state': 'waiting', 'message': 'Waiting for saved AYCF availability.'}
    from cache_db import ScanCacheDB
    from feeder_automation import run_auto_checks
    from feeder_store import FeederStore
    from feeder_trips import feeder_opportunities
    from termux.run_state import process_lock

    def check():
        db = ScanCacheDB(str(path))
        ctx = local_scope_context(db)
        if not ctx or (expected_run_id and ctx['run_id'] != expected_run_id):
            return {'state': 'waiting', 'message': 'Waiting for a scan matching the current route settings.'}
        report = feeder_opportunities(db, ctx['run_id'], ctx['scope'], limit=None)
        store = FeederStore(path.with_name('feeder_quotes.sqlite3'))
        return run_auto_checks(store, report, key)

    try:
        if scan_lock_owned:
            return check()
        # Hold the lock through this short batch so a scan cannot start midway
        # through the snapshot. The controller separately locks fare batches.
        with process_lock(state / 'scan.lock') as acquired:
            if not acquired:
                return {'state': 'scan_busy', 'message': 'Fare checks deferred while the AYCF scan is running.'}
            return check()
    except Exception:
        # No provider URLs, API keys or raw exceptions in scheduler logs.
        return {'state': 'error', 'message': 'Automatic fare checks could not run; saved fares are unchanged.'}


if __name__ == '__main__':
    print(json.dumps(run(), sort_keys=True))
