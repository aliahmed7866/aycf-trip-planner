"""Back up pending scan state, rebuild current checks and start one fresh scan."""
from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time

from cache_db import ScanCacheDB
from termux import automated_morning, run_state, supervisor


def _busy(message):
    print(f'[AYCF] {message}', flush=True)
    return {'ok': True, 'state': 'already_running', 'scan_performed': False, 'message': message}


def _reset_pending(db):
    """Caller owns supervisor, scan-process and database scan locks."""
    backup_root = run_state.STATE_DIR / 'scan-reset-backups'
    backup_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S-')
    backup = Path(tempfile.mkdtemp(prefix=stamp, dir=backup_root))
    destination = backup / 'aycf.sqlite3'
    with closing(sqlite3.connect(db.path)) as source, closing(sqlite3.connect(destination)) as target:
        source.backup(target)
    os.chmod(destination, 0o600)
    for name, value in [('scan-status.json', run_state.read_status()),
                        ('supervisor-status.json', supervisor._load(supervisor.SUPERVISOR_FILE))]:
        path = backup / name
        path.write_text(json.dumps(value, indent=2), encoding='utf-8')
        os.chmod(path, 0o600)

    today = date.today().isoformat()
    with db.connect() as conn:
        # Keep positive rows searchable; only their completion markers change.
        checks = conn.execute('UPDATE route_checks SET complete=0 WHERE travel_date>=?', (today,)).rowcount
        runs = conn.execute('''UPDATE pdf_runs SET scanned_at=NULL
            WHERE departure_end>=? OR generated_at>=? OR run_id IN
            (SELECT pdf_run_id FROM route_checks WHERE travel_date>=?)''', (today, today, today)).rowcount
        failures = conn.execute("DELETE FROM scan_runs WHERE status IN ('failed','partial','interrupted','queued','pending')").rowcount
    sup = supervisor._load(supervisor.SUPERVISOR_FILE)
    for key in ('pending_since', 'last_scan_failure', 'last_scan_outcome', 'last_scan_attempt_at',
                'last_scan_finished_at', 'last_scan_rc', 'fresh_pending', 'cooldown_until', 'retry_at', 'effective_request_interval'):
        sup.pop(key, None)
    sup.update(scan_pending=False, state='idle', message='Old scan retry state cleared; a fresh scan is starting.')
    supervisor._save(sup)
    run_state.write_status('running', 'Starting a fresh scan of the selected scope.',
                           started_at=int(time.time()), fresh=True, reset_backup=str(backup))
    print(f'[AYCF] Fresh scan backup: {backup}', flush=True)
    print(f'[AYCF] Reset {checks} current/future check markers across {runs} catalogues; '
          f'cleared {failures} failed/pending scan records. Saved flights, route history, '
          'scope settings, login and Wizz pacing are preserved.', flush=True)
    return {'backup': str(backup), 'checks_reset': checks, 'catalogues_reset': runs,
            'failed_records_cleared': failures}


def _queue_after_cooldown(result, reset):
    """Persist scheduling intent without changing the shared Wizz deadline."""
    message = (f"Pending work cleared. Fresh scan queued until {result['retry_at']}; "
               "it will start on the next supervisor wake after that time. "
               "No Wizz requests were sent by this reset.")
    details = {key: result[key] for key in ('cooldown_until', 'retry_at', 'effective_request_interval')}
    run_state.write_status('rate_limited', message, scan_performed=False, http_status=429,
                           resume_scan=True, fresh_pending=True, reset_backup=reset['backup'], **details)
    sup = supervisor._load(supervisor.SUPERVISOR_FILE)
    sup.update(state='rate_limited', message=message, scan_pending=True,
               pending_since=int(time.time()), **details)
    supervisor._save(sup)
    print(f'[AYCF] {message}', flush=True)
    return {**result, 'ok': True, 'queued': True, 'message': message}


def run():
    # Same acquisition order as the supervisor. Never kill or reset a live scan.
    with run_state.process_lock(supervisor.STATE_DIR / 'supervisor.lock') as scheduler_free:
        if not scheduler_free:
            return _busy('Supervisor work is active. Nothing was reset; try after it finishes.')
        with run_state.single_scan_lock() as acquired:
            if not acquired:
                return _busy('A scan is already running. Nothing was reset.')
            db = ScanCacheDB()
            with db.scan_lock() as db_free:
                if not db_free:
                    return _busy('Another scanner owns the database. Nothing was reset.')
                return _run_preflight_reset(db)


def _run_preflight_reset(db):
    """Caller owns scan/process locks; scheduler may own its lock in the parent."""
    try:
        reset = {}
        def reset_once():
            if not reset:
                reset.update(_reset_pending(db))
        from wizz_rate_limit import rate_limit_status
        blocked = rate_limit_status()['blocked']
        if blocked:
            # Local clearing remains available during a cooldown.
            reset_once()
        result = automated_morning._run_with_lock(force=False, locked_db=db,
            **({} if blocked else {'before_scan': reset_once}))
        if isinstance(result, dict) and result.get('state') == 'rate_limited' and (not reset or not result.get('scan_performed')):
            reset_once()
            result = _queue_after_cooldown({**result, 'scan_performed': False}, reset)
        if isinstance(result, dict):
            if not reset and result.get('state') == 'wizz_service_unavailable':
                # Preserve the explicit fresh intent across the supervisor's
                # later retry, without clearing markers during the outage.
                saved = run_state.read_status()
                state = saved.pop('state', 'service_unavailable')
                message = saved.pop('message', '') + ' Fresh reset deferred until preflight succeeds.'
                run_state.write_status(state, message, **saved, fresh_reset_pending=True)
                result = {**result, 'message': message, 'fresh_reset_pending': True}
            return {**result, **({'fresh_reset': reset} if reset else {'reset_performed': False})}
        return result
    except Exception as exc:
        run_state.write_status('failed', str(exc), error_type=type(exc).__name__)
        raise
