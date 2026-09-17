"""One identity for a scan, including its in-process authentication recovery."""
import builtins
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
import time
import uuid

from termux.diagnostic_downloads import installed_revision

WORKER_REVISION = installed_revision(Path(__file__).resolve().parent)
_context = {}


def context():
    return dict(_context)


def observed_scan(function):
    @wraps(function)
    def run(*args, **kwargs):
        global _context
        previous = _context
        _context = {'run_id': uuid.uuid4().hex[:12], 'worker_revision': WORKER_REVISION,
                    'started_at': int(time.time())}
        try:
            log('Scan started.')
            return function(*args, **kwargs)
        finally:
            log('Scan worker finished.')
            _context = previous
    return run


def log(*values, **kwargs):
    stamp = datetime.now(timezone.utc).isoformat(timespec='seconds')
    identity = f" run={_context['run_id']} rev={_context['worker_revision']}" if _context else ''
    builtins.print(f'[{stamp}{identity}]', *values, **kwargs)
