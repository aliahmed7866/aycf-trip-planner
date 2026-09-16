"""Small, persistent fare-check batches for useful Manchester connections.

The caller supplies opportunities from the current Wizz scan. Status reads never
contact SerpApi. A process lock covers selection and checking; the store reserves
both manual and automatic quotas atomically before any provider request.
"""
from __future__ import annotations

import fcntl
import json
from datetime import datetime, timezone

from feeder_store import (
    AUTOMATIC_CACHE_SECONDS, AUTOMATIC_EMPTY_SECONDS, AUTOMATIC_LIMIT_24H,
    LIMIT_24H, LIMIT_31D, _utc_now,
)

BATCH_INTERVAL_SECONDS = 6 * 60 * 60
BATCH_LIMIT = 2


def rank_feeder_targets(report, history_path=None, now=None):
    # Lazy loading also keeps read-only status independent of ranking/history.
    from feeder_ranking import rank_feeder_targets as rank
    return rank(report, history_path=history_path, now=now)


def _iso(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat() if timestamp is not None else None


def _settings(store):
    with store._connection() as connection:
        return dict(connection.execute('SELECT * FROM automation WHERE id = 1').fetchone())


def _save(store, **values):
    fields = {'last_checked', 'last_batch_epoch', 'next_target_epoch', 'state', 'message', 'checked_count', 'selected'}
    if not values or not set(values) <= fields:
        raise ValueError('Invalid automation status update.')
    with store._connection() as connection:
        connection.execute('UPDATE automation SET ' + ', '.join(f'{key} = ?' for key in values) + ' WHERE id = 1',
                           tuple(values.values()))


def _quota_due(store, current):
    """When all exhausted rolling allowances can admit one more request."""
    timestamp = current.timestamp()
    deadlines = []
    with store._connection() as connection:
        for seconds, limit, automatic in ((86400, LIMIT_24H, False),
                                          (86400, AUTOMATIC_LIMIT_24H, True),
                                          (31 * 86400, LIMIT_31D, False)):
            row = connection.execute(
                'SELECT reserved_at FROM requests WHERE reserved_at > ?' +
                (' AND automatic = 1' if automatic else '') +
                ' ORDER BY reserved_at DESC LIMIT 1 OFFSET ?', (timestamp - seconds, limit - 1),
            ).fetchone()
            if row:
                deadlines.append(row['reserved_at'] + seconds)
    return max(deadlines, default=0)


def automation_status(store, now=None):
    """Read persisted settings/heartbeat and shared quotas without network work."""
    current = _utc_now(now)
    settings = _settings(store)
    due = []
    if settings['last_batch_epoch'] is not None:
        due.append(settings['last_batch_epoch'] + BATCH_INTERVAL_SECONDS)
    if settings['next_target_epoch'] is not None:
        due.append(settings['next_target_epoch'])
    quota_due = _quota_due(store, current)
    if quota_due:
        due.append(quota_due)
    next_epoch = max(due, default=current.timestamp())
    enabled = bool(settings['enabled'])
    return {
        'enabled': enabled,
        'last_checked': settings['last_checked'],
        'last_batch_at': _iso(settings['last_batch_epoch']),
        'next_due': _iso(max(current.timestamp(), next_epoch)) if enabled and settings['state'] != 'unconfigured' else None,
        'state': settings['state'] if enabled else 'paused',
        'message': settings['message'] if enabled else 'Automatic fare checks are paused. Manual fare checks remain available.',
        'checked_count': settings['checked_count'],
        'selected': json.loads(settings['selected']),
        'batch_limit': BATCH_LIMIT,
        'interval_hours': BATCH_INTERVAL_SECONDS // 3600,
        **store.usage(now=current),
    }


def set_automation_enabled(store, enabled):
    """Persist the switch without resetting any reservation or batch cooldown."""
    with store._connection() as connection:
        connection.execute('UPDATE automation SET enabled = ? WHERE id = 1', (int(bool(enabled)),))


def _eligible_after(status, timestamp):
    if status['state'] == 'running' and (status.get('lease_until') or 0) > timestamp:
        return status['lease_until']
    checked = status.get('checked_epoch')
    if checked is not None and status['state'] in {'complete', 'error'}:
        cooldown = (AUTOMATIC_EMPTY_SECONDS if status['state'] == 'complete' and not status.get('offers')
                    else AUTOMATIC_CACHE_SECONDS)
        return checked + cooldown
    return timestamp


def run_auto_checks(store, report, api_key, history_path=None, now=None, fetcher=None):
    """Check at most two eligible ranked hub/dates, then wait at least six hours.

    Missing keys, empty reports and cached candidates do not consume allowance or
    start the batch clock. Partial reports may contribute their observed positive
    opportunities; unknown flights are never assumed to be available.
    """
    current = _utc_now(now)
    timestamp = current.timestamp()
    lock_path = store.path.with_name(store.path.name + '.automation.lock')
    with lock_path.open('a') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {**automation_status(store, now=current), 'state': 'running',
                    'message': 'Another automatic fare-check batch is already running.'}
        try:
            return _run_locked(store, report, api_key, history_path, current, timestamp, fetcher)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _run_locked(store, report, api_key, history_path, current, timestamp, fetcher):
    settings = _settings(store)
    _save(store, last_checked=current.isoformat())
    if not settings['enabled']:
        return automation_status(store, now=current)
    if not api_key or not str(api_key).strip():
        _save(store, state='unconfigured', message='Add AYCF_SERPAPI_KEY to enable automatic fare checks.')
        return automation_status(store, now=current)
    if settings['last_batch_epoch'] is not None and timestamp < settings['last_batch_epoch'] + BATCH_INTERVAL_SECONDS:
        # Keep the last outcome and targets visible while the batch cools down.
        if settings['state'] == 'running':
            _save(store, state='waiting', message='A previous fare-check batch stopped; its cooldown is still in effect.')
        return automation_status(store, now=current)
    if _quota_due(store, current) > timestamp:
        _save(store, state='blocked', message='Automatic fare checks are waiting for the shared free-data allowance to reset.')
        return automation_status(store, now=current)
    if not isinstance(report, dict) or not report.get('opportunities'):
        _save(store, state='waiting', message='Waiting for current AYCF flights to the selected special destinations.',
              checked_count=0, selected='[]', next_target_epoch=None)
        return automation_status(store, now=current)
    try:
        ranked = rank_feeder_targets(report, history_path=history_path, now=current)
    except Exception:
        _save(store, state='error', message='Connection priorities could not be read. No fare requests were sent.',
              checked_count=0, selected='[]', next_target_epoch=None)
        return automation_status(store, now=current)
    eligible, waiting, seen = [], [], set()
    for target in ranked:
        hub, day = str(target['hub']).upper(), str(target.get('date') or target.get('travel_date'))
        if (hub, day) in seen:
            continue
        seen.add((hub, day))
        search = store.search_status(hub, day)
        ready_at = _eligible_after(search, timestamp)
        if ready_at > timestamp:
            waiting.append(ready_at)
            continue
        eligible.append({'hub': hub, 'date': day, 'reason': str(target.get('reason') or 'Useful onward AYCF connections.'),
                         'discovery': search.get('checked_epoch') is None})
    if not eligible:
        _save(store, state='waiting', message='Useful hub dates are already checked or waiting for their next eligible check.',
              checked_count=0, selected='[]', next_target_epoch=min(waiting) if waiting else None)
        return automation_status(store, now=current)
    # Keep one top-ranked refresh, and reserve the other slot for an unchecked
    # opportunity when possible. Otherwise the same two positive fares could
    # consume every daily check and leave useful destinations unexplored.
    selected = [eligible[0]]
    remainder = eligible[1:]
    if remainder:
        alternatives = [item for item in remainder if item['discovery']] or remainder
        selected.append(next((item for item in alternatives if item['hub'] != selected[0]['hub']), alternatives[0]))
    selected = [{key: value for key, value in item.items() if key != 'discovery'} for item in selected]
    _save(store, state='running', message='Checking fares for useful Manchester connections.',
          checked_count=0, selected=json.dumps(selected), next_target_epoch=None)
    checked = 0
    attempted = []
    final_state, final_message = 'complete', 'Automatic fare checks completed.'
    for target in selected:
        # A pause takes effect between provider requests without clearing the
        # cooldown of any request already sent.
        if not _settings(store)['enabled']:
            break
        result = store.refresh(target['hub'], target['date'], api_key, fetcher=fetcher,
                               now=current, automatic=True)
        attempted.append({**target, 'state': result['state'], 'offers': result.get('offers', 0)})
        if not result.get('cached') and result['state'] in {'complete', 'error'}:
            checked += 1
        if result['state'] in {'error', 'blocked', 'unconfigured'}:
            final_state = result['state']
            final_message = ('The fare provider could not complete the check; automatic requests will wait before retrying.'
                             if result['state'] == 'error' else result['message'])
            break
    _save(store, state=final_state, message=final_message, checked_count=checked, selected=json.dumps(attempted))
    return automation_status(store, now=current)
