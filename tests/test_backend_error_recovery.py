import json
from datetime import date, datetime, timezone
from unittest.mock import Mock

import pytest
import requests

from morning_scan import CapturedRequestWizzClient, _scan_days
from parallel_fetch import fetch_group
from scan_service_errors import is_service_error
from scanner import WizzIntegrationChanged
from termux import supervisor
from tests.test_supervisor_recovery import cycle, saved


PAYLOAD = {'code': 'PASS-0000', 'key': 'wallet.error.generic',
           'message': 'cyf.flights.FlightSearchException: Wizzair backend failure'}
LEGACY = ('Wizz rejected AYCF search Sharm el-Sheikh (SSH) -> Milan Malpensa (MXP) '
          'on 2026-10-27 with HTTP 400: '
          '{"code":"PASS-0000","key":"wallet.error.generic",'
          '"message":"cyf.flights.FlightSearchException: Wizza')


def response(payload, layers=1):
    result = requests.Response()
    result.status_code = 400
    for _ in range(layers):
        payload = json.dumps(payload)
    result._content = payload.encode()
    return result


@pytest.mark.parametrize('layers', [1, 2, 3])
def test_backend_error_survives_decoder_and_other_airports_continue(monkeypatch, layers):
    client = CapturedRequestWizzClient({'cookies': []})
    failure = requests.HTTPError(response=response(PAYLOAD, layers))
    monkeypatch.setattr(client, '_request', Mock(side_effect=failure))
    def check(a, b, day):
        if a == 'SSH':
            return client._send_and_decode({}, 'AYCF search SSH -> MXP')
        return []
    monkeypatch.setattr(client, 'check', check)
    flights, checked, unknown = fetch_group(client, [('SSH', 'MXP'), ('LTN', 'BUD')], date.today())
    assert flights == []
    assert checked == [('LTN', 'BUD')]
    assert len(unknown) == 1 and 'HTTP 400 for SSH -> MXP' in unknown[0]


@pytest.mark.parametrize('payload', [
    {'code': 'OTHER', **{k: v for k, v in PAYLOAD.items() if k != 'code'}},
    {**PAYLOAD, 'key': 'other'}, {**PAYLOAD, 'message': 'missing origin'},
    'cyf.flights.FlightSearchException: arbitrary text', [],
])
def test_unrecognised_400_remains_blocked(monkeypatch, payload):
    failure = requests.HTTPError(response=response(payload, 2))
    assert not is_service_error(failure)
    client = CapturedRequestWizzClient({'cookies': []})
    monkeypatch.setattr(client, '_request', Mock(side_effect=failure))
    with pytest.raises(WizzIntegrationChanged):
        client._send_and_decode({}, 'test')


def test_supervisor_recovers_saved_backend_block_once(cycle):
    _, status, calls, _ = cycle
    status.update(state='request_repair_required', message=LEGACY)
    supervisor.main()
    assert len(calls) == 1 and calls[0][-1] == 'morning'
    assert not saved()['scan_pending']
    supervisor.main()
    assert len(calls) == 1


@pytest.mark.parametrize('state,message', [
    ('request_rejected', LEGACY),
    ('request_repair_required', 'Wizz rejected a malformed request with HTTP 400'),
    ('request_repair_required', LEGACY.replace('PASS-0000', 'OTHER')),
])
def test_supervisor_preserves_other_manual_blocks(cycle, state, message):
    _, status, calls, _ = cycle
    status.update(state=state, message=message)
    supervisor.main()
    assert not calls
    assert saved()['state'] == state


def test_scan_days_excludes_yesterday_but_keeps_today_and_pdf_end():
    days = list(_scan_days(datetime(2026, 10, 26), datetime(2026, 10, 29), today=date(2026, 10, 27)))
    assert days == [date(2026, 10, day) for day in (27, 28, 29)]
    assert list(_scan_days(datetime(2026, 10, 25), datetime(2026, 10, 26), today=date(2026, 10, 27))) == []


def test_scan_days_uses_current_utc_day(monkeypatch):
    import morning_scan
    clock = Mock()
    clock.now.return_value = datetime(2026, 10, 27, 0, 5, tzinfo=timezone.utc)
    monkeypatch.setattr(morning_scan, 'datetime', clock)
    assert list(_scan_days(datetime(2026, 10, 26), datetime(2026, 10, 27))) == [date(2026, 10, 27)]
    clock.now.assert_called_once_with(timezone.utc)


@pytest.mark.parametrize('module_name', ['morning_scan', 'tiered_morning'])
def test_expired_pdf_does_not_authenticate_or_mark_complete(monkeypatch, module_name):
    import importlib
    import pandas as pd
    worker = importlib.import_module(module_name)
    expired = datetime(2000, 1, 1)
    frame = pd.DataFrame([('London Luton', 'Budapest')], columns=['departure_from', 'departure_to'])
    monkeypatch.setattr(worker, 'refresh_direct_snapshot', lambda *a: ('pdf', frame, expired, expired, expired))
    monkeypatch.setattr(worker, '_mirror_for_web', lambda *a: None)
    vault = Mock(side_effect=AssertionError('Expired dates must not reach authentication'))
    monkeypatch.setattr(worker, 'SessionVault', vault)
    db = Mock()
    result = worker._run_locked(db)
    assert result['state'] == 'stale_pdf' and result['ok'] is False
    assert not result['scan_performed']
    db.upsert_pdf_run.assert_not_called()
