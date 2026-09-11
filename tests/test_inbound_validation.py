from datetime import date, datetime
from unittest.mock import patch

import pytest
import requests

from morning_scan import CapturedRequestWizzClient
from scanner import WizzIntegrationChanged, WizzAvailabilityUnknown, WizzSessionExpired, TTLCache, _parse_dt
from scan_scope import scan_plan, scan_jobs
from inbound_coverage import inbound_coverage
from cache_db import ScanCacheDB
from scanner import Flight


def client():
    obj = CapturedRequestWizzClient({'cookies': []}, cache=TTLCache())
    obj.dynamic_url = 'https://multipass.wizzair.com/example'
    return obj


def test_inbound_datetime_fields_and_request_direction():
    obj = client()
    obj.captured_request_template = {'origin': 'LTN', 'destination': 'BUD', 'departure': '2020-01-01', 'flightType': 'OW'}
    data = {'flightsOutbound': [{'flightCode': 'W123', 'departureDateTime': '2026-09-13T13:00', 'arrivalDateTime': '2026-09-13T15:00'}]}
    with patch.object(obj, '_send_and_decode', return_value=data) as send:
        flights = obj.check('Budapest', 'London Luton', date(2026, 9, 13))
    assert send.call_args.args[0]['origin'] == 'BUD'
    assert send.call_args.args[0]['destination'] == 'LTN'
    assert send.call_args.args[0]['departure'] == '2026-09-13'
    assert flights[0].origin == 'Budapest' and flights[0].destination == 'London Luton'
    assert flights[0].departure == datetime(2026, 9, 13, 13)


@pytest.mark.parametrize('payload', [{}, {'error': 'unavailable service'}, {'flightsOutbound': [{'flightCode': 'W1'}]},
                                     {'flights': [{'flightCode': 'W1', 'departure': 'bad', 'arrival': '10:00'}]}, {'flights': ['invalid']}])
def test_malformed_responses_never_cached_as_no_seats(payload):
    obj = client()
    with patch.object(obj, '_send_and_decode', return_value=payload):
        with pytest.raises(WizzIntegrationChanged):
            obj.check('Budapest', 'London Luton', date(2026, 9, 13))
    assert obj.cache.get('budapest|london luton|2026-09-13') is None


def test_known_empty_collection_can_be_cached():
    obj = client()
    with patch.object(obj, '_send_and_decode', return_value={'data': {'flightsOutbound': []}}):
        assert obj.check('Budapest', 'London Luton', date(2026, 9, 13)) == []
    assert obj.cache.get('budapest|london luton|2026-09-13') == []


def test_wallet_redirect_does_not_prove_no_seats():
    obj = client()
    response = requests.Response()
    response.status_code = 302
    response.headers['Location'] = 'https://multipass.wizzair.com/en/w6/subscriptions/spa/private-page/wallets'
    with patch.object(obj, '_request', return_value=response), patch.object(obj, 'bootstrap') as bootstrap:
        with pytest.raises(WizzAvailabilityUnknown, match='unknown'):
            obj.check('Budapest', 'London Luton', date(2026, 9, 13))
    assert obj.cache.get('budapest|london luton|2026-09-13') is None


def test_time_only_meridiem_anchored_to_requested_day():
    assert _parse_dt('2026-09-14', '8:30 PM') == datetime(2026, 9, 14, 20, 30)


def test_inbound_before_outbound_of_equal_priority_each_day():
    scope = {'origins': ['Liverpool'], 'destination_mode': 'all', 'connection_hubs': []}
    plan = scan_plan([('Liverpool', 'Rome')], scope, days=2)
    jobs = scan_jobs(plan, scope, [date(2026, 9, 12), date(2026, 9, 13)])
    assert [(j[1], j[2], j[3].day) for j in jobs] == [('Rome', 'Liverpool', 12), ('Liverpool', 'Rome', 12), ('Rome', 'Liverpool', 13), ('Liverpool', 'Rome', 13)]


def test_inbound_audit_distinguishes_missing_empty_and_positive(tmp_path):
    db = ScanCacheDB(str(tmp_path / 'db.sqlite'))
    db.upsert_pdf_run('run', '2026-09-11', '2026-09-11', '2026-09-14', 2)
    db.replace_route_check('run', 'Rome', 'Liverpool', date(2026, 9, 11), [])
    db.replace_route_check('run', 'Rome', 'Liverpool', date(2026, 9, 12), [Flight('Rome','Liverpool','W1',datetime(2026,9,12,12),datetime(2026,9,12,14),'','')])
    scope = {'origins': ['Liverpool']}
    result = inbound_coverage(db, 'run', [('Rome','Liverpool'),('Liverpool','Rome')], scope, ['Liverpool'], today=date(2026,9,11))
    assert result['expected'] == 4
    assert result['checked'] == 2 and result['positive'] == result['empty'] == 1
    assert result['missing'] == 2


def wallet_response():
    response = requests.Response()
    response.status_code = 302
    response.headers['Location'] = '/en/w6/subscriptions/spa/private-page/wallets'
    return response


def authenticated_wallet():
    response = requests.Response()
    response.status_code = 200
    response.url = 'https://multipass.wizzair.com/en/w6/subscriptions/spa/private-page/wallets'
    response._content = b'window.CVO.flightSearchUrlJson = "https://multipass.wizzair.com/w6/subscriptions/json/availability/new-id";'
    return response


def test_wallet_warms_session_and_retries_rotated_endpoint():
    obj = client()
    response = requests.Response()
    response.status_code = 200
    response._content = b'{"flightsOutbound": []}'
    with patch.object(obj, '_request', side_effect=[wallet_response(), authenticated_wallet(), response]) as send:
        assert obj.check('Budapest', 'London Luton', date(2026, 9, 13)) == []
    assert send.call_args_list[1].args[0] == 'GET'
    assert send.call_args_list[2].args[1].endswith('/new-id')


def test_preflight_verifies_wallet_without_claiming_route_is_empty():
    obj = client()
    obj.captured_request_template = {'origin': 'BUD', 'destination': 'LTN', 'departure': '2026-09-13'}
    with patch.object(obj, '_request', side_effect=[wallet_response(), authenticated_wallet(), wallet_response()]):
        result = obj.preflight()
    assert result['ok'] and 'availability unknown' in result['response']
    assert obj.no_availability_responses == 0
    assert obj.cache.get('budapest|london luton|2026-09-13') is None


def test_wallet_bootstrap_auth_failure_still_requires_repair():
    obj = client()
    with patch.object(obj, '_request', return_value=wallet_response()), patch.object(obj, 'bootstrap', side_effect=WizzSessionExpired('expired')):
        with pytest.raises(WizzSessionExpired):
            obj.check('Budapest', 'London Luton', date(2026, 9, 13))


def test_missing_wallet_endpoint_is_pending_not_fatal_or_authenticated():
    obj = client()
    page = authenticated_wallet()
    page._content = b'<html>Unrecognised wallet shell</html>'
    with patch.object(obj, '_request', side_effect=[wallet_response(), page]):
        with pytest.raises(WizzAvailabilityUnknown, match='inconclusive'):
            obj.check('Budapest', 'London Luton', date(2026, 9, 13))
    assert not obj._wallet_verified
    assert obj.cache.get('budapest|london luton|2026-09-13') is None
    obj.captured_request_template = {'origin': 'BUD', 'destination': 'LTN', 'departure': '2026-09-13'}
    with patch.object(obj, '_request', side_effect=[wallet_response(), page]):
        assert obj.preflight()['ok'] is False


def test_bootstrap_reads_pass_id_and_detects_login_html():
    obj = client()
    page = authenticated_wallet()
    page._content = b'{"pass_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}'
    with patch.object(obj, '_request', return_value=page):
        assert obj.bootstrap()['url'].endswith('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')
    page._content = b'<form><input name="password"></form>'
    with patch.object(obj, '_request', return_value=page):
        with pytest.raises(WizzSessionExpired):
            obj.bootstrap()
