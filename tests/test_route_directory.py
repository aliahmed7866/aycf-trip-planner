import json
import time
from datetime import date
from unittest.mock import patch

import pytest
import requests

from route_directory import parse_directory, save_directory, load_directory, directory_path, MAX_AGE_SECONDS
from scan_scope import route_requests, scan_plan, scan_jobs, scope_fingerprint
from morning_scan import CapturedRequestWizzClient
from scanner import WizzAvailabilityUnknown


def row(a, bs):
    return {'departureStation': {'id': a}, 'arrivalStations': [{'id': b} for b in bs]}


def test_directed_london_directory_narrows_requests_and_estimates():
    routes = parse_directory([row('ATH',['LTN']), row('LTN',['ATH']), row('LGW',['BUD']), row('STN',['BUD'])])
    scope = {'origins':['London Luton','London Gatwick','London Stansted'], '_route_directory':{'routes':routes}}
    assert route_requests('Athens','London',scope) == [('Athens','London Luton')]
    assert route_requests('London','Athens',scope) == [('London Luton','Athens')]
    # Unknown origin is not proof of an unsupported route.
    assert len(route_requests('Krakow','London',scope)) == 3
    plan = scan_plan([('London','Athens')],scope,days=1)
    jobs = scan_jobs(plan,scope,[date(2026,9,11)])
    assert all(len(job[6]) == 1 for job in jobs)
    excluded = dict(scope, excluded_airports=['London Luton'])
    assert route_requests('Athens','London',excluded) == []
    changed = dict(scope, _route_directory={'routes':{'ATH':['LGW']}})
    assert scope_fingerprint(scope) != scope_fingerprint(changed)
    assert scope_fingerprint(scope) == scope_fingerprint(dict(scope,_route_directory={'routes':routes,'captured_at':123}))


def test_directory_does_not_invent_reverse_edges():
    scope = {'origins':['London Luton'], '_route_directory':{'routes':{'ATH':['LTN'], 'LTN':['BUD']}}}
    assert route_requests('Athens','London',scope)
    assert not route_requests('London','Athens',scope)


def test_invalid_or_empty_origin_rows_are_not_used_as_negative_evidence():
    assert parse_directory([row('ATH',['LTN']),row('ATH',[])]) == {}
    assert parse_directory([row('ATH',['LTN']),row('ATH',['bad-id'])]) == {}
    assert parse_directory([row('ATH',['LTN']),row('ATH',['LGW'])]) == {'ATH':['LGW','LTN']}


def test_capture_expiry_and_failed_capture_preserve_previous_file(tmp_path,monkeypatch):
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path))
    assert save_directory([row('ATH',['LTN'])])
    data = load_directory()
    assert data['routes'] == {'ATH':['LTN']}
    previous = directory_path().read_text()
    assert not save_directory(None)
    assert directory_path().read_text() == previous
    assert not load_directory(now=data['captured_at']+MAX_AGE_SECONDS+1)
    directory_path().write_text('{')
    assert load_directory() == {}


def test_verified_wallet_does_not_repeat_identical_redirect_immediately():
    client = CapturedRequestWizzClient({'cookies': []})
    client.dynamic_url = 'https://multipass.wizzair.com/example'
    client._wallet_verified = True
    response = requests.Response()
    response.status_code = 302
    response.headers['Location'] = '/en/w6/subscriptions/spa/private-page/wallets'
    with patch.object(client,'_request',return_value=response) as send:
        with pytest.raises(WizzAvailabilityUnknown):
            client._send_and_decode({},'route')
    assert send.call_count == 1
