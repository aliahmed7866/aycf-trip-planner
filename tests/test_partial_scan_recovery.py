from datetime import date, datetime
from unittest.mock import patch
import sqlite3

import pytest

from cache_db import ScanCacheDB
from scanner import Flight, WizzAvailabilityUnknown, WizzIntegrationChanged
from morning_scan import CapturedRequestWizzClient
from parallel_fetch import ParallelFetcher

DAY = date(2026, 9, 11)


def flight(origin='London Luton'):
    return Flight(origin, 'Budapest', 'W1', datetime(2026,9,11,10), datetime(2026,9,11,13), '10:00', '13:00')


def test_group_attempts_remaining_airports_and_persists_partial_then_resumes(tmp_path):
    db = ScanCacheDB(str(tmp_path / 'cache.sqlite'))
    db.upsert_pdf_run('run', DAY.isoformat(), DAY.isoformat(), '2026-09-14', 1)
    db.mark_pdf_scanned('run')
    attempts = []
    class Client:
        live_requests = no_availability_responses = wallet_redirects = html_retries = 0
        def check(self, a, b, day):
            attempts.append(a)
            if a == 'London Gatwick':
                raise WizzAvailabilityUnknown('pending')
            return [flight()] if a == 'London Luton' else []
    job = ('primary','London','Budapest',DAY,['London Gatwick','London Luton','London Stansted'],['Budapest'])
    result = ParallelFetcher(Client, workers=1)._job(job)
    assert attempts == ['London Gatwick','London Luton','London Stansted']
    assert len(result['flights']) == 1 and len(result['unknown']) == 1
    db.replace_route_check('run','London','Budapest',DAY,result['flights'],complete=False,checked_pairs=result['checked_pairs'])
    assert db.get_flights('London','Budapest',DAY,'run')[0].origin == 'London Luton'
    assert not db.route_checked('run','London','Budapest',DAY)
    assert db.route_check_info('run','London','Budapest',DAY) is None
    assert not db.get_pdf_run('run')['scanned_at']
    # A later verified empty Luton result removes that airport's earlier flight.
    db.replace_route_check('run','London','Budapest',DAY,[],complete=False,checked_pairs=[('London Luton','Budapest')])
    assert db.get_flights('London','Budapest',DAY,'run') is None
    db.replace_route_check('run','London','Budapest',DAY,[])
    assert db.route_checked('run','London','Budapest',DAY)
    assert db.get_flights('London','Budapest',DAY,'run') == []


def test_existing_cache_migration_preserves_complete_checks(tmp_path):
    path = str(tmp_path / 'old.sqlite')
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE route_checks (pdf_run_id TEXT, origin TEXT, destination TEXT, travel_date TEXT, fetched_at TEXT, flight_count INTEGER, PRIMARY KEY(pdf_run_id,origin,destination,travel_date))')
        conn.execute("INSERT INTO route_checks VALUES ('run','A','B','2026-09-11','2026-09-11',0)")
    db = ScanCacheDB(path)
    assert db.route_checked('run','A','B',DAY)


@pytest.mark.parametrize('data', [{}, {'error': 'unavailable'}, {'flights': [{'flightCode': 'W1'}]}, {'flights': [{'flightCode': 'W1','departure':'bad','arrival':'12:00'}]}])
def test_auth_preflight_rejects_malformed_availability(data):
    client = CapturedRequestWizzClient({'cookies': []})
    client.dynamic_url = 'https://multipass.wizzair.com/example'
    client.captured_request_template = {'origin':'BUD','destination':'LTN','departure':'2020-01-01'}
    with patch.object(client, '_send_and_decode', return_value=data) as send:
        with pytest.raises(WizzIntegrationChanged):
            client.preflight()
    assert send.call_args.args[0]['departure'] == date.today().isoformat()
    assert client.captured_request_template['departure'] == '2020-01-01'


def test_all_wallet_probes_stop_before_full_scan():
    from morning_scan import verify_scan_requests
    client = CapturedRequestWizzClient({'cookies': []})
    jobs = [('primary',str(i),'UK',DAY,[str(i)],['UK'],[(str(i),'UK')]) for i in range(100)]
    with patch.object(client, 'preflight', return_value={'ok':True,'availability_verified':False}) as probe:
        result = verify_scan_requests(client,jobs)
    assert result['state'] == 'request_repair_required'
    assert probe.call_count == 3
    with patch.object(client, 'preflight', side_effect=[{'ok':False}, {'ok':True,'availability_verified':True}]):
        assert verify_scan_requests(client,jobs)['ok']


def test_partial_current_scan_remains_visible_in_planner_and_flights(tmp_path, monkeypatch):
    import pandas as pd
    import app as web
    from types import SimpleNamespace
    from scan_scope import default_scope
    db = ScanCacheDB(str(tmp_path / 'web.sqlite'))
    db.upsert_pdf_run('current', DAY.isoformat(), DAY.isoformat(), '2026-09-14', 1)
    db.replace_route_check('current', 'London', 'Budapest', DAY, [flight()],
                           complete=False, checked_pairs=[('London Luton', 'Budapest')])
    frame = pd.DataFrame([{'departure_from':'London', 'departure_to':'Budapest',
                           'data_generated':DAY.isoformat(), 'departure_start':DAY.isoformat(),
                           'departure_end':'2026-09-14'}])
    graph = SimpleNamespace(latest_frame=lambda:frame, cities=lambda:['London', 'Budapest'])
    monkeypatch.setattr(web, '_cache_dir', lambda:str(tmp_path))
    monkeypatch.setattr(web, 'update_data_if_needed', lambda **kw:SimpleNamespace(data_dir=str(tmp_path)))
    monkeypatch.setattr(web, 'CurrentRouteGraph', lambda path:graph)
    monkeypatch.setattr(web, 'ScanCacheDB', lambda:db)
    monkeypatch.setattr(web, 'load_scope', default_scope)
    monkeypatch.setattr(web, 'scan_scope_with_preferences', lambda scope:scope)
    monkeypatch.setattr(web, 'scan_run_id', lambda *args:'current')
    monkeypatch.setattr(web, '_vault_or_none', lambda:None)
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path))
    app = web.create_app()
    app.testing = True
    client = app.test_client()
    response = client.get('/')
    assert response.status_code == 200
    assert b'Partial scan' in response.data
    assert b'New scan required' not in response.data
    response = client.get('/flights')
    assert response.status_code == 200
    assert b'London Luton' in response.data and b'W1' in response.data
    assert b'Partial scan' in response.data
    assert not db.get_pdf_run('current')['scanned_at']
    # A typed destination searches all origins, supports codes/city groups,
    # and never silently drops an unmatched filter.
    db.replace_route_check('current', 'Liverpool', 'Budapest', DAY,
                           [flight('Liverpool')], complete=True)
    for query in ('Budapest', 'bud', 'budap'):
        page = client.get('/flights', query_string={'destination': query}).data
        assert b'London Luton' in page and b'Liverpool' in page
        assert page.count(b'<article ') == 2
    assert b'No matches' in client.get('/flights?destination=nowhere').data
    for destination, code in [('London Luton', 'W2'), ('London Gatwick', 'W3')]:
        incoming = Flight('Budapest', destination, code, datetime(2026,9,11,15),
                          datetime(2026,9,11,17), '15:00', '17:00')
        db.replace_route_check('current', 'Budapest', destination, DAY,
                               [incoming], complete=True)
    page = client.get('/flights?destination=London').data
    assert page.count(b'<article ') == 2 and b'W2' in page and b'W3' in page
    page = client.get('/flights?destination=LTN').data
    assert page.count(b'<article ') == 1 and b'W2' in page and b'W3' not in page
    page = client.get('/flights?destination=London&flight=W3').data
    assert page.count(b'<article ') == 1 and b'W3' in page
    assert b'name="destination" id="flight-destination-search"' in page
    # A changed scope must never expose the previous scope's inventory.
    monkeypatch.setattr(web, 'scan_run_id', lambda *args:'new-scope')
    response = client.get('/')
    assert b'New scan required' in response.data


@pytest.mark.parametrize('complete,flights,usable', [(False, False, False), (False, True, True), (True, False, True)])
def test_readiness_distinguishes_unknown_empty_and_verified_inventory(tmp_path, complete, flights, usable):
    from search_cache import scan_readiness
    db = ScanCacheDB(str(tmp_path / 'readiness.sqlite'))
    db.upsert_pdf_run('current', DAY.isoformat(), DAY.isoformat(), '2026-09-14', 1)
    db.replace_route_check('current', 'London', 'Budapest', DAY,
                           [flight()] if flights else [], complete=complete,
                           checked_pairs=[('London Luton','Budapest')] if flights or complete else [])
    state = scan_readiness(db, 'current', db.get_pdf_run('current'))
    assert state == {'ready': False, 'usable': usable, 'partial': usable}
    db.mark_pdf_scanned('current')
    assert scan_readiness(db, 'current', db.get_pdf_run('current')) == {'ready': True, 'usable': True, 'partial': False}
    assert scan_readiness(db, 'missing', None) == {'ready': False, 'usable': False, 'partial': False}
