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
