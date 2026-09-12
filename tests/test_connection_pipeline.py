"""Exercise saved UI choices through both workers and both cached search paths."""
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from flask import Flask, request

import morning_scan
import tiered_morning
import scan_settings
from cache_db import ScanCacheDB
from scanner import Flight, WizzAvailabilityUnknown
from scan_scope import load_scope, save_scope, scan_plan
from short_trips import released_short_trips
from itinerary_search import cached_scan_itineraries
from search_support import approved_connections
from termux import multi_search


@pytest.mark.parametrize('worker', [morning_scan, tiered_morning])
@pytest.mark.parametrize('partial', [False, True])
def test_saved_settings_scan_cache_search_and_resume(worker, partial, tmp_path, monkeypatch):
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path / 'config'))
    monkeypatch.setenv('AYCF_DISABLE_PUBLIC_STATION_MAP', 'true')
    monkeypatch.setenv('AYCF_SCAN_WORKERS', '1')
    today = date.today()
    generated = datetime.combine(today, time.min)
    end = generated + timedelta(days=2, hours=23)
    pairs = [('London', 'Rome'), ('Rome', 'Bilbao'), ('Bilbao', 'London')]
    frame = pd.DataFrame([dict(departure_from=a, departure_to=b,
                              data_generated=generated.isoformat(),
                              availability_start=generated.isoformat(),
                              availability_end=end.isoformat()) for a, b in pairs])
    enrich = lambda s: dict(s, preferred_destinations=['Rome'])
    monkeypatch.setattr(scan_settings, 'scan_scope_with_preferences', enrich)
    save_scope(['London Luton'], 'all', [], ['Rome'], excluded_airports=['Bilbao'])
    root = Path(__file__).resolve().parents[1]
    app = Flask(__name__, template_folder=str(root / 'templates'))
    app.secret_key = 'test'
    app.jinja_env.globals['csrf_token'] = lambda: 'test-csrf'
    app.add_url_rule('/', 'index', lambda: 'planner')
    app.register_blueprint(scan_settings.create_scan_settings_blueprint(
        lambda: (frame, pairs, [], [], ''), lambda: request.form.get('csrf_token') == 'test-csrf'))
    form = dict(csrf_token='test-csrf', excluded_airports='Bilbao',
                connection_airports='Bilbao', connection_budget='6')
    with app.test_client() as client:
        preview = client.post('/settings/scan-exclusions/preview', data=form).get_json()
        assert preview['connection_coverage']['extra_checks'] == 6
        saved = client.post('/settings/scan-exclusions', data=form, headers={'Accept': 'application/json'})
        assert saved.status_code == 200 and saved.get_json()['ok']
        html = client.get('/settings/scan-exclusions').get_data(as_text=True)
        assert 'value="Bilbao" checked' in html
    calls, probes = [], []
    fail = {('Bilbao', 'London Luton', today + timedelta(days=2))} if partial else set()

    class Client:
        def __init__(self, *args, **kwargs):
            self.live_requests = self.no_availability_responses = self.wallet_redirects = self.html_retries = 0
            self.station_ids = {}
            self.dynamic_url = 'https://multipass.wizzair.com/test'
            self.captured_request_method = 'POST'
            self.captured_template_type = 'json'
            self.captured_request_template = {}
            self.http = SimpleNamespace(cookies={})
        def preflight(self, a, b, day):
            probes.append((a, b, day))
            assert self.station_ids.get('bilbao') == 'BIO'
            return {'ok': True}
        def check(self, a, b, day):
            calls.append((a, b, day))
            self.live_requests += 1
            if (a, b, day) in fail:
                fail.remove((a, b, day))
                raise WizzAvailabilityUnknown('Test pending return')
            schedule = {('London Luton', 'Rome', today): (8, 11),
                        ('Rome', 'Bilbao', today + timedelta(days=1)): (15, 18),
                        ('Bilbao', 'London Luton', today + timedelta(days=1)): (21, 22)}
            if (a, b, day) not in schedule:
                return []
            departure, arrival = schedule[(a, b, day)]
            return [Flight(a, b, 'W123', datetime.combine(day, time(departure)),
                           datetime.combine(day, time(arrival)), '', '')]

    monkeypatch.setattr(worker, 'refresh_direct_snapshot', lambda *a: ('pdf', frame, generated, generated, end))
    monkeypatch.setattr(worker, '_mirror_for_web', lambda *a: None)
    monkeypatch.setattr(worker, 'scan_scope_with_preferences', enrich)
    monkeypatch.setattr(worker, 'SessionVault', lambda: SimpleNamespace(load=lambda: {'cookies': []}))
    monkeypatch.setattr(worker, 'CapturedRequestWizzClient', Client)
    monkeypatch.setattr(worker, '_apply_wizz_runtime', lambda c: True)
    db = ScanCacheDB(str(tmp_path / 'scan.sqlite3'))
    first = worker._run_locked(db)
    assert first['state'] == 'partial' if partial else first['ok']
    assert len(calls) == preview['request_units'] == 12
    assert set(probes) <= set(calls)
    graph = SimpleNamespace(latest_frame=lambda: frame, edges_for_day=lambda day: set(pairs))
    monkeypatch.setattr(multi_search, 'scan_scope_with_preferences', enrich)
    context = multi_search._current_scope_run(graph, db)
    assert context['usable']
    assert context['ready'] is not partial
    now = generated.replace(tzinfo=timezone.utc)
    trips = released_short_trips(db, context['run_id'], scope=context['scope'],
                                origins=['London Luton'], returns=['London Luton'],
                                max_stops=1, now=now)
    assert trips['total'] == 1 and trips['trips'][0]['destination'] == 'Rome'
    found, _ = cached_scan_itineraries(graph, db, 'Rome', 'London', today + timedelta(days=1),
                                      days=1, max_stops=1, scope=context['scope'], approved_hubs=['Rome'],
                                      pdf_run_id=context['run_id'])
    assert len(approved_connections(found, context['scope'])) == 1
    found, _ = cached_scan_itineraries(graph, db, 'Rome', None, today + timedelta(days=1),
                                      days=1, max_stops=1, scope=context['scope'], approved_hubs=['Rome'],
                                      pdf_run_id=context['run_id'])
    assert found and all(row['path'][-1] != 'Bilbao' for row in found)
    before = len(calls)
    second = worker._run_locked(db)
    assert second['ok']
    assert len(calls) - before == (1 if partial else 0)
    assert multi_search._current_scope_run(graph, db)['run_id'] == context['run_id']
