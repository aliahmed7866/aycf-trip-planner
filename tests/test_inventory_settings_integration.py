"""Physical airport identity must survive settings, planning and worker requests."""
from datetime import date
from types import SimpleNamespace

import pytest
from airport_catalog import CURRENT_WIZZ_IATA, airport_code
from scanner import WizzAYCFClient
from station_resolver import prepare_required_stations
from scan_scope import (default_scope, load_scope, save_scope, route_requests,
                        scan_plan, scan_jobs, endpoint_matches, scope_fingerprint)
from scan_settings import exclusion_catalog, submitted_settings
from recommendation_preferences import save_preferred_destinations, load_preferred_destinations
from werkzeug.datastructures import MultiDict


def test_new_tenerife_airport_uses_island_local_time():
    from datetime import datetime, timedelta
    from short_trips import airport_zone
    assert datetime(2026, 9, 12, tzinfo=airport_zone('Tenerife Norte')).utcoffset() == timedelta(hours=1)


def test_canonical_physical_airports_override_stale_captured_aliases(monkeypatch):
    monkeypatch.setenv('AYCF_DISABLE_PUBLIC_STATION_MAP', 'true')
    client = SimpleNamespace(station_ids={'dubai': 'DWC', 'alexandria': 'HBE',
                                         'warsaw modlin': 'WAW', 'custom station': 'XYZ'})
    names = ['Dubai', 'Alexandria', 'Alexandria (Borg El Arab)', 'Warsaw Modlin', 'Custom station']
    assert prepare_required_stations(client, names)['unresolved'] == []
    for name, code in zip(names, ['DXB', 'ALY', 'HBE', 'WMI', 'XYZ']):
        assert WizzAYCFClient.resolve_station(client, name) == code


def test_similarly_named_airports_remain_distinct_in_saved_exclusions(tmp_path, monkeypatch):
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path))
    names = ['Alexandria', 'Alexandria (Borg El Arab)']
    assert not endpoint_matches(*names)
    assert not endpoint_matches(*reversed(names))
    save_scope(['London Luton'], 'all', [], excluded_airports=names)
    assert load_scope()['excluded_airports'] == names


def test_startup_hook_and_saved_iata_choices_use_the_same_inventory():
    import sitecustomize
    client = SimpleNamespace(station_ids={'dubai': 'DWC'})
    assert sitecustomize._resolve_station_with_local_aliases(client, 'Dubai') == 'DXB'
    assert sitecustomize._resolve_station_with_local_aliases(client, 'Nis') == 'INI'
    scope = dict(default_scope(), origins=['LTN'])
    catalog = exclusion_catalog([('London', 'Budapest')], scope)
    assert catalog['selected_uk_options'] == ['London Luton']
    assert route_requests('London', 'Budapest', scope) == [('London Luton', 'Budapest')]


def test_directional_directory_expands_city_but_not_airport_or_reverse():
    scope = dict(default_scope(), _route_directory={'routes': {
        'OTP': ['LTN'], 'BBU': ['LGW'], 'LTN': ['OTP'], 'LGW': ['OTP'],
    }})
    assert route_requests('Bucharest', 'London', scope) == [
        ('Bucharest Baneasa', 'London Gatwick'), ('Bucharest Otopeni', 'London Luton')]
    assert route_requests('London', 'Bucharest', scope) == [
        ('London Gatwick', 'Bucharest Otopeni'), ('London Luton', 'Bucharest Otopeni')]
    assert route_requests('Bucharest Otopeni', 'London', scope) == [('Bucharest Otopeni', 'London Luton')]
    scope['excluded_routes'] = [['BBU', 'LGW']]
    assert route_requests('Bucharest', 'London', scope) == [('Bucharest Otopeni', 'London Luton')]
    scope['excluded_airports'] = ['Bucharest']
    assert route_requests('Bucharest', 'London', scope) == []


@pytest.mark.parametrize('city, code, label', [
    ('Warsaw', 'WMI', 'Warsaw Modlin'), ('Paris', 'ORY', 'Paris Orly'),
    ('Milan', 'BGY', 'Milan Bergamo'), ('Rome', 'CIA', 'Rome Ciampino'),
])
def test_secondary_airport_jobs_and_preflight_match_preview(city, code, label, monkeypatch):
    scope = dict(default_scope(), origins=['London Luton'], _route_directory={
        'routes': {'LTN': [code], code: ['LTN']}})
    plan = scan_plan([('London', city)], scope, days=1)
    jobs = scan_jobs(plan, scope, [date(2026, 9, 12)])
    requests = [pair for job in jobs for pair in job[6]]
    assert set(requests) == {('London Luton', label), (label, 'London Luton')}
    assert len(requests) == plan['request_units'] == 2
    client = SimpleNamespace(station_ids={})
    monkeypatch.setenv('AYCF_DISABLE_PUBLIC_STATION_MAP', 'true')
    assert not prepare_required_stations(client, [n for pair in requests for n in pair])['unresolved']
    assert client.station_ids[label.casefold()] == code


def test_missing_directory_does_not_multiply_city_requests():
    assert route_requests('Warsaw', 'Budapest', default_scope()) == [('Warsaw', 'Budapest')]


def test_unified_settings_preserve_preferences_and_preview_equals_saved_plan(tmp_path, monkeypatch):
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path))
    save_preferred_destinations(['Kutaisi', 'Yerevan', 'Baku'])
    scope = dict(default_scope(), preferred_destinations=load_preferred_destinations())
    pairs = [('London', 'Rome'), ('Rome', 'Bilbao'), ('Bilbao', 'London')]
    catalog = exclusion_catalog(pairs, scope)
    form = MultiDict([('scope_settings', '1'), ('scope_origins', 'London Luton'),
        ('destination_mode', 'all'), ('connection_hubs', 'Rome'),
        ('excluded_airports', 'Bilbao'), ('connection_airports', 'Bilbao'),
        ('connection_budget', '6')])
    proposed = submitted_settings(form, catalog, scope)
    preview = scan_plan(pairs, dict(proposed), days=3)
    save_scope(proposed['origins'], proposed['destination_mode'], proposed['destinations'],
               proposed['connection_hubs'], proposed['workers'],
               **{key: proposed[key] for key in ('excluded_airports', 'excluded_countries',
                   'excluded_routes', 'connection_airports', 'connection_budget')})
    saved = dict(load_scope(), preferred_destinations=load_preferred_destinations())
    assert saved['preferred_destinations'] == ['Kutaisi', 'Yerevan', 'Baku']
    assert scan_plan(pairs, saved, days=3)['request_units'] == preview['request_units']
    assert saved['excluded_airports'] == ['Bilbao']
    assert scope_fingerprint(scope) != scope_fingerprint(saved)
    assert {item['code'] for rows in catalog['countries'].values() for item in rows if item['code']} >= CURRENT_WIZZ_IATA
    assert [a['name'] for a in catalog['connection_options']] == sorted(
        [a['name'] for a in catalog['connection_options']], key=str.casefold)
    assert not any(a['code'] == 'STN' for a in catalog['connection_options'])
