from datetime import date
from scan_scope import scan_plan, scan_jobs, default_scope, route_requests, save_scope, load_scope


def scope(**changes):
    return dict(default_scope(), **dict({'origins': ['London Luton'],
        'connection_hubs': ['Rome'], 'excluded_airports': ['Bilbao'],
        'connection_airports': ['Bilbao'], 'connection_budget': 100}, **changes))


PAIRS = [('Bilbao', 'London'), ('Rome', 'Bilbao'), ('Bilbao', 'Madrid')]


def test_complete_bundles_with_exact_budget_and_no_unrelated_edges():
    s = scope()
    p = scan_plan(PAIRS, s, days=3)
    assert p['connection_coverage']['extra_checks'] == 6
    assert set(p['routes']) == {('Bilbao', 'London'), ('Rome', 'Bilbao')}
    jobs = scan_jobs(p, s, [date(2026, 9, d) for d in (12, 13, 14)])
    assert sum(len(j[6]) for j in jobs) == 6
    assert route_requests('Bilbao', 'Madrid', s) == []
    assert scan_plan(PAIRS, s, days=3) == p  # no mutation accumulating coverage
    s = scope(connection_budget=5)
    p = scan_plan(PAIRS, s, days=3)
    assert not p['routes']
    assert p['connection_coverage']['deferred_bundles'] == 1


def test_no_override_of_country_route_or_disabled_transit():
    for changes in ({'excluded_countries': ['Spain']},
                    {'excluded_routes': [['Rome', 'Bilbao']]},
                    {'connection_airports': []}, {'connection_budget': 0}):
        assert not scan_plan(PAIRS, scope(**changes), days=3)['routes']


def test_directed_anchor_required_and_outbound_supported():
    assert not scan_plan([('Rome', 'Bilbao')], scope())['routes']
    pairs = [('London', 'Bilbao'), ('Bilbao', 'Rome')]
    assert set(scan_plan(pairs, scope())['routes']) == set(pairs)


def test_preferences_rank_before_hubs_and_deduplicate():
    s = scope(preferred_destinations=['Budapest'], connection_budget=6)
    p = scan_plan(PAIRS + [('Budapest', 'Bilbao')], s, days=3)
    assert set(p['routes']) == {('Budapest', 'Bilbao'), ('Bilbao', 'London')}
    assert p['connection_coverage']['extra_checks'] == 6


def test_settings_survive_other_scope_saves(tmp_path, monkeypatch):
    monkeypatch.setenv('AYCF_CONFIG_DIR', str(tmp_path))
    save_scope(['London Luton'], 'all', [], connection_airports=['Bilbao'], connection_budget=42)
    save_scope(['Liverpool'], 'all', [])
    assert load_scope()['connection_airports'] == ['Bilbao']
    assert load_scope()['connection_budget'] == 42


def test_connection_form_validates_budget_and_keeps_saved_options():
    import pytest
    from werkzeug.datastructures import MultiDict
    from scan_settings import submitted_exclusions, exclusion_catalog
    catalog = exclusion_catalog([], scope())
    result = submitted_exclusions(MultiDict([
        ('connection_airports', 'Bilbao'), ('connection_budget', '50')]), catalog)
    assert result['connection_airports'] == ['Bilbao']
    assert result['connection_budget'] == 50
    for value in ('101', '-1', 'abc'):
        with pytest.raises(ValueError):
            submitted_exclusions(MultiDict([('connection_budget', value)]), catalog)


def test_prepared_requests_still_obey_new_hard_vetoes_and_disabled_settings():
    s = scope()
    scan_plan(PAIRS, s, days=3)
    assert route_requests('Bilbao', 'London', s)
    for changes in ({'connection_budget': 0}, {'connection_airports': []},
                    {'excluded_countries': ['Spain']},
                    {'excluded_routes': [['BIO', 'LTN']]}):
        assert not route_requests('Bilbao', 'London', dict(s, **changes))


def test_saved_iata_connection_remains_selected_under_readable_alias():
    from scan_settings import exclusion_catalog
    catalog = exclusion_catalog(PAIRS, scope(connection_airports=['BIO']))
    bilbao = next(a for a in catalog['countries']['Spain'] if a['code'] == 'BIO')
    assert bilbao['name'] == 'Bilbao' and bilbao['connection']
