from datetime import datetime, timedelta, timezone
import sqlite3

import pytest

from feeder_ranking import rank_feeder_targets
from route_history import _connect

NOW = datetime(2026, 9, 16, 6, tzinfo=timezone.utc)


def opportunity(hub='BUD', destination='KUT', country='Georgia', departure='2026-09-17T15:00Z',
                dates=('2026-09-16', '2026-09-17'), via=None):
    legs = [{'origin': hub, 'destination': via or destination,
             'departure': departure, 'flight_number': 'W1'}]
    if via:
        legs.append({'origin': via, 'destination': destination,
                     'departure': '2026-09-17T21:00Z', 'flight_number': 'W2'})
    return {'hub': hub, 'hub_name': hub, 'destination': destination,
            'destination_name': destination, 'country': country, 'legs': legs,
            'first_departure': departure, 'feeder_dates': list(dates)}


def history(path, origin='Budapest', destination='Kutaisi', days=(-1, -2, -3),
            positives=None, physical_origin=None, fetched_day=None):
    with _connect(str(path)) as conn:
        for index, offset in enumerate(days):
            instant = NOW + timedelta(days=offset)
            observed = (NOW + timedelta(days=fetched_day)).isoformat() if fetched_day is not None else instant.isoformat()
            scan = conn.execute('SELECT COALESCE(MAX(scan_run_id),0)+1 FROM snapshots').fetchone()[0]
            cursor = conn.execute('INSERT INTO snapshots(scan_run_id,pdf_run_id,scanned_at,recorded_at) VALUES(?,?,?,?)',
                                  (scan, 'run', instant.isoformat(), instant.isoformat()))
            positive = positives is None or index in positives
            conn.execute('INSERT INTO route_appearances VALUES(?,?,?,?,?,?,?)',
                         (cursor.lastrowid, 'run', origin, destination, instant.date().isoformat(), int(positive), observed))
            if positive:
                conn.execute('INSERT INTO flight_appearances VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                             (cursor.lastrowid, 'run', origin, destination, instant.date().isoformat(),
                              'W1', instant.isoformat(), instant.isoformat(), physical_origin,
                              destination, observed))


def rank(items, path, **extra):
    return rank_feeder_targets({'opportunities': items, **extra}, history_path=path, now=NOW)


def test_history_ranks_current_targets_without_inventing_routes_or_dates(tmp_path):
    path = tmp_path / 'history.sqlite3'
    history(path)
    history(path, origin='Milan Bergamo')
    targets = rank([opportunity('WAW'), opportunity()], path)
    assert targets[0]['hub'] == 'BUD'
    assert targets[0]['stability_label'] == 'Stable recent history'
    assert targets[0]['history_days'] == targets[0]['checked_days'] == 3
    assert targets[0]['history_rate'] == 100
    assert {item['hub'] for item in targets} == {'BUD', 'WAW'}
    assert {item['date'] for item in targets} == {'2026-09-16', '2026-09-17'}
    assert rank([], path) == []


def test_repeated_scans_and_reused_checks_do_not_create_stability(tmp_path):
    path = tmp_path / 'history.sqlite3'
    history(path, days=(-1, -1, -1))
    history(path, days=(-1, -2, -3), fetched_day=-4)
    target = rank([opportunity()], path)[0]
    assert target['history_days'] == target['checked_days'] == 2
    assert target['stability_label'] == 'Limited history'


def test_rate_denominator_includes_negative_checked_days(tmp_path):
    path = tmp_path / 'history.sqlite3'
    history(path, days=(-1, -2, -3, -4, -5, -6), positives={0, 1, 2})
    target = rank([opportunity()], path)[0]
    assert target['history_days'] == 3 and target['checked_days'] == 6
    assert target['history_rate'] == 50
    assert target['stability_label'] == 'Variable history'


def test_recent_decline_is_not_hidden_by_older_positive_history(tmp_path):
    path = tmp_path / 'history.sqlite3'
    history(path, days=(-1, -2, -3), positives=set())
    history(path, days=(-35, -36, -37, -38, -39, -40))
    target = rank([opportunity()], path)[0]
    assert target['history_window_days'] == 30
    assert target['history_rate'] == 0
    assert not target['stable']


def test_sixty_day_fallback_is_bounded_and_ignores_future_history(tmp_path):
    path = tmp_path / 'history.sqlite3'
    history(path, days=(-35, -36, -37, -70, 1))
    target = rank([opportunity()], path)[0]
    assert target['history_window_days'] == 60
    assert target['history_days'] == 3 and target['stable']


@pytest.mark.parametrize('group,physical,wrong', [('Warsaw', 'Warsaw Chopin', 'WMI'),
                                                ('Milan', 'Milan Bergamo', 'MXP')])
def test_city_group_history_is_not_exact_airport_stability(tmp_path, group, physical, wrong):
    path = tmp_path / 'history.sqlite3'
    history(path, origin=group, physical_origin=physical)
    exact = 'WAW' if group == 'Warsaw' else 'BGY'
    targets = rank([opportunity(exact), opportunity(wrong)], path)
    right = next(item for item in targets if item['hub'] == exact)
    other = next(item for item in targets if item['hub'] == wrong)
    assert right['history_days'] == 3 and right['checked_days'] == 0
    assert right['history_rate'] is None and not right['stable']
    assert other['history_days'] == 0


def test_path_stability_requires_every_onward_leg(tmp_path):
    path = tmp_path / 'history.sqlite3'
    history(path, origin='Milan Bergamo', destination='Budapest')
    target = rank([opportunity('BGY', via='BUD')], path)[0]
    assert target['history_days'] == 0 and not target['stable']
    history(path)
    target = rank([opportunity('BGY', via='BUD')], path)[0]
    assert target['stable'] and target['direct_count'] == 0


def test_direct_routes_diversity_and_duplicate_paths(tmp_path):
    path = tmp_path / 'missing.sqlite3'
    bud = opportunity()
    items = [opportunity('BGY', via='BUD'), opportunity('WAW'), bud, dict(bud),
             opportunity('BUD', 'SPX', 'Egypt')]
    targets = rank(items, path)
    assert targets[0]['hub'] == 'BUD'
    assert targets[0]['opportunity_count'] == 2
    assert targets[0]['countries'] == ['Egypt', 'Georgia']
    assert targets[-1]['hub'] == 'BGY'
    assert targets == rank(list(reversed(items)), path)


def test_broad_dates_pruned_and_future_departure_window_required(tmp_path):
    item = opportunity(departure='2026-09-18T15:00Z', dates=('2026-09-16', '2026-09-17', '2026-09-18'))
    targets = rank([item], tmp_path / 'missing')
    assert {target['date'] for target in targets} == {'2026-09-17', '2026-09-18'}
    assert targets[0]['date'] == '2026-09-18'
    assert rank([opportunity(departure='2026-09-16T09:30Z')], tmp_path / 'missing') == []
    assert rank([item], tmp_path / 'missing', max_layover_minutes=120) == []


def test_custom_transfer_constraints_and_uk_local_query_dates(tmp_path):
    item = opportunity(departure='2026-09-17T04:30Z', dates=('2026-09-16', '2026-09-17'))
    targets = rank([item], tmp_path / 'missing', min_transfer_minutes=360)
    assert [target['date'] for target in targets] == ['2026-09-16']


@pytest.mark.parametrize('kind', ['missing', 'corrupt', 'old_schema'])
def test_missing_or_unreadable_history_falls_back_without_creating_database(tmp_path, kind):
    path = tmp_path / kind / 'history.sqlite3'
    if kind != 'missing':
        path.parent.mkdir()
        if kind == 'corrupt':
            path.write_bytes(b'broken')
        else:
            with sqlite3.connect(path) as conn:
                conn.execute('CREATE TABLE unrelated (value)')
        before = path.read_bytes()
    targets = rank([opportunity()], path)
    assert targets and all(target['stability_label'] == 'Limited history' for target in targets)
    if kind == 'missing':
        assert not path.parent.exists()
    else:
        assert path.read_bytes() == before


def test_group_hubs_non_target_countries_and_malformed_paths_not_queried(tmp_path):
    items = [opportunity('Warsaw'), opportunity(country='Hungary'),
             opportunity(departure='invalid'), {'hub': 'BUD'}]
    assert rank(items, tmp_path / 'missing') == []
