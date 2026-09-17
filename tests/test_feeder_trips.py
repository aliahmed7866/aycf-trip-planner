from datetime import datetime, timedelta, timezone

import pytest

from cache_db import ScanCacheDB
from feeder_trips import feeder_opportunities, match_feeders, normalize_offer
from scanner import Flight, _parse_dt

UTC = timezone.utc
NOW = datetime(2026, 9, 16, 6, tzinfo=UTC)


@pytest.fixture
def db(tmp_path):
    db = ScanCacheDB(str(tmp_path / 'aycf.sqlite3'))
    db.upsert_pdf_run('current', '2026-09-16T00:00:00', '2026-09-16', '2026-09-19', 10)
    db.mark_pdf_scanned('current')
    return db


def wizz(db, origin='Budapest', destination='Kutaisi', departure='2026-09-16T15:00',
         arrival='2026-09-16T20:00', number='W6123', run='current', observed=NOW,
         logical=None, texts=None, complete=True):
    day = datetime.fromisoformat(departure).date()
    dep = _parse_dt(day.isoformat(), (texts or (departure, arrival))[0])
    arr = _parse_dt(day.isoformat(), (texts or (departure, arrival))[1])
    db.replace_route_check(run, *(logical or (origin, destination)), day,
                           [Flight(origin, destination, number, dep, arr, *(texts or ('', '')))],
                           complete=complete)
    with db.connect() as conn:
        conn.execute('UPDATE route_flights SET fetched_at=? WHERE pdf_run_id=? AND flight_code=?',
                     (observed.isoformat(), run, number))


def fare(**changes):
    return dict({'origin': 'MAN', 'destination': 'BUD', 'airline': 'Ryanair',
                 'flight_number': 'FR123', 'departure': '2026-09-16T09:00+01:00',
                 'arrival': '2026-09-16T12:00+02:00', 'price_gbp': '29.99',
                 'observed_at': NOW.isoformat(), 'source': 'manual',
                 'booking_url': 'https://www.ryanair.com/'}, **changes)


def search(db, **kwargs):
    return feeder_opportunities(db, 'current', kwargs.pop('scope', {}), now=NOW, **kwargs)


def test_normalize_converts_local_airport_times_and_keeps_money_exact():
    result = normalize_offer(fare(departure='2026-09-16T09:00', arrival='2026-09-16T12:00'), now=NOW)
    assert result['departure'] == '2026-09-16T08:00:00+00:00'
    assert result['arrival'] == '2026-09-16T10:00:00+00:00'
    assert result['price_gbp'] == '29.99'


@pytest.mark.parametrize('changes', [
    {'origin': 'Manchester'}, {'destination': 'Warsaw'}, {'destination': 'ZZZ'},
    {'origin': 'LPL'}, {'destination': 'MAN'}, {'price_gbp': 'NaN'},
    {'price_gbp': 'Infinity'}, {'price_gbp': '-1'}, {'price_gbp': '0'}, {'price_gbp': '1.001'},
    {'price_gbp': '1e9999999999'}, {'price_gbp': 'sNaN'},
    {'currency': 'EUR'}, {'arrival': '2026-09-16T07:00Z'},
    {'booking_url': 'javascript:alert(1)'}, {'booking_url': 'https://me:secret@example.com/'},
    {'observed_at': '2026-09-16T07:00Z'}, {'observed_at': None}, {'airline': 'Unknown'},
])
def test_rejects_invalid_offers(changes):
    with pytest.raises(ValueError):
        normalize_offer(fare(**changes), now=NOW)


def test_ambiguous_and_nonexistent_local_dst_times_rejected():
    with pytest.raises(ValueError):
        normalize_offer(fare(departure='2026-10-25T01:30', arrival='2026-10-25T06:00'), now=NOW)
    with pytest.raises(ValueError):
        normalize_offer(fare(departure='2027-03-28T01:30', arrival='2027-03-28T06:00'), now=NOW)
    result = normalize_offer(fare(departure='2026-10-25T01:30+01:00',
                                   arrival='2026-10-25T06:00+01:00'), now=NOW)
    assert result['departure'] == '2026-10-25T00:30:00+00:00'


def test_direct_opportunity_matches_real_utc_connection_without_mixing_currencies(db):
    wizz(db)
    report = search(db)
    assert report['flight_count'] == report['total'] == 1
    opportunity = report['opportunities'][0]
    assert opportunity['hub'] == 'BUD'
    assert opportunity['destination'] == 'KUT'
    assert opportunity['country'] == 'Georgia'
    assert opportunity['first_departure'] == '2026-09-16T13:00:00+00:00'
    assert opportunity['final_arrival'] == '2026-09-16T16:00:00+00:00'
    assert opportunity['feeder_dates'] == ['2026-09-16']
    result = match_feeders(report, [fare()], now=NOW)[0]
    assert result['transfer_minutes'] == 180
    assert result['total_minutes'] == 480
    assert result['feeder_price_gbp'] == '29.99'
    assert result['aycf_fee_eur'] == '9.99'
    assert result['aycf_fee_count'] == 1 and result['outbound_only']
    assert 'total_price' not in result


@pytest.mark.parametrize('price,count', [('39.99', 1), ('40', 0), ('40.01', 0)])
def test_below_forty_is_a_strict_cap(db, price, count):
    wizz(db)
    assert len(match_feeders(search(db), [fare(price_gbp=price)], now=NOW)) == count


def test_warsaw_modlin_does_not_join_chopin_and_grouped_legacy_label_is_rejected(db):
    wizz(db, origin='Warsaw Chopin', logical=('Warsaw', 'Kutaisi'))
    assert match_feeders(search(db), [fare(destination='WMI')], now=NOW) == []
    assert match_feeders(search(db), [fare(destination='WAW')], now=NOW)
    with db.connect() as conn:
        conn.execute("UPDATE route_flights SET physical_origin='Warsaw'")
    report = search(db)
    assert report['opportunities'] == []
    assert report['omitted_times'] == 1


def test_offset_bearing_wizz_raw_times_are_not_treated_as_local_again(db):
    wizz(db, texts=('2026-09-16T15:00+02:00', '2026-09-16T20:00+04:00'))
    result = match_feeders(search(db), [fare()], now=NOW)
    assert result[0]['transfer_minutes'] == 180
    assert result[0]['total_minutes'] == 480


def test_stale_fares_past_departures_and_stale_wizz_rows_are_omitted(db):
    wizz(db)
    report = search(db)
    assert not match_feeders(report, [fare(observed_at=(NOW - timedelta(hours=6, seconds=1)).isoformat())], now=NOW)
    assert match_feeders(report, [fare(observed_at=(NOW - timedelta(hours=6)).isoformat())], now=NOW)
    assert not match_feeders(report, [fare(departure='2026-09-16T06:00Z')], now=NOW)
    assert not match_feeders(report, [fare()], now=NOW + timedelta(hours=25))
    with db.connect() as conn:
        conn.execute('UPDATE route_flights SET fetched_at=?', ((NOW - timedelta(hours=25)).isoformat(),))
    assert search(db)['stale_flights'] == 1
    assert search(db)['opportunities'] == []


def test_missing_old_and_out_of_window_checks_not_promoted_to_availability(db):
    db.upsert_pdf_run('old', '2026-09-15', '2026-09-16', '2026-09-19', 10)
    wizz(db, run='old')
    assert search(db)['opportunities'] == []
    wizz(db, departure='2026-09-20T15:00', arrival='2026-09-20T20:00')
    assert search(db)['opportunities'] == []
    wizz(db)
    with db.connect() as conn:
        conn.execute("DELETE FROM route_checks WHERE pdf_run_id='current'")
    assert search(db)['opportunities'] == []


def test_positive_partial_check_is_available_with_partial_notice(db):
    wizz(db, complete=False)
    report = search(db)
    assert report['scan_partial'] and len(report['opportunities']) == 1


def test_two_wizz_legs_need_approved_intermediate_and_exact_airports(db):
    wizz(db, origin='Milan Bergamo', destination='Budapest', departure='2026-09-16T12:00',
         arrival='2026-09-16T14:00', number='W111')
    wizz(db, departure='2026-09-16T18:00', arrival='2026-09-16T23:00')
    assert search(db)['total'] == 1
    report = search(db, scope={'connection_hubs': ['Budapest']})
    assert report['total'] == 2
    longer = next(item for item in report['opportunities'] if item['wizz_legs'] == 2)
    assert longer['hub'] == 'BGY' and longer['destination'] == 'KUT'
    assert [leg['origin'] for leg in longer['legs']] == ['BGY', 'BUD']
    assert search(db, scope={'connection_hubs': ['Budapest']}, max_wizz_legs=1)['total'] == 1


def test_cycle_and_short_internal_connection_rejected(db):
    wizz(db, origin='Kutaisi', destination='Budapest', departure='2026-09-16T08:00',
         arrival='2026-09-16T10:00', number='W111')
    wizz(db, departure='2026-09-16T15:00', arrival='2026-09-16T20:00')
    assert search(db, scope={'connection_hubs': ['Budapest']})['total'] == 1
    wizz(db, origin='Milan Bergamo', destination='Budapest', departure='2026-09-16T12:00',
         arrival='2026-09-16T14:00', number='W222')
    assert search(db, scope={'connection_hubs': ['Budapest']})['total'] == 1


def test_country_targets_and_existing_exclusions_are_respected(db):
    wizz(db)
    assert search(db, countries=['Egypt'])['opportunities'] == []
    assert search(db, scope={'excluded_countries': ['Georgia']})['opportunities'] == []
    assert search(db, scope={'excluded_airports': ['BUD']})['opportunities'] == []
    assert search(db, scope={'excluded_routes': [['BUD', 'KUT']]})['opportunities'] == []
    transit = {'excluded_airports': ['BUD'], 'connection_airports': ['BUD']}
    assert search(db, scope=transit)['total'] == 0
    assert search(db, scope=dict(transit, excluded_airports=[]))['total'] == 1
    assert search(db, scope=dict(transit, excluded_countries=['Hungary']))['total'] == 0


def test_long_layover_and_last_minute_feeder_rejected(db):
    wizz(db)
    report = search(db)
    assert not match_feeders(report, [fare(arrival='2026-09-16T12:01+02:00')], now=NOW)
    assert match_feeders(report, [fare(arrival='2026-09-16T12:00+02:00')], now=NOW)
    assert not match_feeders(report, [fare()], max_layover_minutes=120, now=NOW)


def test_previous_day_feeder_and_work_display_limits_reported(db):
    wizz(db, departure='2026-09-17T15:00', arrival='2026-09-17T20:00')
    wizz(db, origin='Warsaw Chopin', departure='2026-09-17T15:00',
         arrival='2026-09-17T20:00', number='W222')
    report = search(db, limit=1)
    assert report['limited'] and report['total'] == 2
    assert report['opportunities'][0]['feeder_dates'] == ['2026-09-16', '2026-09-17']
    assert search(db, work_limit=1)['incomplete']
    assert feeder_opportunities(db, None, None, now=NOW)['scope_unknown']
    assert feeder_opportunities(db, None, None, now=NOW)['incomplete']


def test_overnight_feeder_connection_uses_elapsed_time_not_calendar_dates(db):
    wizz(db, departure='2026-09-17T03:00', arrival='2026-09-17T08:00')
    offer = fare(departure='2026-09-16T19:00+01:00', arrival='2026-09-16T22:00+02:00')
    result = match_feeders(search(db), [offer], now=NOW)[0]
    assert result['transfer_minutes'] == 300
    assert result['total_minutes'] == 600
    assert result['feeder_dates'] == ['2026-09-16']


def test_internal_connection_cannot_switch_bucharest_airports(db):
    wizz(db, origin='Milan Bergamo', destination='Bucharest Baneasa',
         departure='2026-09-16T10:00', arrival='2026-09-16T13:00', number='W111')
    wizz(db, origin='Bucharest Otopeni', departure='2026-09-16T18:00', arrival='2026-09-16T22:00')
    report = search(db, scope={'connection_hubs': ['Bucharest']})
    assert report['total'] == 1 and report['opportunities'][0]['hub'] == 'OTP'


def test_match_rejects_path_returning_to_manchester(db):
    wizz(db)
    report = search(db)
    report['opportunities'][0]['legs'][0]['destination'] = 'MAN'
    assert not match_feeders(report, [fare()], now=NOW)


def test_matching_deduplicates_and_sorts_cash_then_leg_count(db):
    wizz(db)
    report = search(db)
    results = match_feeders(report, [fare(), fare(), fare(price_gbp='19.99', flight_number='FR222')], now=NOW)
    assert [item['feeder_price_gbp'] for item in results] == ['19.99', '29.99']


def test_unlimited_opportunities_keep_priced_match_after_first_eighty(db):
    base = datetime(2026, 9, 16, 15)
    for offset in range(80):
        wizz(db, departure=(base + timedelta(minutes=offset)).isoformat(),
             arrival=(base + timedelta(hours=5, minutes=offset)).isoformat(), number=f'W{offset}', complete=False)
    wizz(db, origin='Warsaw Chopin', departure='2026-09-16T18:00',
         arrival='2026-09-16T23:00', number='W81')
    limited = search(db)
    assert limited['limited'] and limited['total'] == 81
    assert not match_feeders(limited, [fare(destination='WAW')], now=NOW)
    full = search(db, limit=None)
    assert not full['limited'] and len(full['opportunities']) == 81
    assert match_feeders(full, [fare(destination='WAW')], now=NOW)[0]['hub'] == 'WAW'


def test_pure_search_never_mutates_scan_cache(db):
    wizz(db)
    with db.connect() as conn:
        before = [tuple(row) for row in conn.execute('SELECT * FROM route_flights')]
    match_feeders(search(db), [fare()], now=NOW)
    with db.connect() as conn:
        after = [tuple(row) for row in conn.execute('SELECT * FROM route_flights')]
    assert before == after
