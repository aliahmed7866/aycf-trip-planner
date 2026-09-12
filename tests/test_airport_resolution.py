from airport_resolution import archive_pair, resolve_airport_rows


def test_london_airport_inherits_shared_history_without_losing_identity():
    rows = [
        {"origin": "London", "destination": "Budapest", "archive_score": 82.0, "recent_30d": 90.0, "trend": "improving", "archive": {"last_seen": "2026-09-01"}},
        {"origin": "London Luton", "destination": "Budapest", "archive_score": None, "recent_30d": None, "trend": "insufficient", "positive_checks": 2, "total_checks": 4},
    ]
    result = resolve_airport_rows(rows)
    luton = next(r for r in result if r["origin"] == "London Luton")
    assert luton["archive_score"] == 82.0
    assert luton["historical_scope"] == "London-wide"
    assert luton["archive_origin"] == "London"
    assert luton["airport_evidence"] == "Observed in local AYCF scans"


def test_city_history_does_not_fabricate_unseen_airports():
    rows = [{"origin": "London", "destination": "Budapest", "archive_score": 82.0}]
    result = resolve_airport_rows(rows)
    assert [r["origin"] for r in result] == ["London"]
    assert archive_pair("London Gatwick", "Budapest") == ("London", "Budapest")


def test_secondary_airport_history_is_labelled_as_shared_city_evidence():
    rows = [{'origin': 'Bucharest', 'destination': 'London', 'archive_score': 70}]
    evidence = [{'origin': 'Bucharest Baneasa', 'destination': 'London Gatwick',
                 'observed_scans': 1, 'positive_checks': 1}]
    result = resolve_airport_rows(rows, evidence)
    assert len(result) == 1
    assert result[0]['historical_scope'] == 'Bucharest / London-wide'
    assert result[0]['physical_evidence']
    assert result[0]['total_checks'] is None
    assert archive_pair('Warsaw Modlin', 'Paris Orly') == ('Warsaw', 'Paris')


def test_physical_flight_evidence_materializes_airport_and_suppresses_generic_row():
    rows = [{"origin": "London", "destination": "Budapest", "archive_score": 82.0, "recent_30d": 90.0, "trend": "steady"}]
    evidence = [{"origin": "London Luton", "destination": "Budapest", "observed_scans": 2, "positive_checks": 3, "available_dates": 2, "flight_appearances": 4, "last_seen": "2026-09-02"}]
    result = resolve_airport_rows(rows, evidence)
    assert not any(r["origin"] == "London" for r in result)
    luton = next(r for r in result if r["origin"] == "London Luton")
    assert luton["archive_score"] == 82.0
    assert luton["physical_evidence"] is True
    assert luton["historical_scope"] == "London-wide"
