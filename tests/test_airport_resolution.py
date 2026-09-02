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
