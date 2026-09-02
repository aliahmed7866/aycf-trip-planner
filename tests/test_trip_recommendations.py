from unittest.mock import patch

from trip_recommendations import period_months, recommend_trips


def _row(origin, destination, score, trend="steady"):
    return {"origin": origin, "destination": destination, "archive_score": score, "trend": trend}


def test_period_months_supports_month_and_season():
    assert period_months(4, "winter") == (4,)
    assert period_months(None, "winter") == (12, 1, 2)


def test_recommendations_include_direct_and_hub_with_weakest_leg_penalty():
    rows = [_row("Liverpool", "Rome", 80), _row("Liverpool", "Budapest", 90), _row("Budapest", "Cairo", 70)]
    rates = {("Liverpool", "Rome"): 80, ("Liverpool", "Budapest"): 90, ("Budapest", "Cairo"): 60}
    with patch("trip_recommendations._period_rates", return_value=rates):
        result = recommend_trips(rows, ["Liverpool"], ["Budapest"], month=7)
    assert any(r["is_direct"] and r["destination"] == "Rome" for r in result)
    connected = next(r for r in result if not r["is_direct"])
    assert connected["hub"] == "Budapest"
    assert connected["destination"] == "Cairo"
    assert connected["score"] < connected["legs"][1]["score"]


def test_recommendations_exclude_unconfigured_hubs():
    rows = [_row("Liverpool", "Rome", 90), _row("Rome", "Cairo", 90)]
    rates = {("Liverpool", "Rome"): 90, ("Rome", "Cairo"): 90}
    with patch("trip_recommendations._period_rates", return_value=rates):
        result = recommend_trips(rows, ["Liverpool"], ["Budapest"], season="summer")
    assert all(r["is_direct"] for r in result)


def test_london_airport_recommendation_uses_london_archive_rate():
    rows = [
        _row("London", "Budapest", 80),
        {"origin": "London Luton", "destination": "Budapest", "archive_score": None, "positive_checks": 1, "total_checks": 2},
    ]
    rates = {("London", "Budapest"): 75}
    with patch("trip_recommendations._period_rates", return_value=rates):
        result = recommend_trips(rows, ["London Luton"], ["Budapest"], month=7)
    trip = next(r for r in result if r["origin"] == "London Luton")
    assert trip["legs"][0]["historical_scope"] == "London-wide"
    assert trip["period_score"] == 75


def test_recommendations_apply_saved_destination_policy():
    rows = [_row("Liverpool", "Rome", 90), _row("Liverpool", "Cairo", 80)]
    rates = {("Liverpool", "Rome"): 90, ("Liverpool", "Cairo"): 80}
    with patch("trip_recommendations._period_rates", return_value=rates):
        only = recommend_trips(rows, ["Liverpool"], [], month=7, destination_mode="only", destinations=["Cairo"])
        excluded = recommend_trips(rows, ["Liverpool"], [], month=7, destination_mode="exclude", destinations=["Rome"])
    assert [trip["destination"] for trip in only] == ["Cairo"]
    assert [trip["destination"] for trip in excluded] == ["Cairo"]


def test_recommendations_filter_origin_and_journey_type():
    rows = [
        _row("London Luton", "Budapest", 90),
        _row("Budapest", "Cairo", 80),
        _row("Liverpool", "Rome", 85),
    ]
    rates = {("London", "Budapest"): 90, ("Budapest", "Cairo"): 80, ("Liverpool", "Rome"): 85}
    rows[0].update({"archive_origin": "London", "archive_destination": "Budapest", "historical_scope": "London-wide", "airport_evidence": "Confirmed by airport-specific AYCF flights"})
    with patch("trip_recommendations._period_rates", return_value=rates):
        connected = recommend_trips(rows, ["London Luton", "Liverpool"], ["Budapest"], month=7, origin_filter="London Luton", trip_type="connected")
        direct = recommend_trips(rows, ["London Luton", "Liverpool"], ["Budapest"], month=7, origin_filter="Liverpool", trip_type="direct")
    assert [(trip["origin"], trip["hub"], trip["destination"]) for trip in connected] == [("London Luton", "Budapest", "Cairo")]
    assert all(trip["origin"] == "Liverpool" and trip["is_direct"] for trip in direct)
