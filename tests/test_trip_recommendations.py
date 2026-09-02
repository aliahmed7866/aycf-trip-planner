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
