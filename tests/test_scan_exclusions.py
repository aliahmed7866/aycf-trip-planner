"""Exclusions must prevent network jobs, not just hide their results."""
import json
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from flask import Flask

import morning_scan
import tiered_morning
from cache_db import ScanCacheDB
from parallel_fetch import ParallelFetcher
from recommendation_preferences import save_preferred_destinations, load_preferred_destinations
from scan_scope import (default_scope, load_scope, save_scope, expand_scan_routes, route_requests,
                        scope_fingerprint, scan_jobs, scan_plan, route_allowed)
from scan_settings import create_scan_settings_blueprint, exclusion_catalog, exclusion_preview
from scanner import Flight
from trip_recommendations import recommend_trips
from watch_service import available_dates_for_watch, WatchRouteNotCovered


@pytest.fixture(autouse=True)
def private_config(tmp_path, monkeypatch):
    monkeypatch.setenv("AYCF_CONFIG_DIR", str(tmp_path / "config"))
    monkeypatch.setenv("AYCF_WATCH_DB", str(tmp_path / "watches.sqlite3"))


@pytest.mark.parametrize("exclusions", [
    {"excluded_airports": ["Kutaisi"]}, {"excluded_airports": ["KUT"]},
    {"excluded_countries": ["Georgia"]},
    {"destination_mode": "exclude", "destinations": ["Kutaisi"]},
])
def test_exclusions_override_special_preferred_watched_and_reverse_coverage(exclusions):
    pairs = [("Liverpool", "Budapest"), ("Budapest", "Kutaisi"), ("Kutaisi", "Rome")]
    scope = dict(default_scope(), preferred_destinations=["Kutaisi"],
                 watch_routes=[("Kutaisi", "Budapest")], **exclusions)
    primary, hubs = expand_scan_routes(pairs, scope)
    assert ("Liverpool", "Budapest") in primary
    assert not any("Kutaisi" in pair for pair in primary + hubs)


def test_one_airport_does_not_exclude_its_siblings_or_reappear_as_group_fallback():
    scope = dict(default_scope(), excluded_airports=["LGW"])
    assert route_requests("London", "Rome", scope) == [("London Luton", "Rome"), ("London Stansted", "Rome")]
    assert route_requests("Rome", "London", scope) == [("Rome", "London Luton"), ("Rome", "London Stansted")]
    scope["excluded_airports"] = ["LGW", "LTN", "STN"]
    assert not route_requests("London", "Rome", scope)
    assert scan_plan([("London", "Rome")], scope)["request_units"] == 0


def test_city_and_country_exclude_all_airports():
    for change in ({"excluded_airports": ["London"]}, {"excluded_countries": ["United Kingdom"]}):
        scope = dict(default_scope(), **change)
        assert not route_requests("London Luton", "Rome", scope)
        assert not route_requests("Rome", "London", scope)


def test_route_pair_exclusion_is_both_directions_and_airport_specific():
    scope = dict(default_scope(), excluded_routes=[["LTN", "Rome"]])
    assert ("London Luton", "Rome") not in route_requests("London", "Rome", scope)
    assert ("Rome", "London Luton") not in route_requests("Rome", "London", scope)
    assert ("London Gatwick", "Rome") in route_requests("London", "Rome", scope)
    assert route_allowed("London Luton", "Budapest", scope)


def test_blocked_hub_ingress_does_not_generate_useless_normal_hub_jobs():
    scope = dict(default_scope(), origins=["Liverpool"], connection_hubs=["Rome"],
                 excluded_routes=[["Liverpool", "Rome"]])
    assert scan_plan([("Liverpool", "Rome"), ("Rome", "Athens")], scope)["routes"] == []


def test_save_clear_and_legacy_caller_preserve_preferences_and_exclusions():
    save_preferred_destinations(["Kutaisi"])
    saved = save_scope(["Liverpool"], "all", [], ["Budapest"], 5,
                       excluded_airports=["KUT"], excluded_countries=["Egypt"], excluded_routes=[["Rome", "Liverpool"]])
    assert load_scope() == saved
    save_scope(["Liverpool"], "all", [], ["Rome"], 5)
    assert load_scope()["excluded_airports"] == ["KUT"]
    cleared = save_scope(["Liverpool"], "all", [], [], 5,
                         excluded_airports=[], excluded_countries=[], excluded_routes=[])
    assert route_allowed("Liverpool", "Kutaisi", cleared)
    assert load_preferred_destinations() == ["Kutaisi"]


def test_fingerprint_changes_on_exclusions_and_is_order_case_independent():
    first = dict(default_scope(), excluded_airports=["Kutaisi", "Rome"], excluded_routes=[["Rome", "Liverpool"]])
    second = dict(default_scope(), excluded_airports=["rome", "kutaisi"], excluded_routes=[["liverpool", "rome"]])
    assert scope_fingerprint(first) == scope_fingerprint(second)
    assert scope_fingerprint(first) != scope_fingerprint(default_scope())
    assert scope_fingerprint(dict(default_scope(), excluded_countries=["Egypt"])) != scope_fingerprint(default_scope())


def test_preview_counts_exact_requests_in_both_directions():
    pairs = [("London", "Rome"), ("Rome", "London")]
    scope = dict(default_scope(), connection_hubs=[], excluded_routes=[["LTN", "Rome"]])
    result = exclusion_preview(pairs, scope)
    assert result["request_units"] == 16
    assert result["saved_requests"] == 8


def test_order_keeps_near_preferred_hub_legs_ahead_of_normal_and_future_jobs():
    today = date.today()
    plan = {"primary_routes": [("Liverpool", "Rome")], "hub_routes": [("Rome", "Nice")]}
    jobs = scan_jobs(plan, dict(default_scope(), preferred_destinations=["Nice"]), [today + timedelta(days=1), today])
    assert [(job[1], job[2], job[3]) for job in jobs] == [
        ("Rome", "Nice", today), ("Liverpool", "Rome", today),
        ("Rome", "Nice", today + timedelta(days=1)), ("Liverpool", "Rome", today + timedelta(days=1))]


@pytest.mark.parametrize("worker", [morning_scan, tiered_morning])
@pytest.mark.parametrize("unknown_return", [False, True])
def test_both_workers_never_request_excluded_routes_including_preflight(worker, tmp_path, monkeypatch, unknown_return):
    today = date.today()
    generated = datetime.combine(today, datetime.min.time())
    frame = pd.DataFrame([("London", "Rome"), ("Rome", "London"), ("Rome", "Kutaisi")],
                         columns=["departure_from", "departure_to"])
    scope = dict(default_scope(), connection_hubs=["Rome"], excluded_countries=["Georgia"],
                 excluded_routes=[["London Luton", "Rome"]], preferred_destinations=["Kutaisi"],
                 watch_routes=[("Kutaisi", "Rome")])
    calls, probes = [], []

    class Client:
        def __init__(self, *args, **kwargs):
            self.live_requests = self.no_availability_responses = self.wallet_redirects = self.html_retries = 0
            self.station_ids = {}
        def preflight(self, a, b, day):
            probes.append((a, b))
            return {"ok": True}
        def check(self, a, b, day):
            calls.append((a, b))
            if unknown_return and a == "Rome":
                raise morning_scan.WizzAvailabilityUnknown("Wallet route unknown")
            self.live_requests += 1
            return []

    monkeypatch.setenv("AYCF_SCAN_WORKERS", "1")
    monkeypatch.setattr(worker, "refresh_direct_snapshot", lambda *a: ("pdf", frame, generated, generated, generated))
    monkeypatch.setattr(worker, "_mirror_for_web", lambda *a: None)
    monkeypatch.setattr(worker, "load_scope", lambda: scope)
    monkeypatch.setattr(worker, "scan_scope_with_preferences", lambda value: value)
    monkeypatch.setattr(worker, "SessionVault", lambda: SimpleNamespace(load=lambda: {"cookies": []}))
    monkeypatch.setattr(worker, "CapturedRequestWizzClient", Client)
    monkeypatch.setattr(worker, "_apply_wizz_runtime", lambda client: True)
    monkeypatch.setattr(worker, "prepare_required_stations", lambda *a: {"resolved": 3, "required": 3, "unresolved": [], "aliases": 3})
    db = ScanCacheDB(str(tmp_path / "scan.sqlite3"))
    result = worker._run_locked(db)
    if unknown_return:
        assert result["state"] == "partial" and not result["ok"]
        assert result["unknown_checks"] == 1
        with db.connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM route_checks WHERE origin='Rome'").fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM route_checks WHERE origin='London'").fetchone()[0] == 1
            assert conn.execute("SELECT status FROM scan_runs ORDER BY id DESC LIMIT 1").fetchone()[0] == "partial"
        return
    assert result["ok"]
    assert len(calls) == 4
    assert set(calls) == {("London Gatwick", "Rome"), ("London Stansted", "Rome"),
                          ("Rome", "London Gatwick"), ("Rome", "London Stansted")}
    assert probes and all(pair in calls for pair in probes)


def test_parallel_fetcher_does_not_rebuild_cross_product_from_allowed_requests():
    client = Mock(live_requests=0, no_availability_responses=0, wallet_redirects=0, html_retries=0)
    client.check.return_value = []
    fetcher = ParallelFetcher(lambda: client)
    fetcher._job(("primary", "A", "B", date.today(), ["A1", "A2"], ["B1", "B2"], [("A1", "B1"), ("A2", "B2")]))
    assert [(call.args[0], call.args[1]) for call in client.check.call_args_list] == [("A1", "B1"), ("A2", "B2")]


def test_captured_preflight_rewrites_excluded_recorded_route():
    client = morning_scan.CapturedRequestWizzClient({"cookies": []})
    client.dynamic_url = "https://multipass.wizzair.com/test"
    client.captured_request_template = {"origin": "KUT", "destination": "LTN", "departure": "2020-01-01"}
    with patch.object(client, "resolve_station", side_effect=["LGW", "FCO"]), patch.object(client, "_send_and_decode", return_value={}) as send:
        client.preflight("London Gatwick", "Rome", date(2026, 9, 8))
    assert send.call_args.args[0] == {"origin": "LGW", "destination": "FCO", "departure": "2026-09-08"}


def test_unknown_and_disappeared_airports_remain_editable():
    scope = dict(default_scope(), excluded_airports=["Missing airport"], excluded_routes=[["Missing airport", "Rome"]])
    catalog = exclusion_catalog([("Liverpool", "Rome")], scope)
    assert any(item["name"] == "Missing airport" and item["excluded"] for item in catalog["countries"]["Other / unmapped"])
    assert any(item["excluded"] for item in catalog["routes"])


def settings_client(pairs):
    app = Flask(__name__, template_folder="../templates")
    app.secret_key = "test"
    app.add_url_rule("/", "index", lambda: "planner")
    app.add_url_rule("/flights", "all_flights", lambda: "flights")
    app.jinja_env.globals["csrf_token"] = lambda: "test"
    from flask import request
    app.register_blueprint(create_scan_settings_blueprint(lambda: (None, pairs, [], [], ""), lambda: request.form.get("csrf_token") == "test"))
    return app.test_client()


def test_settings_preview_is_read_only_and_save_keeps_other_settings():
    save_scope(["Liverpool"], "all", [], ["Rome"], 5)
    client = settings_client([("Liverpool", "Rome")])
    with patch("scan_settings.scan_scope_with_preferences", side_effect=lambda scope: scope):
        preview = client.post("/settings/scan-exclusions/preview", data={"csrf_token": "test", "excluded_countries": "Italy"})
        assert preview.status_code == 200 and preview.json["request_units"] == 0
        assert load_scope()["excluded_countries"] == []
        assert client.post("/settings/scan-exclusions", data={"excluded_countries": "Italy"}).status_code == 400
        assert client.post("/settings/scan-exclusions", data={"csrf_token": "test", "excluded_countries": "Italy"}).status_code == 302
    assert load_scope()["excluded_countries"] == ["Italy"]
    assert load_scope()["connection_hubs"] == ["Rome"] and load_scope()["workers"] == 5


def test_settings_render_saved_choices_and_can_reenable_missing_route():
    save_scope(["Liverpool"], "all", [], [], excluded_airports=["Missing airport"],
               excluded_routes=[["Missing airport", "Rome"]])
    client = settings_client([("Liverpool", "Rome")])
    with patch("scan_settings.scan_scope_with_preferences", side_effect=lambda scope: scope):
        response = client.get("/settings/scan-exclusions")
        assert response.status_code == 200
        assert b"Missing airport" in response.data and b"Other / unmapped" in response.data
        assert b'value="Missing airport" checked' in response.data
        assert client.post("/settings/scan-exclusions", data={"csrf_token": "test"}).status_code == 302
    assert not load_scope()["excluded_airports"] and not load_scope()["excluded_routes"]


def test_legacy_exclusions_are_visible_and_can_be_cleared_in_new_editor():
    save_scope(["Liverpool"], "exclude", ["Rome"], [])
    client = settings_client([("Liverpool", "Rome")])
    with patch("scan_settings.scan_scope_with_preferences", side_effect=lambda scope: scope):
        response = client.get("/settings/scan-exclusions")
        assert b'value="Rome" checked' in response.data
        client.post("/settings/scan-exclusions", data={"csrf_token": "test"})
    assert load_scope()["destination_mode"] == "all"
    assert route_allowed("Liverpool", "Rome", load_scope())


def test_invalid_submission_does_not_silently_remove_saved_exclusions():
    save_scope(["Liverpool"], "all", [], [], excluded_countries=["Italy"])
    client = settings_client([("Liverpool", "Rome")])
    with patch("scan_settings.scan_scope_with_preferences", side_effect=lambda scope: scope):
        response = client.post("/settings/scan-exclusions/preview", data={"csrf_token": "test", "excluded_airports": "Not in catalog"})
    assert response.status_code == 400
    assert load_scope()["excluded_countries"] == ["Italy"]


def test_stability_does_not_present_stale_airport_coverage_as_current():
    from stability_blueprint import _current_scan_observation
    save_scope(["London Luton", "London Gatwick"], "all", [], [], excluded_airports=["LTN"])
    db = Mock()
    db.latest_completed_pdf_run.return_value = {"run_id": "old", "scope_json": "{}"}
    assert _current_scan_observation(db, "London", "Rome")["pending_scope"]
    assert _current_scan_observation(db, "London Luton", "Rome")["excluded"]


def test_watch_excluded_now_cannot_notify_from_old_cache():
    save_scope(["Liverpool"], "all", [], [], excluded_countries=["Georgia"])
    db = Mock()
    with pytest.raises(WatchRouteNotCovered, match="Paused by scan exclusions"):
        available_dates_for_watch(db, {"origin": "Liverpool", "destination": "Kutaisi"})
    db.latest_completed_pdf_run.assert_not_called()


def test_partial_london_watch_waits_for_new_scope():
    save_scope(["London Luton", "London Gatwick"], "all", [], [], excluded_airports=["LTN"])
    db = Mock()
    db.latest_completed_pdf_run.return_value = {"run_id": "old", "scope_json": "{}"}
    with pytest.raises(WatchRouteNotCovered, match="waiting for a completed scan"):
        available_dates_for_watch(db, {"origin": "London", "destination": "Rome"})


def test_recommendations_remove_excluded_connection_leg_before_ranking():
    rows = [{"origin": a, "destination": b, "archive_score": 90} for a, b in [
        ("Liverpool", "Rome"), ("Rome", "Nice"), ("Liverpool", "Athens")]]
    scope = dict(default_scope(), excluded_routes=[["Rome", "Nice"]])
    with patch("trip_recommendations._period_rates", return_value={(r["origin"], r["destination"]): 90 for r in rows}):
        trips = recommend_trips(rows, ["Liverpool"], ["Rome"], month=7, scope=scope)
    assert trips and all(trip["destination"] != "Nice" for trip in trips)


def test_aliases_share_one_checked_airport_and_route_control():
    scope = dict(default_scope(), excluded_airports=["KUT"], excluded_routes=[["FCO", "LPL"]])
    catalog = exclusion_catalog([("Liverpool", "Rome"), ("Rome", "Kutaisi")], scope)
    kutaisi = catalog["countries"]["Georgia"]
    assert len(kutaisi) == 1 and kutaisi[0]["name"] == "Kutaisi" and kutaisi[0]["excluded"]
    matches = [row for row in catalog["routes"] if row["excluded"]]
    assert len(matches) == 1
    assert {matches[0]["origin"], matches[0]["destination"]} == {"Liverpool", "Rome"}


def test_exclusion_aliases_do_not_change_cache_identity_when_relabelled():
    scope = dict(default_scope(), excluded_airports=["KUT"], excluded_routes=[["LTN", "FCO"]])
    equivalent = dict(default_scope(), excluded_airports=["Kutaisi"], excluded_routes=[["Rome", "London Luton"]])
    assert scope_fingerprint(scope) == scope_fingerprint(equivalent)


@pytest.mark.parametrize("extra", [
    {"preferred_destinations": ["London Luton"]},
    {"watch_routes": [["London Luton", "Liverpool"]]},
])
def test_excluded_airport_interest_cannot_expand_its_city_siblings(extra):
    scope = dict(default_scope(), origins=["Liverpool"], destination_mode="only", destinations=["Rome"],
                 connection_hubs=[], excluded_airports=["LTN"], **extra)
    assert scan_plan([("London", "Liverpool")], scope)["routes"] == []


def test_airport_watch_cannot_invent_a_sibling_route_from_physical_pdf_edge():
    scope = dict(default_scope(), origins=["Liverpool"], destination_mode="only", destinations=["Rome"],
                 connection_hubs=[], watch_routes=[["London Luton", "Liverpool"]])
    assert scan_plan([("London Gatwick", "Liverpool")], scope)["routes"] == []


def test_airport_priority_matches_grouped_jobs_only_for_allowed_members():
    from scan_scope import route_priority
    scope = dict(default_scope(), preferred_destinations=["LTN"])
    assert route_priority("London", "Rome", scope) == 0
    scope["excluded_airports"] = ["LTN"]
    assert route_priority("London", "Rome", scope) == 3


def test_preview_matches_pdf_window_and_worker_override(monkeypatch):
    from scan_scope import scan_window, configured_workers
    frame = pd.DataFrame([{"availability_start": "2026-09-09T00:00:00", "availability_end": "2026-09-10T23:59:59"}])
    window = scan_window(frame)
    assert window["days"] == 2 and not window["estimated"]
    scope = dict(default_scope(), workers=5)
    assert exclusion_preview([("Liverpool", "Rome")], scope, window)["request_units"] == 4
    monkeypatch.setenv("AYCF_SCAN_WORKERS", "2")
    assert scan_plan([], scope)["workers"] == configured_workers(scope) == 2
    monkeypatch.setenv("AYCF_SCAN_WORKERS", "invalid")
    assert scan_plan([], scope)["workers"] == configured_workers(scope) == 5
    assert scan_window(None)["estimated"]


def test_stale_tab_cannot_overwrite_new_exclusions():
    from scan_scope import exclusions_fingerprint
    saved = save_scope(["Liverpool"], "all", [], [])
    revision = exclusions_fingerprint(saved)
    save_scope(["Liverpool"], "all", [], [], excluded_countries=["Italy"])
    client = settings_client([("Liverpool", "Rome")])
    with patch("scan_settings.scan_scope_with_preferences", side_effect=lambda scope: scope):
        response = client.post("/settings/scan-exclusions", data={"csrf_token": "test", "exclusion_revision": revision}, headers={"Accept": "application/json"})
    assert response.status_code == 409
    assert "another tab" in response.json["error"]
    assert load_scope()["excluded_countries"] == ["Italy"]


def test_json_save_accepts_current_revision_and_returns_reload_url():
    from scan_scope import exclusions_fingerprint
    saved = save_scope(["Liverpool"], "all", [], [])
    client = settings_client([("Liverpool", "Rome")])
    with patch("scan_settings.scan_scope_with_preferences", side_effect=lambda scope: scope):
        response = client.post("/settings/scan-exclusions", data={"csrf_token": "test", "exclusion_revision": exclusions_fingerprint(saved), "excluded_countries": "Italy"}, headers={"Accept": "application/json"})
    assert response.status_code == 200 and response.json["ok"]
    assert load_scope()["excluded_countries"] == ["Italy"]


@pytest.mark.parametrize("extra", [
    {"preferred_destinations": ["LTN"]},
    {"watch_routes": [["LTN", "Liverpool"]]},
])
def test_excluded_physical_pair_cannot_promote_or_expand_sibling(extra):
    from scan_scope import route_priority
    scope = dict(default_scope(), origins=["Liverpool"], destination_mode="only", destinations=["Rome"],
                 connection_hubs=[], excluded_routes=[["LTN", "Liverpool"]], **extra)
    assert route_priority("London", "Liverpool", scope) == 3
    assert scan_plan([("London", "Liverpool")], scope)["routes"] == []
