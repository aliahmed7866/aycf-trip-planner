import tempfile
from datetime import date, datetime
from pathlib import Path

from cache_db import ScanCacheDB
from scanner import Flight
from route_history import history_stats, snapshot_latest_run
from scan_scope import expand_scan_routes
import watch_service


def test_special_coverage_includes_both_pdf_directions():
    pairs = [
        ("Liverpool", "Budapest"),
        ("Budapest", "Liverpool"),
        ("Budapest", "Yerevan"),
        ("Yerevan", "Budapest"),
        ("Paris", "Yerevan"),
        ("Yerevan", "Paris"),
    ]
    scope = {"origins": ["Liverpool"], "destination_mode": "all", "destinations": [], "connection_hubs": ["Budapest"], "workers": 1}
    primary, hubs = expand_scan_routes(pairs, scope)
    assert ("Paris", "Yerevan") in primary
    assert ("Yerevan", "Paris") in primary
    assert ("Budapest", "Yerevan") in primary
    assert ("Yerevan", "Budapest") in primary


def test_any_date_watch_uses_all_available_dates_and_migrates_existing_db():
    with tempfile.TemporaryDirectory() as td:
        scan_path = str(Path(td) / "scan.sqlite3")
        watch_path = str(Path(td) / "watches.sqlite3")
        db = ScanCacheDB(scan_path)
        run_id = "run1"
        db.upsert_pdf_run(run_id, "2026-09-02T00:00:00", "2026-09-02", "2026-09-05", 1)
        with db.connect() as conn:
            conn.execute("INSERT INTO route_checks(pdf_run_id,origin,destination,travel_date,fetched_at,flight_count) VALUES(?,?,?,?,?,?)", (run_id,"Liverpool","Yerevan","2026-09-03","2026-09-02T01:00:00",1))
            conn.execute("INSERT INTO route_checks(pdf_run_id,origin,destination,travel_date,fetched_at,flight_count) VALUES(?,?,?,?,?,?)", (run_id,"Liverpool","Yerevan","2026-09-04","2026-09-02T01:00:00",0))
        db.mark_pdf_scanned(run_id)
        watch_id = watch_service.add_watch("Liverpool", "Yerevan", any_date=True, path=watch_path)
        watch = [w for w in watch_service.list_watches(watch_path) if w["id"] == watch_id][0]
        assert watch["any_date"] == 1
        assert watch_service.available_dates_for_watch(db, watch) == {date(2026, 9, 3)}


def test_route_history_keeps_each_completed_scan_snapshot():
    with tempfile.TemporaryDirectory() as td:
        scan_path = str(Path(td) / "scan.sqlite3")
        history_path = str(Path(td) / "history.sqlite3")
        db = ScanCacheDB(scan_path)
        run_id = "run1"
        db.upsert_pdf_run(run_id, "2026-09-02T00:00:00", "2026-09-02", "2026-09-05", 1)
        scan1 = db.start_scan(run_id)
        with db.connect() as conn:
            conn.execute("INSERT INTO route_checks(pdf_run_id,origin,destination,travel_date,fetched_at,flight_count) VALUES(?,?,?,?,?,?)", (run_id,"Liverpool","Yerevan","2026-09-03","2026-09-02T01:00:00",1))
        db.finish_scan(scan1, "completed", 1, 1, 1)
        db.mark_pdf_scanned(run_id)
        first = snapshot_latest_run(db, history_path)
        assert first["skipped"] is False

        scan2 = db.start_scan(run_id)
        with db.connect() as conn:
            conn.execute("UPDATE route_checks SET fetched_at=?,flight_count=? WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date=?", ("2026-09-02T03:00:00",0,run_id,"Liverpool","Yerevan","2026-09-03"))
        db.finish_scan(scan2, "completed", 1, 1, 0)
        second = snapshot_latest_run(db, history_path)
        assert second["skipped"] is False
        assert history_stats(history_path)["snapshots"] == 2


def test_uncovered_watch_preserves_existing_available_match():
    with tempfile.TemporaryDirectory() as td:
        scan_path = str(Path(td) / "scan.sqlite3")
        watch_path = str(Path(td) / "watches.sqlite3")
        db = ScanCacheDB(scan_path)
        run_id = "run-uncovered"
        db.upsert_pdf_run(run_id, "2026-09-03T00:00:00", "2026-09-03", "2026-09-06", 2)
        with db.connect() as conn:
            conn.execute("INSERT INTO route_checks(pdf_run_id,origin,destination,travel_date,fetched_at,flight_count) VALUES(?,?,?,?,?,?)", (run_id, "A", "X", "2026-09-03", "2026-09-03T01:00:00", 0))
            conn.execute("INSERT INTO route_checks(pdf_run_id,origin,destination,travel_date,fetched_at,flight_count) VALUES(?,?,?,?,?,?)", (run_id, "Y", "B", "2026-09-03", "2026-09-03T01:00:00", 0))
        db.mark_pdf_scanned(run_id)
        watch_id = watch_service.add_watch("A", "B", any_date=True, path=watch_path)
        with watch_service._connect(watch_path) as conn:
            conn.execute("INSERT INTO flight_watch_matches(watch_id,flight_date,first_seen_at,last_seen_at,available) VALUES(?,?,?,?,1)", (watch_id, "2026-09-04", "2026-09-03T01:00:00", "2026-09-03T01:00:00"))
        summary = watch_service.check_watches(db, notify=False, path=watch_path)
        watch = watch_service.list_watches(watch_path)[0]
    assert summary["uncovered"] == 1
    assert summary["errors"] == 0
    assert watch["active_matches"] == 1
    assert "Not covered by latest scan" in watch["last_error"]


def test_exact_london_airport_watch_filters_physical_flights():
    with tempfile.TemporaryDirectory() as td:
        db = ScanCacheDB(str(Path(td) / "scan.sqlite3"))
        run_id = "run-london"
        scope = {"origins": ["London Luton", "London Gatwick"], "destination_mode": "all", "destinations": [], "connection_hubs": []}
        db.upsert_pdf_run(run_id, "2026-09-03T00:00:00", "2026-09-03", "2026-09-06", 1, scope_id="scope", scope=scope)
        luton = Flight("London Luton", "Budapest", "W1", datetime(2026, 9, 3, 8), datetime(2026, 9, 3, 11), "08:00", "11:00")
        gatwick = Flight("London Gatwick", "Budapest", "W2", datetime(2026, 9, 4, 8), datetime(2026, 9, 4, 11), "08:00", "11:00")
        db.replace_route_check(run_id, "London", "Budapest", date(2026, 9, 3), [luton])
        db.replace_route_check(run_id, "London", "Budapest", date(2026, 9, 4), [gatwick])
        db.mark_pdf_scanned(run_id)
        watch = {"origin": "London Luton", "destination": "Budapest", "any_date": 1}
        available = watch_service.available_dates_for_watch(db, watch)
        options = watch_service.watch_city_options(db)
    assert available == {date(2026, 9, 3)}
    assert "London Luton" in options
    assert "London Gatwick" in options


def test_exact_london_airport_zero_result_is_valid_when_scope_proves_coverage():
    with tempfile.TemporaryDirectory() as td:
        db = ScanCacheDB(str(Path(td) / "scan.sqlite3"))
        run_id = "run-london-empty"
        scope = {"origins": ["London Luton"], "destination_mode": "all", "destinations": [], "connection_hubs": []}
        db.upsert_pdf_run(run_id, "2026-09-03T00:00:00", "2026-09-03", "2026-09-06", 1, scope_id="scope", scope=scope)
        db.replace_route_check(run_id, "London", "Budapest", date(2026, 9, 3), [])
        db.mark_pdf_scanned(run_id)
        watch = {"origin": "London Luton", "destination": "Budapest", "any_date": 1}
        assert watch_service.available_dates_for_watch(db, watch) == set()


def test_unselected_london_airport_is_reported_uncovered():
    with tempfile.TemporaryDirectory() as td:
        db = ScanCacheDB(str(Path(td) / "scan.sqlite3"))
        run_id = "run-london-scope"
        scope = {"origins": ["London Luton"], "destination_mode": "all", "destinations": [], "connection_hubs": []}
        db.upsert_pdf_run(run_id, "2026-09-03T00:00:00", "2026-09-03", "2026-09-06", 1, scope_id="scope", scope=scope)
        db.replace_route_check(run_id, "London", "Budapest", date(2026, 9, 3), [])
        db.mark_pdf_scanned(run_id)
        watch = {"origin": "London Stansted", "destination": "Budapest", "any_date": 1}
        try:
            watch_service.available_dates_for_watch(db, watch)
        except watch_service.WatchRouteNotCovered as exc:
            assert "London Stansted" in str(exc)
        else:
            raise AssertionError("an unscanned airport must not be treated as checked-empty")
