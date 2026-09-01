import tempfile
from datetime import date
from pathlib import Path

from cache_db import ScanCacheDB
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
