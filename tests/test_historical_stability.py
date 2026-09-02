import csv
import tempfile
from datetime import date
from pathlib import Path

from historical_stability import archive_scores, external_stats, import_archive, period_rates, refresh_period_rates, travel_context


def _write_day(root: Path, day: str, routes):
    path = root / f"{day}T07_00_00.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["departure_from", "departure_to", "availability_start", "availability_end", "data_generated"])
        writer.writeheader()
        for origin, destination in routes:
            writer.writerow({
                "departure_from": origin,
                "departure_to": destination,
                "availability_start": day,
                "availability_end": day,
                "data_generated": f"{day}T07:00:00",
            })


def test_import_archive_keeps_requested_window_and_scores_recent_days_more():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "archive"
        root.mkdir()
        db = str(Path(td) / "history.sqlite3")
        _write_day(root, "2026-08-01", [("A", "B"), ("C", "D")])
        _write_day(root, "2026-08-15", [("A", "B")])
        _write_day(root, "2026-09-01", [("A", "B")])
        result = import_archive(str(root), days=40, path=db, today=date(2026, 9, 2))
        assert result["snapshot_days"] == 3
        assert external_stats(db)["routes"] == 2
        scores = {(r["origin"], r["destination"]): r for r in archive_scores(db)}
        assert scores[("A", "B")]["archive_score"] > scores[("C", "D")]["archive_score"]
        assert scores[("A", "B")]["recent_30d"] == 100.0


def test_import_archive_excludes_older_files():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "archive"
        root.mkdir()
        db = str(Path(td) / "history.sqlite3")
        _write_day(root, "2025-01-01", [("Old", "Route")])
        _write_day(root, "2026-09-01", [("New", "Route")])
        result = import_archive(str(root), days=365, path=db, today=date(2026, 9, 2))
        assert result["snapshot_days"] == 1
        assert external_stats(db)["routes"] == 1


def test_peak_contexts_are_classified():
    assert travel_context(date(2026, 8, 1)) == "summer_peak"
    assert travel_context(date(2026, 12, 24)) == "christmas_new_year"
    assert travel_context(date(2026, 4, 5)) == "easter"


def _write_window(root: Path, snapshot_day: str, rows):
    path = root / f"{snapshot_day}T07_00_00.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["departure_from", "departure_to", "availability_start", "availability_end", "data_generated"])
        writer.writeheader()
        for origin, destination, start, end in rows:
            writer.writerow({
                "departure_from": origin,
                "departure_to": destination,
                "availability_start": start,
                "availability_end": end,
                "data_generated": f"{snapshot_day}T07:00:00",
            })


def test_period_rates_follow_advertised_travel_month_not_publication_month():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "archive"
        root.mkdir()
        db = str(Path(td) / "history.sqlite3")
        _write_window(root, "2026-01-10", [
            ("A", "Summer", "2026-07-01", "2026-07-04"),
            ("B", "Winter", "2026-01-11", "2026-01-14"),
        ])
        _write_window(root, "2026-02-10", [
            ("A", "Summer", "2026-07-20", "2026-07-23"),
            ("C", "Summer", "2026-07-20", "2026-07-23"),
        ])
        import_archive(str(root), days=100, path=db, today=date(2026, 3, 1))
        july = period_rates((7,), db)
        january = period_rates((1,), db)
    assert july[("A", "Summer")] == 100.0
    assert july[("C", "Summer")] == 50.0
    assert ("B", "Winter") not in july
    assert january[("B", "Winter")] == 100.0
    assert ("A", "Summer") not in january


def test_month_boundary_counts_once_in_season_denominator():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "archive"
        root.mkdir()
        db = str(Path(td) / "history.sqlite3")
        _write_window(root, "2026-06-01", [
            ("A", "B", "2026-06-30", "2026-07-02"),
            ("C", "D", "2026-07-01", "2026-07-02"),
        ])
        import_archive(str(root), days=30, path=db, today=date(2026, 6, 15))
        assert period_rates((6,), db)[("A", "B")] == 100.0
        july = period_rates((7,), db)
        summer = period_rates((6, 7, 8), db)
    assert july[("A", "B")] == 100.0
    assert july[("C", "D")] == 100.0
    assert summer[("A", "B")] == 100.0
    assert summer[("C", "D")] == 100.0


def test_materialized_period_rates_are_reused_when_archive_is_unchanged(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "archive"
        root.mkdir()
        db = str(Path(td) / "history.sqlite3")
        _write_window(root, "2026-01-01", [("A", "B", "2026-07-01", "2026-07-04")])
        import_archive(str(root), days=10, path=db, today=date(2026, 1, 2))
        refresh_period_rates(db)
        monkeypatch.setattr("historical_stability.refresh_period_rates", lambda _path=None: (_ for _ in ()).throw(AssertionError("must reuse materialized rates")))
        assert period_rates((7,), db)[("A", "B")] == 100.0
