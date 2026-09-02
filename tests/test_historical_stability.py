import csv
import tempfile
from datetime import date
from pathlib import Path

from historical_stability import archive_scores, external_stats, import_archive, route_intelligence, travel_context


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


def test_route_intelligence_builds_timeline_weekdays_and_improving_trend():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "archive"
        root.mkdir()
        db = str(Path(td) / "history.sqlite3")
        # Previous 30d: route appears once. Recent 30d: route appears on every snapshot.
        _write_day(root, "2026-07-10", [("A", "B")])
        _write_day(root, "2026-07-20", [])
        _write_day(root, "2026-08-05", [("A", "B")])
        _write_day(root, "2026-08-15", [("A", "B")])
        _write_day(root, "2026-08-25", [("A", "B")])
        _write_day(root, "2026-09-01", [("A", "B")])
        import_archive(str(root), days=90, path=db, today=date(2026, 9, 2))
        info = route_intelligence("A", "B", db)
        assert info is not None
        assert info["trend"] == "improving"
        assert info["recent_30d"] == 100.0
        assert len(info["heatmap"]) >= 5
        assert set(info["weekday_scores"]) == {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
        assert any(m["label"] == "Aug 2026" for m in info["monthly"])
