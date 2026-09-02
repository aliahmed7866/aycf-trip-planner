import csv
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import patch

import historical_stability
from historical_stability import import_archive, route_intelligence


def _write_day(root: Path, day: str, routes):
    path = root / f"{day}T07_00_00.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["departure_from", "departure_to", "availability_start", "availability_end", "data_generated"])
        writer.writeheader()
        for origin, destination in routes:
            writer.writerow({"departure_from": origin, "departure_to": destination, "availability_start": day, "availability_end": day, "data_generated": f"{day}T07:00:00"})


def test_route_intelligence_builds_timeline_without_rescoring_archive():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "archive"
        root.mkdir()
        db = str(Path(td) / "history.sqlite3")
        _write_day(root, "2026-07-01", [("A", "B"), ("C", "D")])
        _write_day(root, "2026-08-01", [("A", "B")])
        _write_day(root, "2026-09-01", [("A", "B")])
        import_archive(str(root), days=90, path=db, today=date(2026, 9, 2))
        with patch.object(historical_stability, "archive_scores", side_effect=AssertionError("must not rescore all routes")):
            result = route_intelligence("A", "B", path=db)
        assert result is not None
        assert result["origin"] == "A"
        assert result["destination"] == "B"
        assert len(result["heatmap"]) == 3
        assert result["observed_days"] == 3
        assert set(result["weekday_scores"]) == {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
        assert result["monthly"]


def test_route_intelligence_missing_route_returns_none():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "archive"
        root.mkdir()
        db = str(Path(td) / "history.sqlite3")
        _write_day(root, "2026-09-01", [("A", "B")])
        import_archive(str(root), days=30, path=db, today=date(2026, 9, 2))
        assert route_intelligence("X", "Y", path=db) is None
