import json
import tempfile
from pathlib import Path
from unittest import mock

import stability_cache


def test_legacy_cache_upgrade_materializes_airports_without_archive_rescore():
    with tempfile.TemporaryDirectory() as td:
        db = str(Path(td) / "history.sqlite3")
        rows = [{"origin": "London", "destination": "Budapest", "archive_score": 80.0, "recent_30d": 70.0, "trend": "steady"}]
        with stability_cache._connect(db) as conn:
            conn.execute(
                "INSERT INTO stability_materialized_cache(cache_key,generated_at,rows_json,stats_json,external_json) VALUES(?,?,?,?,?)",
                (stability_cache.CACHE_KEY, "2026-09-02T00:00:00+00:00", json.dumps(rows), "{}", "{}"),
            )
        evidence = [{"origin": "London Luton", "destination": "Budapest", "observed_scans": 1, "positive_checks": 1, "available_dates": 1, "flight_appearances": 1, "last_seen": "2026-09-02"}]
        with mock.patch.object(stability_cache, "airport_route_evidence", return_value=evidence), \
             mock.patch.object(stability_cache, "archive_scores", side_effect=AssertionError("must not rescore archive")):
            upgraded = stability_cache.upgrade_stability_cache(db)
        assert upgraded["schema_version"] == stability_cache.CACHE_SCHEMA_VERSION
        assert upgraded["rows"][0]["origin"] == "London Luton"
        assert upgraded["rows"][0]["historical_scope"] == "London-wide"


def test_current_cache_upgrade_is_read_only():
    payload = {"schema_version": stability_cache.CACHE_SCHEMA_VERSION, "rows": []}
    with mock.patch.object(stability_cache, "read_stability_cache", return_value=payload), \
         mock.patch.object(stability_cache, "airport_route_evidence") as evidence:
        assert stability_cache.upgrade_stability_cache("unused.sqlite3") is payload
    evidence.assert_not_called()
