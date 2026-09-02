"""Materialized Stability page cache.

Heavy historical scoring is performed after scans/imports, never on page requests.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from historical_stability import archive_scores, external_stats
from route_history import history_db_path, history_stats, stability_rows

CACHE_KEY = "stability-page-v1"


def _connect(path: Optional[str] = None) -> sqlite3.Connection:
    target = path or history_db_path()
    Path(target).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS stability_materialized_cache (
             cache_key TEXT PRIMARY KEY,
             generated_at TEXT NOT NULL,
             rows_json TEXT NOT NULL,
             stats_json TEXT NOT NULL,
             external_json TEXT NOT NULL
           )"""
    )
    return conn


def _combined_rows(limit: int = 5000) -> List[Dict[str, Any]]:
    local = {(r["origin"], r["destination"]): dict(r) for r in stability_rows(limit=limit)}
    archive = {(r["origin"], r["destination"]): r for r in archive_scores(limit=limit)}
    rows: List[Dict[str, Any]] = []
    for key in set(local) | set(archive):
        item = local.get(key, {
            "origin": key[0],
            "destination": key[1],
            "observed_scans": 0,
            "positive_checks": 0,
            "total_checks": 0,
            "available_dates": 0,
            "last_seen": None,
            "flight_appearances": 0,
            "availability_rate": None,
        })
        hist = archive.get(key)
        item["archive"] = hist
        item["archive_score"] = hist["archive_score"] if hist else None
        item["recent_30d"] = hist["recent_30d"] if hist else None
        item["previous_30d"] = hist.get("previous_30d") if hist else None
        item["trend"] = hist.get("trend", "insufficient") if hist else "insufficient"
        rows.append(item)
    rows.sort(key=lambda r: (
        -(r["recent_30d"] if r["recent_30d"] is not None else -1),
        -(r["archive_score"] if r["archive_score"] is not None else -1),
        -(r["availability_rate"] if r["availability_rate"] is not None else -1),
        r["origin"], r["destination"],
    ))
    return rows[:limit]


def refresh_stability_cache(path: Optional[str] = None) -> Dict[str, Any]:
    rows = _combined_rows(limit=5000)
    stats = history_stats(path)
    external = external_stats(path)
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    with _connect(path) as conn:
        conn.execute(
            """INSERT INTO stability_materialized_cache(cache_key,generated_at,rows_json,stats_json,external_json)
               VALUES(?,?,?,?,?)
               ON CONFLICT(cache_key) DO UPDATE SET
                 generated_at=excluded.generated_at,
                 rows_json=excluded.rows_json,
                 stats_json=excluded.stats_json,
                 external_json=excluded.external_json""",
            (
                CACHE_KEY,
                generated_at,
                json.dumps(rows, separators=(",", ":")),
                json.dumps(stats, separators=(",", ":")),
                json.dumps(external, separators=(",", ":")),
            ),
        )
        conn.commit()
    return {"ok": True, "generated_at": generated_at, "rows": len(rows), "archive_days": int(external.get("snapshot_days", 0) or 0)}


def read_stability_cache(path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    with _connect(path) as conn:
        row = conn.execute("SELECT generated_at,rows_json,stats_json,external_json FROM stability_materialized_cache WHERE cache_key=?", (CACHE_KEY,)).fetchone()
    if not row:
        return None
    return {"generated_at": row["generated_at"], "rows": json.loads(row["rows_json"]), "stats": json.loads(row["stats_json"]), "external": json.loads(row["external_json"])}
