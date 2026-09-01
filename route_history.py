"""Persistent history of AYCF route/flight appearances, isolated from the live scan DB."""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from cache_db import ScanCacheDB


def history_db_path() -> str:
    explicit = os.environ.get("AYCF_HISTORY_DB", "").strip()
    if explicit:
        return os.path.expanduser(explicit)
    state = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf")))
    return str(state / "aycf_route_history.sqlite3")


def _connect(path: Optional[str] = None) -> sqlite3.Connection:
    target = path or history_db_path()
    Path(target).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript("""
      CREATE TABLE IF NOT EXISTS snapshots (
        pdf_run_id TEXT PRIMARY KEY,
        scanned_at TEXT NOT NULL,
        recorded_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS route_appearances (
        pdf_run_id TEXT NOT NULL,
        origin TEXT NOT NULL,
        destination TEXT NOT NULL,
        travel_date TEXT NOT NULL,
        flight_count INTEGER NOT NULL,
        fetched_at TEXT,
        PRIMARY KEY(pdf_run_id,origin,destination,travel_date)
      );
      CREATE TABLE IF NOT EXISTS flight_appearances (
        pdf_run_id TEXT NOT NULL,
        origin TEXT NOT NULL,
        destination TEXT NOT NULL,
        travel_date TEXT NOT NULL,
        flight_code TEXT NOT NULL,
        departure TEXT NOT NULL,
        arrival TEXT NOT NULL,
        physical_origin TEXT,
        physical_destination TEXT,
        fetched_at TEXT,
        PRIMARY KEY(pdf_run_id,origin,destination,travel_date,flight_code,departure)
      );
      CREATE INDEX IF NOT EXISTS idx_route_history_route ON route_appearances(origin,destination,travel_date);
      CREATE INDEX IF NOT EXISTS idx_flight_history_route ON flight_appearances(origin,destination,travel_date);
    """)
    return conn


def snapshot_latest_run(scan_db: Optional[ScanCacheDB] = None, path: Optional[str] = None) -> Dict[str, Any]:
    scan_db = scan_db or ScanCacheDB()
    run = scan_db.latest_completed_pdf_run()
    if not run:
        return {"ok": True, "skipped": True, "reason": "No completed scan yet"}
    run_id = str(run["run_id"])
    with _connect(path) as history:
        if history.execute("SELECT 1 FROM snapshots WHERE pdf_run_id=?", (run_id,)).fetchone():
            return {"ok": True, "skipped": True, "pdf_run_id": run_id, "reason": "Already recorded"}
        with scan_db.connect() as source:
            checks = source.execute(
                "SELECT origin,destination,travel_date,flight_count,fetched_at FROM route_checks WHERE pdf_run_id=?",
                (run_id,),
            ).fetchall()
            flights = source.execute(
                "SELECT origin,destination,travel_date,flight_code,departure,arrival,physical_origin,physical_destination,fetched_at FROM route_flights WHERE pdf_run_id=?",
                (run_id,),
            ).fetchall()
        history.executemany(
            "INSERT OR REPLACE INTO route_appearances(pdf_run_id,origin,destination,travel_date,flight_count,fetched_at) VALUES(?,?,?,?,?,?)",
            [(run_id,r["origin"],r["destination"],r["travel_date"],int(r["flight_count"] or 0),r["fetched_at"]) for r in checks],
        )
        history.executemany(
            "INSERT OR REPLACE INTO flight_appearances(pdf_run_id,origin,destination,travel_date,flight_code,departure,arrival,physical_origin,physical_destination,fetched_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            [(run_id,r["origin"],r["destination"],r["travel_date"],r["flight_code"],r["departure"],r["arrival"],r["physical_origin"],r["physical_destination"],r["fetched_at"]) for r in flights],
        )
        history.execute(
            "INSERT INTO snapshots(pdf_run_id,scanned_at,recorded_at) VALUES(?,?,?)",
            (run_id, str(run.get("scanned_at") or ""), datetime.now().astimezone().isoformat(timespec="seconds")),
        )
        history.commit()
    return {"ok": True, "skipped": False, "pdf_run_id": run_id, "route_checks": len(checks), "flights": len(flights)}


def stability_rows(path: Optional[str] = None, limit: int = 300) -> List[Dict[str, Any]]:
    with _connect(path) as conn:
        snapshot_count = int(conn.execute("SELECT COUNT(*) c FROM snapshots").fetchone()["c"])
        rows = conn.execute("""
          SELECT origin,destination,
                 COUNT(DISTINCT pdf_run_id) observed_scans,
                 SUM(CASE WHEN flight_count>0 THEN 1 ELSE 0 END) positive_checks,
                 COUNT(*) total_checks,
                 COUNT(DISTINCT CASE WHEN flight_count>0 THEN travel_date END) available_dates,
                 MAX(CASE WHEN flight_count>0 THEN fetched_at END) last_seen,
                 SUM(flight_count) flight_appearances
          FROM route_appearances
          GROUP BY origin,destination
          ORDER BY positive_checks DESC, flight_appearances DESC, origin, destination
          LIMIT ?
        """, (max(1,min(int(limit),2000)),)).fetchall()
    out=[]
    for row in rows:
        item=dict(row)
        total=max(1,int(item["total_checks"] or 0))
        item["availability_rate"]=round(100.0*int(item["positive_checks"] or 0)/total,1)
        item["snapshot_count"]=snapshot_count
        out.append(item)
    return out


def history_stats(path: Optional[str] = None) -> Dict[str, Any]:
    with _connect(path) as conn:
        return {
            "snapshots": int(conn.execute("SELECT COUNT(*) c FROM snapshots").fetchone()["c"]),
            "routes": int(conn.execute("SELECT COUNT(*) c FROM (SELECT 1 FROM route_appearances GROUP BY origin,destination)").fetchone()["c"]),
            "route_checks": int(conn.execute("SELECT COUNT(*) c FROM route_appearances").fetchone()["c"]),
            "flights": int(conn.execute("SELECT COUNT(*) c FROM flight_appearances").fetchone()["c"]),
            "db_path": history_db_path(),
        }
