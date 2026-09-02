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
        snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
        scan_run_id INTEGER NOT NULL UNIQUE,
        pdf_run_id TEXT NOT NULL,
        scanned_at TEXT NOT NULL,
        recorded_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS route_appearances (
        snapshot_id INTEGER NOT NULL,
        pdf_run_id TEXT NOT NULL,
        origin TEXT NOT NULL,
        destination TEXT NOT NULL,
        travel_date TEXT NOT NULL,
        flight_count INTEGER NOT NULL,
        fetched_at TEXT,
        PRIMARY KEY(snapshot_id,origin,destination,travel_date)
      );
      CREATE TABLE IF NOT EXISTS flight_appearances (
        snapshot_id INTEGER NOT NULL,
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
        PRIMARY KEY(snapshot_id,origin,destination,travel_date,flight_code,departure)
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
    with scan_db.connect() as source:
        scan_run = source.execute(
            "SELECT id,completed_at FROM scan_runs WHERE pdf_run_id=? AND status='completed' ORDER BY id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        if not scan_run:
            return {"ok": True, "skipped": True, "pdf_run_id": run_id, "reason": "No completed scan run yet"}
        scan_run_id = int(scan_run["id"])
        checks = source.execute(
            "SELECT origin,destination,travel_date,flight_count,fetched_at FROM route_checks WHERE pdf_run_id=?",
            (run_id,),
        ).fetchall()
        flights = source.execute(
            "SELECT origin,destination,travel_date,flight_code,departure,arrival,physical_origin,physical_destination,fetched_at FROM route_flights WHERE pdf_run_id=?",
            (run_id,),
        ).fetchall()

    with _connect(path) as history:
        if history.execute("SELECT 1 FROM snapshots WHERE scan_run_id=?", (scan_run_id,)).fetchone():
            return {"ok": True, "skipped": True, "pdf_run_id": run_id, "scan_run_id": scan_run_id, "reason": "Already recorded"}
        cur = history.execute(
            "INSERT INTO snapshots(scan_run_id,pdf_run_id,scanned_at,recorded_at) VALUES(?,?,?,?)",
            (scan_run_id, run_id, str(scan_run["completed_at"] or run.get("scanned_at") or ""), datetime.now().astimezone().isoformat(timespec="seconds")),
        )
        snapshot_id = int(cur.lastrowid)
        history.executemany(
            "INSERT INTO route_appearances(snapshot_id,pdf_run_id,origin,destination,travel_date,flight_count,fetched_at) VALUES(?,?,?,?,?,?,?)",
            [(snapshot_id,run_id,r["origin"],r["destination"],r["travel_date"],int(r["flight_count"] or 0),r["fetched_at"]) for r in checks],
        )
        history.executemany(
            "INSERT INTO flight_appearances(snapshot_id,pdf_run_id,origin,destination,travel_date,flight_code,departure,arrival,physical_origin,physical_destination,fetched_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            [(snapshot_id,run_id,r["origin"],r["destination"],r["travel_date"],r["flight_code"],r["departure"],r["arrival"],r["physical_origin"],r["physical_destination"],r["fetched_at"]) for r in flights],
        )
        history.commit()
    return {"ok": True, "skipped": False, "pdf_run_id": run_id, "scan_run_id": scan_run_id, "snapshot_id": snapshot_id, "route_checks": len(checks), "flights": len(flights)}


def stability_rows(path: Optional[str] = None, limit: int = 300) -> List[Dict[str, Any]]:
    with _connect(path) as conn:
        snapshot_count = int(conn.execute("SELECT COUNT(*) c FROM snapshots").fetchone()["c"])
        rows = conn.execute("""
          SELECT origin,destination,
                 COUNT(DISTINCT snapshot_id) observed_scans,
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


def airport_route_evidence(path: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return exact-airport evidence preserved on positive local flights.

    Route checks retain the PDF's logical city pair. Flight rows retain the
    concrete airports returned by Wizz, which is the safe basis for splitting
    a historical London route into airport-specific display rows.
    """
    with _connect(path) as conn:
        rows = conn.execute("""
          SELECT COALESCE(NULLIF(physical_origin,''),origin) origin,
                 COALESCE(NULLIF(physical_destination,''),destination) destination,
                 COUNT(DISTINCT snapshot_id) observed_scans,
                 COUNT(DISTINCT snapshot_id || '|' || travel_date) positive_checks,
                 COUNT(DISTINCT travel_date) available_dates,
                 COUNT(*) flight_appearances,
                 MAX(fetched_at) last_seen
            FROM flight_appearances
           WHERE COALESCE(NULLIF(physical_origin,''),origin) <> origin
              OR COALESCE(NULLIF(physical_destination,''),destination) <> destination
           GROUP BY COALESCE(NULLIF(physical_origin,''),origin),
                    COALESCE(NULLIF(physical_destination,''),destination)
           ORDER BY flight_appearances DESC, origin, destination
        """).fetchall()
    return [dict(row) for row in rows]


def history_stats(path: Optional[str] = None) -> Dict[str, Any]:
    with _connect(path) as conn:
        return {
            "snapshots": int(conn.execute("SELECT COUNT(*) c FROM snapshots").fetchone()["c"]),
            "routes": int(conn.execute("SELECT COUNT(*) c FROM (SELECT 1 FROM route_appearances GROUP BY origin,destination)").fetchone()["c"]),
            "route_checks": int(conn.execute("SELECT COUNT(*) c FROM route_appearances").fetchone()["c"]),
            "flights": int(conn.execute("SELECT COUNT(*) c FROM flight_appearances").fetchone()["c"]),
            "db_path": history_db_path(),
        }
