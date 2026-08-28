import os
import shutil
import sqlite3
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from cache_db import ScanCacheDB
from scan_scope import normalize_name


def watch_db_path() -> str:
    explicit = os.environ.get("AYCF_WATCH_DB", "").strip()
    if explicit:
        return os.path.expanduser(explicit)
    state = os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf"))
    return str(Path(state) / "aycf_watches.sqlite3")


def _connect(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_watch_db(path: Optional[str] = None) -> str:
    path = path or watch_db_path()
    with _connect(path) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS flight_watches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin TEXT NOT NULL,
            destination TEXT NOT NULL,
            date_from TEXT NOT NULL,
            date_to TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            last_checked_at TEXT,
            last_error TEXT,
            UNIQUE(origin, destination, date_from, date_to)
        );
        CREATE TABLE IF NOT EXISTS flight_watch_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            watch_id INTEGER NOT NULL REFERENCES flight_watches(id) ON DELETE CASCADE,
            flight_date TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            available INTEGER NOT NULL DEFAULT 1,
            notified_at TEXT,
            UNIQUE(watch_id, flight_date)
        );
        CREATE INDEX IF NOT EXISTS idx_watch_matches_watch
          ON flight_watch_matches(watch_id, flight_date);
        """)
    return path


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def add_watch(origin: str, destination: str, date_from: str, date_to: Optional[str] = None, path: Optional[str] = None) -> int:
    origin = (origin or "").strip()
    destination = (destination or "").strip()
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to or date_from)
    if not origin or not destination:
        raise ValueError("Origin and destination are required.")
    if normalize_name(origin) == normalize_name(destination):
        raise ValueError("Origin and destination must be different.")
    if end < start:
        raise ValueError("End date cannot be before start date.")
    if start < date.today():
        raise ValueError("Watch dates cannot be in the past.")
    if (end - start).days > 62:
        raise ValueError("A watch date range can be at most 63 days.")
    path = init_watch_db(path)
    try:
        with _connect(path) as conn:
            cur = conn.execute(
                "INSERT INTO flight_watches(origin,destination,date_from,date_to,created_at) VALUES(?,?,?,?,?)",
                (origin, destination, start.isoformat(), end.isoformat(), _now()),
            )
            return int(cur.lastrowid)
    except sqlite3.IntegrityError as exc:
        raise ValueError("That route/date watch already exists.") from exc


def list_watches(path: Optional[str] = None) -> List[Dict[str, Any]]:
    path = init_watch_db(path)
    with _connect(path) as conn:
        rows = conn.execute("""
          SELECT w.*,
                 COALESCE(SUM(CASE WHEN m.available=1 THEN 1 ELSE 0 END),0) active_matches,
                 MAX(CASE WHEN m.available=1 THEN m.flight_date END) latest_match_date
          FROM flight_watches w
          LEFT JOIN flight_watch_matches m ON m.watch_id=w.id
          GROUP BY w.id ORDER BY w.enabled DESC,w.created_at DESC
        """).fetchall()
        return [dict(r) for r in rows]


def recent_matches(path: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    path = init_watch_db(path)
    with _connect(path) as conn:
        rows = conn.execute("""
          SELECT m.*,w.origin,w.destination FROM flight_watch_matches m
          JOIN flight_watches w ON w.id=m.watch_id
          WHERE m.available=1 ORDER BY m.last_seen_at DESC,m.id DESC LIMIT ?
        """, (max(1,min(int(limit),250)),)).fetchall()
        return [dict(r) for r in rows]


def set_watch_enabled(watch_id: int, enabled: bool, path: Optional[str] = None) -> bool:
    path = init_watch_db(path)
    with _connect(path) as conn:
        return conn.execute("UPDATE flight_watches SET enabled=? WHERE id=?", (1 if enabled else 0,watch_id)).rowcount > 0


def delete_watch(watch_id: int, path: Optional[str] = None) -> bool:
    path = init_watch_db(path)
    with _connect(path) as conn:
        return conn.execute("DELETE FROM flight_watches WHERE id=?", (watch_id,)).rowcount > 0


def _resolve_route_names(db: ScanCacheDB, run_id: str, origin: str, destination: str):
    with db.connect() as conn:
        rows = conn.execute("SELECT DISTINCT origin,destination FROM route_checks WHERE pdf_run_id=?", (run_id,)).fetchall()
    origins = sorted({r["origin"] for r in rows})
    destinations = sorted({r["destination"] for r in rows})
    origin_map = {normalize_name(v): v for v in origins}
    destination_map = {normalize_name(v): v for v in destinations}
    return origin_map.get(normalize_name(origin)), destination_map.get(normalize_name(destination))


def available_dates_for_watch(db: ScanCacheDB, watch: Dict[str, Any]) -> Set[date]:
    run = db.latest_completed_pdf_run()
    if not run:
        raise RuntimeError("No completed AYCF scan is available yet.")
    run_id = run["run_id"]
    origin, destination = _resolve_route_names(db, run_id, str(watch["origin"]), str(watch["destination"]))
    if not origin or not destination:
        return set()
    start = date.fromisoformat(str(watch["date_from"]))
    end = date.fromisoformat(str(watch["date_to"]))
    with db.connect() as conn:
        rows = conn.execute("""
          SELECT travel_date,flight_count FROM route_checks
          WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date BETWEEN ? AND ?
          ORDER BY travel_date
        """, (run_id,origin,destination,start.isoformat(),end.isoformat())).fetchall()
    return {date.fromisoformat(r["travel_date"]) for r in rows if int(r["flight_count"] or 0) > 0}


def send_termux_notification(origin: str, destination: str, flight_date: date, watch_id: int) -> bool:
    if os.environ.get("AYCF_NOTIFICATIONS", "true").lower() in {"0","false","off","no"}:
        return False
    binary = shutil.which("termux-notification")
    if not binary:
        return False
    cmd = [binary,"--id",f"aycf-watch-{watch_id}-{flight_date.isoformat()}","--title","AYCF flight available",
           "--content",f"{origin} → {destination} · {flight_date.strftime('%a %d %b %Y')}","--priority","high"]
    try:
        subprocess.run(cmd,check=True,timeout=15,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        return True
    except Exception:
        return False


def check_watches(scan_db: Optional[ScanCacheDB] = None, notify: bool = True, path: Optional[str] = None) -> Dict[str, Any]:
    scan_db = scan_db or ScanCacheDB()
    path = init_watch_db(path)
    summary = {"checked":0,"new_matches":0,"notifications":0,"errors":0}
    now = _now()
    with _connect(path) as conn:
        watches = [dict(r) for r in conn.execute("SELECT * FROM flight_watches WHERE enabled=1 ORDER BY id").fetchall()]
    for watch in watches:
        summary["checked"] += 1
        try:
            available = available_dates_for_watch(scan_db,watch)
            available_iso = {d.isoformat() for d in available}
            with _connect(path) as conn:
                existing = {r["flight_date"]:dict(r) for r in conn.execute("SELECT * FROM flight_watch_matches WHERE watch_id=?",(watch["id"],)).fetchall()}
                for d in sorted(available):
                    key=d.isoformat(); prior=existing.get(key); newly=prior is None or not bool(prior["available"])
                    if prior is None:
                        conn.execute("INSERT INTO flight_watch_matches(watch_id,flight_date,first_seen_at,last_seen_at,available) VALUES(?,?,?,?,1)",(watch["id"],key,now,now))
                    else:
                        conn.execute("UPDATE flight_watch_matches SET last_seen_at=?,available=1 WHERE id=?",(now,prior["id"]))
                    if newly:
                        summary["new_matches"] += 1
                        if notify and send_termux_notification(watch["origin"],watch["destination"],d,int(watch["id"])):
                            summary["notifications"] += 1
                            conn.execute("UPDATE flight_watch_matches SET notified_at=? WHERE watch_id=? AND flight_date=?",(now,watch["id"],key))
                for key,prior in existing.items():
                    if bool(prior["available"]) and key not in available_iso:
                        conn.execute("UPDATE flight_watch_matches SET available=0,last_seen_at=? WHERE id=?",(now,prior["id"]))
                conn.execute("UPDATE flight_watches SET last_checked_at=?,last_error=NULL WHERE id=?",(now,watch["id"]))
        except Exception as exc:
            summary["errors"] += 1
            with _connect(path) as conn:
                conn.execute("UPDATE flight_watches SET last_checked_at=?,last_error=? WHERE id=?",(now,f"{type(exc).__name__}: {exc}"[:500],watch["id"]))
    return summary


def watch_city_options(scan_db: Optional[ScanCacheDB] = None) -> List[str]:
    scan_db = scan_db or ScanCacheDB()
    run = scan_db.latest_completed_pdf_run() or scan_db.latest_pdf_run()
    if not run:
        return []
    with scan_db.connect() as conn:
        rows = conn.execute("SELECT origin,destination FROM route_checks WHERE pdf_run_id=?",(run["run_id"],)).fetchall()
    return sorted({x for r in rows for x in (r["origin"],r["destination"]) if x})
