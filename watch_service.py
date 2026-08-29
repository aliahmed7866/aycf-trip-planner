import math
import os
import shutil
import sqlite3
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from dateutil import parser as dtparser

from planner import normalise_city


DATE_COLUMNS = ("flight_date", "departure_date", "date", "availability_date")


def watch_db_path(cache_root: str) -> str:
    explicit = os.environ.get("AYCF_WATCH_DB", "").strip()
    if explicit:
        return os.path.expanduser(explicit)
    return os.path.join(cache_root, "aycf_watches.sqlite3")


def _connect(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_watch_db(path: str) -> None:
    with _connect(path) as conn:
        conn.executescript(
            """
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
            """
        )


def _iso_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def add_watch(path: str, origin: str, destination: str, date_from: str, date_to: Optional[str] = None) -> int:
    origin = normalise_city(origin)
    destination = normalise_city(destination)
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to or date_from)
    if not origin or not destination:
        raise ValueError("Origin and destination are required.")
    if origin == destination:
        raise ValueError("Origin and destination must be different.")
    if end < start:
        raise ValueError("End date cannot be before start date.")
    if (end - start).days > 62:
        raise ValueError("A watch date range can be at most 63 days.")

    init_watch_db(path)
    with _connect(path) as conn:
        try:
            cur = conn.execute(
                """INSERT INTO flight_watches
                   (origin, destination, date_from, date_to, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (origin, destination, start.isoformat(), end.isoformat(), _iso_now()),
            )
            return int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ValueError("That route/date watch already exists.") from exc


def list_watches(path: str) -> List[Dict[str, Any]]:
    init_watch_db(path)
    with _connect(path) as conn:
        rows = conn.execute(
            """
            SELECT w.*,
                   COALESCE(SUM(CASE WHEN m.available = 1 THEN 1 ELSE 0 END), 0) AS active_matches,
                   MAX(CASE WHEN m.available = 1 THEN m.flight_date END) AS latest_match_date
            FROM flight_watches w
            LEFT JOIN flight_watch_matches m ON m.watch_id = w.id
            GROUP BY w.id
            ORDER BY w.enabled DESC, w.created_at DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def recent_matches(path: str, limit: int = 50) -> List[Dict[str, Any]]:
    init_watch_db(path)
    with _connect(path) as conn:
        rows = conn.execute(
            """
            SELECT m.*, w.origin, w.destination
            FROM flight_watch_matches m
            JOIN flight_watches w ON w.id = m.watch_id
            WHERE m.available = 1
            ORDER BY m.last_seen_at DESC, m.id DESC
            LIMIT ?
            """,
            (max(1, min(int(limit), 250)),),
        ).fetchall()
        return [dict(r) for r in rows]


def set_watch_enabled(path: str, watch_id: int, enabled: bool) -> bool:
    init_watch_db(path)
    with _connect(path) as conn:
        cur = conn.execute("UPDATE flight_watches SET enabled = ? WHERE id = ?", (1 if enabled else 0, watch_id))
        return cur.rowcount > 0


def delete_watch(path: str, watch_id: int) -> bool:
    init_watch_db(path)
    with _connect(path) as conn:
        cur = conn.execute("DELETE FROM flight_watches WHERE id = ?", (watch_id,))
        return cur.rowcount > 0


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return dtparser.parse(text).date()
    except Exception:
        return None


def _row_dates(row: Dict[str, Any], start: date, end: date) -> Set[date]:
    for col in DATE_COLUMNS:
        if col in row:
            d = _as_date(row.get(col))
            if d is not None:
                return {d} if start <= d <= end else set()

    if "availability_start" in row or "availability_end" in row:
        row_start = _as_date(row.get("availability_start"))
        row_end = _as_date(row.get("availability_end"))
        if row_start is None and row_end is None:
            return set()
        row_start = row_start or row_end
        row_end = row_end or row_start
        lo = max(start, row_start)
        hi = min(end, row_end)
        if hi < lo:
            return set()
        return {lo + timedelta(days=i) for i in range((hi - lo).days + 1)}

    return set()


def _latest_route_rows(rows: Iterable[Dict[str, Any]], origin: str, destination: str) -> List[Dict[str, Any]]:
    """Return route rows from the newest dataset snapshot without pandas.

    Planner rows already contain parsed ``run_ts`` values and ``source_file``.
    Track the newest global snapshot while streaming, retaining only rows for
    the requested route. This preserves the previous latest-snapshot behaviour
    without materialising the full historical dataset in memory.
    """
    latest_ts: Optional[datetime] = None
    latest_file: Optional[str] = None
    route_rows_by_ts: List[Dict[str, Any]] = []
    route_rows_by_file: List[Dict[str, Any]] = []
    saw_timestamp = False

    for row in rows:
        run_ts = row.get("run_ts")
        source_file = str(row.get("source_file") or "")
        matches = (
            normalise_city(str(row.get("departure_from") or "").strip()) == origin
            and normalise_city(str(row.get("departure_to") or "").strip()) == destination
        )

        if isinstance(run_ts, datetime):
            saw_timestamp = True
            if latest_ts is None or run_ts > latest_ts:
                latest_ts = run_ts
                route_rows_by_ts = [row] if matches else []
            elif run_ts == latest_ts and matches:
                route_rows_by_ts.append(row)

        if source_file:
            if latest_file is None or source_file > latest_file:
                latest_file = source_file
                route_rows_by_file = [row] if matches else []
            elif source_file == latest_file and matches:
                route_rows_by_file.append(row)

    return route_rows_by_ts if saw_timestamp else route_rows_by_file


def available_dates_for_watch(planner: Any, watch: Dict[str, Any]) -> Set[date]:
    origin = normalise_city(str(watch["origin"]))
    destination = normalise_city(str(watch["destination"]))
    start = date.fromisoformat(str(watch["date_from"]))
    end = date.fromisoformat(str(watch["date_to"]))

    found: Set[date] = set()
    for row in _latest_route_rows(planner._load_runs(), origin, destination):
        found.update(_row_dates(row, start, end))
    return found


def send_termux_notification(origin: str, destination: str, flight_date: date, watch_id: int) -> bool:
    if os.environ.get("AYCF_NOTIFICATIONS", "true").lower() in {"0", "false", "off", "no"}:
        return False
    binary = shutil.which("termux-notification")
    if not binary:
        return False
    title = "AYCF flight available"
    content = f"{origin} → {destination} · {flight_date.strftime('%a %d %b %Y')}"
    cmd = [
        binary,
        "--id", f"aycf-watch-{watch_id}-{flight_date.isoformat()}",
        "--title", title,
        "--content", content,
        "--priority", "high",
    ]
    try:
        subprocess.run(cmd, check=True, timeout=15, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except Exception:
        return False


def check_watches(path: str, planner: Any, notify: bool = True) -> Dict[str, Any]:
    init_watch_db(path)
    summary = {"checked": 0, "new_matches": 0, "notifications": 0, "errors": 0}
    now = _iso_now()

    with _connect(path) as conn:
        watches = [dict(r) for r in conn.execute("SELECT * FROM flight_watches WHERE enabled = 1 ORDER BY id").fetchall()]

    for watch in watches:
        summary["checked"] += 1
        try:
            available = available_dates_for_watch(planner, watch)
            available_iso = {d.isoformat() for d in available}
            with _connect(path) as conn:
                existing = {
                    str(r["flight_date"]): dict(r)
                    for r in conn.execute("SELECT * FROM flight_watch_matches WHERE watch_id = ?", (watch["id"],)).fetchall()
                }

                for flight_date in sorted(available):
                    key = flight_date.isoformat()
                    prior = existing.get(key)
                    newly_available = prior is None or not bool(prior["available"])
                    if prior is None:
                        conn.execute(
                            """INSERT INTO flight_watch_matches
                               (watch_id, flight_date, first_seen_at, last_seen_at, available)
                               VALUES (?, ?, ?, ?, 1)""",
                            (watch["id"], key, now, now),
                        )
                    else:
                        conn.execute(
                            "UPDATE flight_watch_matches SET last_seen_at = ?, available = 1 WHERE id = ?",
                            (now, prior["id"]),
                        )

                    if newly_available:
                        summary["new_matches"] += 1
                        sent = notify and send_termux_notification(
                            watch["origin"], watch["destination"], flight_date, int(watch["id"])
                        )
                        if sent:
                            summary["notifications"] += 1
                            conn.execute(
                                "UPDATE flight_watch_matches SET notified_at = ? WHERE watch_id = ? AND flight_date = ?",
                                (now, watch["id"], key),
                            )

                for key, prior in existing.items():
                    if bool(prior["available"]) and key not in available_iso:
                        conn.execute(
                            "UPDATE flight_watch_matches SET available = 0, last_seen_at = ? WHERE id = ?",
                            (now, prior["id"]),
                        )

                conn.execute(
                    "UPDATE flight_watches SET last_checked_at = ?, last_error = NULL WHERE id = ?",
                    (now, watch["id"]),
                )
        except Exception as exc:
            summary["errors"] += 1
            with _connect(path) as conn:
                conn.execute(
                    "UPDATE flight_watches SET last_checked_at = ?, last_error = ? WHERE id = ?",
                    (now, f"{type(exc).__name__}: {exc}"[:500], watch["id"]),
                )

    return summary
