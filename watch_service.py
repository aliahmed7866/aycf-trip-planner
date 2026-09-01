import json
import os
import shutil
import sqlite3
import subprocess
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from cache_db import ScanCacheDB
from scan_scope import normalize_name


NOTIFICATION_CHANNEL_ID = "aycf-flight-alerts"
NOTIFICATION_CHANNEL_NAME = "AYCF flight alerts"
ANY_DATE_FROM = "0001-01-01"
ANY_DATE_TO = "9999-12-31"


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


def _ensure_watch_columns(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(flight_watches)").fetchall()}
    if "any_date" not in columns:
        conn.execute("ALTER TABLE flight_watches ADD COLUMN any_date INTEGER NOT NULL DEFAULT 0")


def _ensure_match_columns(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(flight_watch_matches)").fetchall()}
    if "notification_attempted_at" not in columns:
        conn.execute("ALTER TABLE flight_watch_matches ADD COLUMN notification_attempted_at TEXT")
    if "notification_error" not in columns:
        conn.execute("ALTER TABLE flight_watch_matches ADD COLUMN notification_error TEXT")


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
            any_date INTEGER NOT NULL DEFAULT 0,
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
            notification_attempted_at TEXT,
            notification_error TEXT,
            UNIQUE(watch_id, flight_date)
        );
        CREATE INDEX IF NOT EXISTS idx_watch_matches_watch ON flight_watch_matches(watch_id, flight_date);
        """)
        _ensure_watch_columns(conn)
        _ensure_match_columns(conn)
    return path


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def add_watch(origin: str, destination: str, date_from: str = "", date_to: Optional[str] = None, any_date: bool = False, path: Optional[str] = None) -> int:
    origin = (origin or "").strip()
    destination = (destination or "").strip()
    if not origin or not destination:
        raise ValueError("Origin and destination are required.")
    if normalize_name(origin) == normalize_name(destination):
        raise ValueError("Origin and destination must be different.")
    if any_date:
        start_text, end_text = ANY_DATE_FROM, ANY_DATE_TO
    else:
        if not date_from:
            raise ValueError("Choose a date/date range, or enable Any date.")
        start = date.fromisoformat(date_from)
        end = date.fromisoformat(date_to or date_from)
        if end < start:
            raise ValueError("End date cannot be before start date.")
        if start < date.today():
            raise ValueError("Watch dates cannot be in the past.")
        if (end - start).days > 62:
            raise ValueError("A watch date range can be at most 63 days.")
        start_text, end_text = start.isoformat(), end.isoformat()
    path = init_watch_db(path)
    try:
        with _connect(path) as conn:
            cur = conn.execute(
                "INSERT INTO flight_watches(origin,destination,date_from,date_to,any_date,created_at) VALUES(?,?,?,?,?,?)",
                (origin, destination, start_text, end_text, 1 if any_date else 0, _now()),
            )
            return int(cur.lastrowid)
    except sqlite3.IntegrityError as exc:
        raise ValueError("That route watch already exists for the same date mode.") from exc


def list_watches(path: Optional[str] = None) -> List[Dict[str, Any]]:
    path = init_watch_db(path)
    with _connect(path) as conn:
        rows = conn.execute("""
          SELECT w.*,
                 COALESCE(SUM(CASE WHEN m.available=1 THEN 1 ELSE 0 END),0) active_matches,
                 MAX(CASE WHEN m.available=1 THEN m.flight_date END) latest_match_date,
                 COALESCE(SUM(CASE WHEN m.available=1 AND m.notified_at IS NULL THEN 1 ELSE 0 END),0) pending_notifications
          FROM flight_watches w
          LEFT JOIN flight_watch_matches m ON m.watch_id=w.id
          GROUP BY w.id ORDER BY w.enabled DESC,w.created_at DESC
        """).fetchall()
        return [dict(r) for r in rows]


def recent_matches(path: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    path = init_watch_db(path)
    with _connect(path) as conn:
        rows = conn.execute("""
          SELECT m.*,w.origin,w.destination,w.any_date FROM flight_watch_matches m
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
    params: List[Any] = [run_id, origin, destination]
    sql = "SELECT travel_date,flight_count FROM route_checks WHERE pdf_run_id=? AND origin=? AND destination=?"
    if not bool(watch.get("any_date")):
        sql += " AND travel_date BETWEEN ? AND ?"
        params.extend([str(watch["date_from"]), str(watch["date_to"])])
    sql += " ORDER BY travel_date"
    with db.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return {date.fromisoformat(r["travel_date"]) for r in rows if int(r["flight_count"] or 0) > 0}


def notification_status() -> Dict[str, Any]:
    enabled = os.environ.get("AYCF_NOTIFICATIONS", "true").lower() not in {"0", "false", "off", "no"}
    binary = shutil.which("termux-notification")
    channel_tool = shutil.which("termux-notification-channel")
    if not enabled:
        return {"ok": False, "enabled": False, "available": bool(binary), "detail": "Notifications are disabled by AYCF_NOTIFICATIONS."}
    if not binary:
        return {"ok": False, "enabled": True, "available": False, "detail": "termux-notification is unavailable. Install the Termux:API app and the termux-api package."}
    return {"ok": True, "enabled": True, "available": True, "channel_available": bool(channel_tool), "detail": "Termux notification bridge is installed. AYCF uses its own Android notification channel when supported."}


def _ensure_notification_channel() -> Tuple[bool, str]:
    binary = shutil.which("termux-notification-channel")
    if not binary:
        return True, "Custom notification channels are unavailable; using the default Termux channel."
    try:
        proc = subprocess.run([binary, NOTIFICATION_CHANNEL_ID, NOTIFICATION_CHANNEL_NAME], check=False, timeout=10, capture_output=True, text=True)
    except Exception as exc:
        return False, f"Unable to create the AYCF notification channel: {type(exc).__name__}: {exc}"
    if proc.returncode == 0:
        return True, f"Using Android channel '{NOTIFICATION_CHANNEL_NAME}'."
    detail = (proc.stderr or proc.stdout or f"termux-notification-channel exited {proc.returncode}").strip()
    return False, f"Unable to create the AYCF notification channel: {detail[:300]}"


def _android_notification_visible(title: str, content: str) -> Tuple[Optional[bool], str]:
    binary = shutil.which("termux-notification-list")
    if not binary:
        return None, "Notification-list verification is unavailable."
    time.sleep(0.4)
    try:
        proc = subprocess.run([binary], check=False, timeout=10, capture_output=True, text=True)
    except Exception as exc:
        return None, f"Notification-list verification failed: {type(exc).__name__}: {exc}"
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or f"termux-notification-list exited {proc.returncode}").strip()
        return None, f"Notification-list verification needs Android Notification Access: {detail[:220]}"
    try:
        payload = json.loads(proc.stdout or "[]")
    except Exception:
        return None, "Notification-list verification returned unreadable output."
    if isinstance(payload, dict):
        payload = payload.get("notifications") or payload.get("data") or [payload]
    if not isinstance(payload, list):
        return None, "Notification-list verification returned an unexpected response."
    for item in payload:
        if not isinstance(item, dict):
            continue
        item_title = str(item.get("title") or item.get("notificationTitle") or "")
        item_content = str(item.get("content") or item.get("text") or item.get("notificationContent") or "")
        if item_title == title and (not item_content or item_content == content):
            return True, "Android Notification Access reports the alert as active."
    return None, "The command was accepted, but Notification Access did not confirm the alert. This does not prove posting failed."


def _run_notification(title: str, content: str, notification_id: str) -> Tuple[bool, str]:
    status = notification_status()
    if not status["ok"]:
        return False, str(status["detail"])
    channel_ok, channel_detail = _ensure_notification_channel()
    if not channel_ok:
        return False, channel_detail
    cmd = [str(shutil.which("termux-notification")), "--id", str(notification_id), "--title", title, "--content", content, "--priority", "high", "--sound", "--vibrate", "200,120,200"]
    if shutil.which("termux-notification-channel"):
        cmd.extend(["--channel", NOTIFICATION_CHANNEL_ID])
    try:
        proc = subprocess.run(cmd, check=False, timeout=15, capture_output=True, text=True)
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or f"termux-notification exited {proc.returncode}").strip()
        return False, detail[:500]
    visible, verify_detail = _android_notification_visible(title, content)
    if visible is True:
        return True, f"Notification posted. {channel_detail} {verify_detail}"
    return True, f"Notification command accepted. {channel_detail} {verify_detail}"


def _watch_notification_id(watch_id: int, flight_date: date) -> str:
    return str(100000 + (int(watch_id) % 8000) * 100 + (flight_date.toordinal() % 100))


def send_termux_notification(origin: str, destination: str, flight_date: date, watch_id: int) -> Tuple[bool, str]:
    return _run_notification("AYCF flight available", f"{origin} → {destination} · {flight_date.strftime('%a %d %b %Y')}", _watch_notification_id(watch_id, flight_date))


def send_route_dates_notification(origin: str, destination: str, dates: List[date], watch_id: int) -> Tuple[bool, str]:
    labels = [d.strftime("%d %b") for d in sorted(dates)]
    preview = ", ".join(labels[:4])
    if len(labels) > 4:
        preview += f" +{len(labels)-4} more"
    return _run_notification("AYCF route available", f"{origin} → {destination} · {preview}", str(180000 + int(watch_id) % 8000))


def send_test_notification() -> Tuple[bool, str]:
    return _run_notification("AYCF notifications ready", "Test alert from your AYCF flight watcher.", "990001")


def check_watches(scan_db: Optional[ScanCacheDB] = None, notify: bool = True, path: Optional[str] = None) -> Dict[str, Any]:
    scan_db = scan_db or ScanCacheDB()
    path = init_watch_db(path)
    summary = {"checked":0,"new_matches":0,"notifications":0,"notification_failures":0,"errors":0}
    now = _now()
    with _connect(path) as conn:
        watches = [dict(r) for r in conn.execute("SELECT * FROM flight_watches WHERE enabled=1 ORDER BY id").fetchall()]
    for watch in watches:
        summary["checked"] += 1
        try:
            available = available_dates_for_watch(scan_db,watch)
            available_iso = {d.isoformat() for d in available}
            pending_group: List[date] = []
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
                    should_notify = notify and (newly or prior is None or not prior.get("notified_at"))
                    if should_notify and bool(watch.get("any_date")):
                        pending_group.append(d)
                    elif should_notify:
                        sent, detail = send_termux_notification(watch["origin"],watch["destination"],d,int(watch["id"]))
                        if sent:
                            summary["notifications"] += 1
                            conn.execute("UPDATE flight_watch_matches SET notified_at=?,notification_attempted_at=?,notification_error=NULL WHERE watch_id=? AND flight_date=?",(now,now,watch["id"],key))
                        else:
                            summary["notification_failures"] += 1
                            conn.execute("UPDATE flight_watch_matches SET notification_attempted_at=?,notification_error=? WHERE watch_id=? AND flight_date=?",(now,detail[:500],watch["id"],key))
                if pending_group:
                    sent, detail = send_route_dates_notification(watch["origin"], watch["destination"], pending_group, int(watch["id"]))
                    keys=[d.isoformat() for d in pending_group]
                    placeholders=",".join("?" for _ in keys)
                    if sent:
                        summary["notifications"] += 1
                        conn.execute(f"UPDATE flight_watch_matches SET notified_at=?,notification_attempted_at=?,notification_error=NULL WHERE watch_id=? AND flight_date IN ({placeholders})", [now,now,watch["id"],*keys])
                    else:
                        summary["notification_failures"] += 1
                        conn.execute(f"UPDATE flight_watch_matches SET notification_attempted_at=?,notification_error=? WHERE watch_id=? AND flight_date IN ({placeholders})", [now,detail[:500],watch["id"],*keys])
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
