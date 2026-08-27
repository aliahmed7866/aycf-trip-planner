import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from scanner import Flight, combine_path


def default_db_path() -> str:
    return os.environ.get("AYCF_DB_PATH", "/data/aycf.sqlite3" if os.environ.get("RAILWAY_ENVIRONMENT") else "./cache/aycf.sqlite3")


class ScanCacheDB:
    def __init__(self, path: Optional[str] = None):
        self.path = path or default_db_path()
        parent = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(parent, exist_ok=True)
        self._init_schema()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self):
        with self.connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS pdf_runs (
                    run_id TEXT PRIMARY KEY,
                    generated_at TEXT NOT NULL,
                    departure_start TEXT,
                    departure_end TEXT,
                    route_count INTEGER NOT NULL,
                    scanned_at TEXT,
                    scope_id TEXT,
                    scope_json TEXT
                );
                CREATE TABLE IF NOT EXISTS scan_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pdf_run_id TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    route_day_checks INTEGER NOT NULL DEFAULT 0,
                    live_requests INTEGER NOT NULL DEFAULT 0,
                    flights_found INTEGER NOT NULL DEFAULT 0,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS route_flights (
                    pdf_run_id TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    travel_date TEXT NOT NULL,
                    flight_code TEXT NOT NULL,
                    departure TEXT NOT NULL,
                    arrival TEXT NOT NULL,
                    departure_text TEXT,
                    arrival_text TEXT,
                    duration TEXT,
                    fetched_at TEXT NOT NULL,
                    PRIMARY KEY (pdf_run_id, origin, destination, travel_date, flight_code, departure)
                );
                CREATE TABLE IF NOT EXISTS route_checks (
                    pdf_run_id TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    travel_date TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    flight_count INTEGER NOT NULL,
                    PRIMARY KEY (pdf_run_id, origin, destination, travel_date)
                );
                CREATE INDEX IF NOT EXISTS idx_route_flights_lookup ON route_flights(origin, destination, travel_date, pdf_run_id);
                CREATE INDEX IF NOT EXISTS idx_route_checks_date ON route_checks(travel_date, pdf_run_id);
                CREATE INDEX IF NOT EXISTS idx_scan_runs_pdf ON scan_runs(pdf_run_id, started_at);
                """
            )
            columns = {row["name"] for row in db.execute("PRAGMA table_info(pdf_runs)").fetchall()}
            if "scope_id" not in columns:
                db.execute("ALTER TABLE pdf_runs ADD COLUMN scope_id TEXT")
            if "scope_json" not in columns:
                db.execute("ALTER TABLE pdf_runs ADD COLUMN scope_json TEXT")

    def latest_pdf_run(self) -> Optional[Dict[str, Any]]:
        with self.connect() as db:
            row = db.execute("SELECT * FROM pdf_runs ORDER BY generated_at DESC, scanned_at IS NOT NULL DESC, scanned_at DESC, rowid DESC LIMIT 1").fetchone()
            return dict(row) if row else None

    def latest_completed_pdf_run(self) -> Optional[Dict[str, Any]]:
        with self.connect() as db:
            row = db.execute("SELECT * FROM pdf_runs WHERE scanned_at IS NOT NULL ORDER BY generated_at DESC, scanned_at DESC, rowid DESC LIMIT 1").fetchone()
            return dict(row) if row else None

    def get_pdf_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as db:
            row = db.execute("SELECT * FROM pdf_runs WHERE run_id=?", (run_id,)).fetchone()
            return dict(row) if row else None

    def upsert_pdf_run(self, run_id: str, generated_at: str, departure_start: Optional[str], departure_end: Optional[str], route_count: int, scope_id: Optional[str] = None, scope: Optional[dict] = None):
        scope_json = json.dumps(scope, sort_keys=True, ensure_ascii=False) if scope else None
        with self.connect() as db:
            db.execute(
                """INSERT INTO pdf_runs(run_id, generated_at, departure_start, departure_end, route_count, scope_id, scope_json)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(run_id) DO UPDATE SET generated_at=excluded.generated_at, departure_start=excluded.departure_start,
                   departure_end=excluded.departure_end, route_count=excluded.route_count, scope_id=excluded.scope_id, scope_json=excluded.scope_json""",
                (run_id, generated_at, departure_start, departure_end, int(route_count), scope_id, scope_json),
            )

    def mark_pdf_scanned(self, run_id: str):
        with self.connect() as db:
            db.execute("UPDATE pdf_runs SET scanned_at=? WHERE run_id=?", (datetime.utcnow().isoformat(), run_id))

    def scan_in_progress(self, pdf_run_id: str, stale_after_hours: int = 6) -> bool:
        cutoff = (datetime.utcnow() - timedelta(hours=stale_after_hours)).isoformat()
        with self.connect() as db:
            row = db.execute("SELECT 1 FROM scan_runs WHERE pdf_run_id=? AND status='running' AND started_at>=? ORDER BY id DESC LIMIT 1", (pdf_run_id, cutoff)).fetchone()
            return bool(row)

    def start_scan(self, pdf_run_id: str) -> int:
        with self.connect() as db:
            cur = db.execute("INSERT INTO scan_runs(pdf_run_id, started_at, status) VALUES(?,?,?)", (pdf_run_id, datetime.utcnow().isoformat(), "running"))
            return int(cur.lastrowid)

    def finish_scan(self, scan_id: int, status: str, route_day_checks: int, live_requests: int, flights_found: int, error: Optional[str] = None):
        with self.connect() as db:
            db.execute("UPDATE scan_runs SET completed_at=?, status=?, route_day_checks=?, live_requests=?, flights_found=?, error=? WHERE id=?", (datetime.utcnow().isoformat(), status, route_day_checks, live_requests, flights_found, error, scan_id))

    def route_checked(self, pdf_run_id: str, origin: str, destination: str, travel_day: date) -> bool:
        with self.connect() as db:
            row = db.execute("SELECT 1 FROM route_checks WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date=?", (pdf_run_id, origin, destination, travel_day.isoformat())).fetchone()
            return bool(row)

    def route_flight_count(self, pdf_run_id: str, origin: str, destination: str, travel_day: date) -> Optional[int]:
        """Return the persisted flight count for a checked route/day, or None if unchecked."""
        with self.connect() as db:
            row = db.execute(
                "SELECT flight_count FROM route_checks WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date=?",
                (pdf_run_id, origin, destination, travel_day.isoformat()),
            ).fetchone()
            return int(row["flight_count"]) if row else None

    def replace_route_check(self, pdf_run_id: str, origin: str, destination: str, travel_day: date, flights: Iterable[Flight]):
        rows = list(flights)
        now = datetime.utcnow().isoformat()
        day = travel_day.isoformat()
        with self.connect() as db:
            db.execute("DELETE FROM route_flights WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date=?", (pdf_run_id, origin, destination, day))
            for f in rows:
                db.execute("""INSERT OR REPLACE INTO route_flights
                       (pdf_run_id, origin, destination, travel_date, flight_code, departure, arrival, departure_text, arrival_text, duration, fetched_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""", (pdf_run_id, origin, destination, day, f.flight_code, f.departure.isoformat(), f.arrival.isoformat(), f.departure_text, f.arrival_text, f.duration, now))
            db.execute("""INSERT INTO route_checks(pdf_run_id, origin, destination, travel_date, fetched_at, flight_count)
                   VALUES(?,?,?,?,?,?) ON CONFLICT(pdf_run_id, origin,destination,travel_date) DO UPDATE SET
                   fetched_at=excluded.fetched_at, flight_count=excluded.flight_count""", (pdf_run_id, origin, destination, day, now, len(rows)))

    def get_flights(self, origin: str, destination: str, travel_day: date, pdf_run_id: Optional[str] = None) -> Optional[List[Flight]]:
        with self.connect() as db:
            if pdf_run_id is None:
                row = db.execute("SELECT run_id FROM pdf_runs ORDER BY generated_at DESC, scanned_at IS NOT NULL DESC, scanned_at DESC, rowid DESC LIMIT 1").fetchone()
                if not row:
                    return None
                pdf_run_id = row["run_id"]
            checked = db.execute("SELECT 1 FROM route_checks WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date=?", (pdf_run_id, origin, destination, travel_day.isoformat())).fetchone()
            if not checked:
                return None
            rows = db.execute("SELECT * FROM route_flights WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date=? ORDER BY departure", (pdf_run_id, origin, destination, travel_day.isoformat())).fetchall()
        return [Flight(origin=r["origin"], destination=r["destination"], flight_code=r["flight_code"], departure=datetime.fromisoformat(r["departure"]), arrival=datetime.fromisoformat(r["arrival"]), departure_text=r["departure_text"] or "", arrival_text=r["arrival_text"] or "", duration=r["duration"] or "") for r in rows]

    def stats(self) -> Dict[str, Any]:
        with self.connect() as db:
            pdf = db.execute("SELECT * FROM pdf_runs WHERE scanned_at IS NOT NULL ORDER BY generated_at DESC, scanned_at DESC, rowid DESC LIMIT 1").fetchone()
            if not pdf:
                pdf = db.execute("SELECT * FROM pdf_runs ORDER BY generated_at DESC, rowid DESC LIMIT 1").fetchone()
            scan = None
            if pdf:
                scan = db.execute("SELECT * FROM scan_runs WHERE pdf_run_id=? ORDER BY id DESC LIMIT 1", (pdf["run_id"],)).fetchone()
            checks = db.execute("SELECT COUNT(*) c FROM route_checks").fetchone()["c"]
            flights = db.execute("SELECT COUNT(*) c FROM route_flights").fetchone()["c"]
        result = {"pdf": dict(pdf) if pdf else None, "scan": dict(scan) if scan else None, "cached_checks": checks, "cached_flights": flights, "db_path": os.path.abspath(self.path)}
        if result["pdf"] and result["pdf"].get("scope_json"):
            try:
                result["pdf"]["scope"] = json.loads(result["pdf"]["scope_json"])
            except Exception:
                result["pdf"]["scope"] = None
        return result


class CachedFlightClient:
    def __init__(self, db: ScanCacheDB, pdf_run_id: Optional[str] = None):
        self.db = db
        self.pdf_run_id = pdf_run_id
        self.misses = 0

    def check(self, origin: str, destination: str, day: date) -> List[Flight]:
        rows = self.db.get_flights(origin, destination, day, self.pdf_run_id)
        if rows is None:
            self.misses += 1
            return []
        return rows


def cached_scan_itineraries(graph, db: ScanCacheDB, origin: str, destination: Optional[str], start_day: date, days: int = 4, max_stops: int = 1, min_transfer_minutes: int = 150, limit: int = 100, max_paths_per_day: int = 250, pdf_run_id: Optional[str] = None) -> Tuple[List[Dict[str, Any]], int]:
    client = CachedFlightClient(db, pdf_run_id=pdf_run_id)
    results: List[Dict[str, Any]] = []
    seen = set()
    for offset in range(days):
        day = start_day + timedelta(days=offset)
        for path in graph.paths(origin, destination, day, max_stops=max_stops, max_paths=max_paths_per_day):
            for combo in combine_path(client, path, day, min_transfer_minutes):
                key = tuple((leg["flight_code"], leg["departure"]) for leg in combo["legs"])
                if key in seen:
                    continue
                seen.add(key)
                combo["date"] = day.isoformat()
                combo["source"] = "morning-cache"
                results.append(combo)
                if len(results) >= limit:
                    results.sort(key=lambda r: r["legs"][0]["departure"])
                    return results, client.misses
    results.sort(key=lambda r: r["legs"][0]["departure"])
    return results, client.misses
