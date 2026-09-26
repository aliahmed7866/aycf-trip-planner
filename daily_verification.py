"""Durable airport/date observations, reused only on their UTC verification day."""
from dataclasses import asdict
from datetime import datetime, timezone
import json
import threading

from airport_catalog import airport_code
from scanner import Flight


def verified_today(value):
    try:
        observed = datetime.fromisoformat(value)
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return observed <= now and observed.astimezone(timezone.utc).date() == now.date()
    except (TypeError, ValueError):
        return False


class DailyVerification:
    def __init__(self, db):
        self.db = db
        self._guard = threading.Lock()
        self._locks = {}
        with db.connect() as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS daily_airport_checks (
                origin TEXT, destination TEXT, travel_date TEXT,
                checked_at TEXT NOT NULL, flights_json TEXT NOT NULL,
                PRIMARY KEY(origin,destination,travel_date))''')
        self._import_verified_groups()

    @staticmethod
    def key(a, b, day):
        return (airport_code(a) or a, airport_code(b) or b, day.isoformat())

    def get(self, a, b, day):
        with self.db.connect() as conn:
            row = conn.execute('SELECT * FROM daily_airport_checks WHERE origin=? AND destination=? AND travel_date=?', self.key(a,b,day)).fetchone()
        if row is None or not verified_today(row['checked_at']):
            return None
        try:
            flights = []
            for item in json.loads(row['flights_json']):
                item['departure'] = datetime.fromisoformat(item['departure'])
                item['arrival'] = datetime.fromisoformat(item['arrival'])
                flights.append(Flight(**item))
            return row['checked_at'], flights
        except (TypeError, ValueError, KeyError):
            return None

    def save(self, a, b, day, flights, observed=None, *, only_if_missing=False):
        data = []
        for flight in flights:
            item = asdict(flight)
            item.update(departure=flight.departure.isoformat(), arrival=flight.arrival.isoformat())
            data.append(item)
        with self.db.connect() as conn:
            if only_if_missing:
                old = conn.execute('SELECT checked_at FROM daily_airport_checks WHERE origin=? AND destination=? AND travel_date=?', self.key(a,b,day)).fetchone()
                if old and verified_today(old['checked_at']):
                    return
            conn.execute('INSERT OR REPLACE INTO daily_airport_checks VALUES (?,?,?,?,?)',
                         (*self.key(a,b,day), observed or datetime.now(timezone.utc).isoformat(), json.dumps(data)))

    def check(self, client, a, b, day):
        # Overlapping city groups can contain the same concrete airport pair.
        with self._guard:
            lock = self._locks.setdefault(self.key(a,b,day), threading.Lock())
        with lock:
            retained = self.get(a,b,day)
            if retained is not None:
                return retained[1]
            flights = client.check(a,b,day)
            self.save(a,b,day,flights)
            return flights

    def group(self, pairs, day):
        observations = [self.get(a,b,day) for a,b in pairs]
        if not observations or any(row is None for row in observations):
            return None
        flights = {(f.flight_code,f.departure,f.arrival,f.origin,f.destination):f
                   for _,rows in observations for f in rows}
        return min(row[0] for row in observations), list(flights.values())

    def _import_verified_groups(self):
        # Upgrade existing complete metadata without guessing coverage of old
        # partial/legacy rows. Most recent proven airport observations win.
        with self.db.connect() as conn:
            rows = conn.execute('SELECT * FROM route_checks WHERE complete=1 AND request_pairs_json IS NOT NULL ORDER BY fetched_at DESC').fetchall()
        for row in rows:
            if not verified_today(row['fetched_at']):
                continue
            try:
                pairs = json.loads(row['request_pairs_json'])
                day = datetime.fromisoformat(row['travel_date']).date()
                flights = self.db.get_flights(row['origin'],row['destination'],day,row['pdf_run_id'])
                if flights is None or len(flights) != row['flight_count']:
                    continue
                # Legacy rows with no physical identities are not proof.
                info = self.db.route_check_info(row['pdf_run_id'],row['origin'],row['destination'],day)
                if not info or info['physical_missing']:
                    continue
                coverage = {self.key(a,b,day) for a,b in pairs}
                if any(self.key(f.origin,f.destination,day) not in coverage for f in flights):
                    continue
                for a,b in pairs:
                    kept = [f for f in flights if self.key(f.origin,f.destination,day) == self.key(a,b,day)]
                    self.save(a,b,day,kept,row['fetched_at'],only_if_missing=True)
            except (TypeError, ValueError):
                continue


def group_verified_today(db, run_id, origin, destination, day, pairs):
    info = db.route_check_info(run_id, origin, destination, day)
    if not info or info['physical_missing'] or not verified_today(info['fetched_at']):
        return None
    with db.connect() as conn:
        row = conn.execute('SELECT request_pairs_json FROM route_checks WHERE pdf_run_id=? AND origin=? AND destination=? AND travel_date=?', (run_id,origin,destination,day.isoformat())).fetchone()
    if row['request_pairs_json'] is not None:
        try:
            covered = {DailyVerification.key(a,b,day) for a,b in json.loads(row['request_pairs_json'])}
            if not {DailyVerification.key(a,b,day) for a,b in pairs} <= covered:
                return None
        except (ValueError, TypeError):
            return None
    return info
