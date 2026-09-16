"""Separate, quota-limited storage for optional Manchester feeder fare checks.

Reading this store never performs a network request. Reservations and query
leases are committed before contacting the provider so different web workers
share the same conservative allowance, including failed requests.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import uuid

from dateutil import tz

from feeder_provider import ProviderError
from feeder_trips import normalize_offer


LIMIT_31D = 220
LIMIT_24H = 8
AUTOMATIC_LIMIT_24H = 6
CACHE_SECONDS = 60 * 60
ERROR_COOLDOWN_SECONDS = 5 * 60
AUTOMATIC_CACHE_SECONDS = 6 * 60 * 60
AUTOMATIC_EMPTY_SECONDS = 24 * 60 * 60
LEASE_SECONDS = 2 * 60
PROVIDER_SOURCE = "Google Flights via SerpApi"
MANCHESTER_ZONE = tz.gettz("Europe/London")


def _utc_now(now=None):
    if now is None:
        return datetime.now(timezone.utc)
    if isinstance(now, (float, int)):
        return datetime.fromtimestamp(now, timezone.utc)
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("The current time must include a timezone.")
    return now.astimezone(timezone.utc)


def _query(hub, travel_date):
    hub = str(hub).strip().upper()
    if not re.fullmatch(r"[A-Z]{3}", hub) or hub == "MAN":
        raise ValueError("Choose a destination airport other than Manchester.")
    travel_date = str(travel_date)
    if date.fromisoformat(travel_date).isoformat() != travel_date:
        raise ValueError("Use a date in YYYY-MM-DD format.")
    return hub, travel_date, f"MAN:{hub}:{travel_date}"


def _offer_id(offer, scope):
    identity = [scope] + [offer.get(key, "") for key in (
        "origin", "destination", "airline", "flight_number", "departure", "arrival", "source"
    )]
    return hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()[:32]


def _departure_date(offer):
    return datetime.fromisoformat(offer["departure"]).astimezone(MANCHESTER_ZONE).date().isoformat()


class FeederStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS offers (
                    id TEXT PRIMARY KEY,
                    origin TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    departure_date TEXT NOT NULL,
                    source TEXT NOT NULL,
                    provider_query_key TEXT,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS offers_query ON offers(provider_query_key);
                CREATE TABLE IF NOT EXISTS searches (
                    query_key TEXT PRIMARY KEY,
                    hub TEXT NOT NULL,
                    travel_date TEXT NOT NULL,
                    state TEXT NOT NULL,
                    checked_at TEXT,
                    checked_epoch REAL,
                    message TEXT NOT NULL,
                    lease_token TEXT,
                    lease_until REAL
                );
                CREATE TABLE IF NOT EXISTS requests (
                    id INTEGER PRIMARY KEY,
                    query_key TEXT NOT NULL,
                    reserved_at REAL NOT NULL,
                    automatic INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS requests_time ON requests(reserved_at);
                CREATE TABLE IF NOT EXISTS automation (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    enabled INTEGER NOT NULL DEFAULT 1,
                    last_checked TEXT,
                    last_batch_epoch REAL,
                    next_target_epoch REAL,
                    state TEXT NOT NULL DEFAULT 'waiting',
                    message TEXT NOT NULL DEFAULT 'Automatic fare checks are ready when a key and onward flights are available.',
                    checked_count INTEGER NOT NULL DEFAULT 0,
                    selected TEXT NOT NULL DEFAULT '[]'
                );
                INSERT OR IGNORE INTO automation(id) VALUES (1);
            """)
            # Existing manually reserved requests retain their original budgets.
            # Serialize the migration when workers open an older database together.
            connection.execute("BEGIN IMMEDIATE")
            columns = {row['name'] for row in connection.execute('PRAGMA table_info(requests)')}
            if 'automatic' not in columns:
                connection.execute('ALTER TABLE requests ADD COLUMN automatic INTEGER NOT NULL DEFAULT 0')

    @contextmanager
    def _connection(self):
        connection = sqlite3.connect(self.path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=15000")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    @staticmethod
    def _put_offer(connection, offer, query_key=None):
        offer = dict(offer)
        offer_id = _offer_id(offer, query_key or "manual")
        offer["id"] = offer_id
        connection.execute("""
            INSERT INTO offers(id, origin, destination, departure_date, source, provider_query_key, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET payload=excluded.payload
        """, (offer_id, offer["origin"], offer["destination"], _departure_date(offer),
              offer["source"], query_key, json.dumps(offer)))
        return offer

    def save_offer(self, offer):
        """Save a manually supplied quote without changing its observation time."""
        normalized = normalize_offer(offer)
        with self._connection() as connection:
            return self._put_offer(connection, normalized)

    def list_offers(self):
        with self._connection() as connection:
            rows = connection.execute("SELECT payload FROM offers ORDER BY departure_date, id").fetchall()
        return [json.loads(row["payload"]) for row in rows]

    def delete_offer(self, offer_id):
        with self._connection() as connection:
            return bool(connection.execute("DELETE FROM offers WHERE id = ?", (str(offer_id),)).rowcount)

    @staticmethod
    def _usage(connection, timestamp):
        row = connection.execute("""
            SELECT COUNT(*) AS usage_31d,
                   COALESCE(SUM(CASE WHEN reserved_at > ? THEN 1 ELSE 0 END), 0) AS usage_24h,
                   COALESCE(SUM(CASE WHEN reserved_at > ? AND automatic = 1 THEN 1 ELSE 0 END), 0) AS automatic_24h
            FROM requests WHERE reserved_at > ?
        """, (timestamp - 24 * 60 * 60, timestamp - 24 * 60 * 60, timestamp - 31 * 24 * 60 * 60)).fetchone()
        return {"usage_31d": row["usage_31d"], "usage_24h": row["usage_24h"],
                "limit_31d": LIMIT_31D, "limit_24h": LIMIT_24H,
                "automatic_24h": row['automatic_24h'], "automatic_limit_24h": AUTOMATIC_LIMIT_24H}

    def usage(self, now=None):
        with self._connection() as connection:
            return self._usage(connection, _utc_now(now).timestamp())

    def search_status(self, hub, travel_date):
        _, _, query_key = _query(hub, travel_date)
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM searches WHERE query_key = ?", (query_key,)).fetchone()
            offers = connection.execute("SELECT COUNT(*) FROM offers WHERE provider_query_key = ?", (query_key,)).fetchone()[0]
        if row is None:
            return {"state": "unsearched", "checked_at": None, "checked_epoch": None, "offers": offers,
                    "lease_until": None, "message": "No fare check has been requested."}
        return {"state": row["state"], "checked_at": row["checked_at"], "message": row["message"],
                "checked_epoch": row['checked_epoch'], "lease_until": row['lease_until'], "offers": offers}

    @staticmethod
    def _result(connection, query_key, state, message, cached=False):
        count = connection.execute("SELECT COUNT(*) FROM offers WHERE provider_query_key = ?", (query_key,)).fetchone()[0]
        return {"state": state, "message": message, "offers": count, "cached": cached}

    def refresh(self, hub, travel_date, api_key, fetcher=None, now=None, automatic=False):
        """Reserve and check one route/date without retries, within shared limits."""
        hub, travel_date, query_key = _query(hub, travel_date)
        current = _utc_now(now)
        timestamp = current.timestamp()
        token = uuid.uuid4().hex
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM searches WHERE query_key = ?", (query_key,)).fetchone()
            if row:
                if row["state"] == "running" and (row["lease_until"] or 0) > timestamp:
                    return self._result(connection, query_key, "running", "A fare check is already running.", True)
                age = timestamp - row["checked_epoch"] if row["checked_epoch"] is not None else None
                ttl = CACHE_SECONDS if row["state"] == "complete" else ERROR_COOLDOWN_SECONDS
                if automatic:
                    count = connection.execute("SELECT COUNT(*) FROM offers WHERE provider_query_key = ?", (query_key,)).fetchone()[0]
                    ttl = AUTOMATIC_EMPTY_SECONDS if row['state'] == 'complete' and not count else AUTOMATIC_CACHE_SECONDS
                if row["state"] in {"complete", "error"} and age is not None and 0 <= age < ttl:
                    return self._result(connection, query_key, row["state"], row["message"], True)
            if not api_key or not str(api_key).strip():
                return self._result(connection, query_key, "unconfigured", "Add AYCF_SERPAPI_KEY to enable optional fare checks.")
            usage = self._usage(connection, timestamp)
            if (usage["usage_31d"] >= LIMIT_31D or usage["usage_24h"] >= LIMIT_24H
                    or (automatic and usage['automatic_24h'] >= AUTOMATIC_LIMIT_24H)):
                message = "The free-data allowance is reserved: at most 8 checks per 24 hours and 220 per 31 days."
                if automatic and usage['automatic_24h'] >= AUTOMATIC_LIMIT_24H:
                    message = 'Automatic daily limit reached; manual checks still share the overall free-data allowance.'
                connection.execute("""
                    INSERT INTO searches(query_key, hub, travel_date, state, message)
                    VALUES (?, ?, ?, 'blocked', ?)
                    ON CONFLICT(query_key) DO UPDATE SET state='blocked', message=excluded.message,
                        lease_token=NULL, lease_until=NULL
                """, (query_key, hub, travel_date, message))
                return self._result(connection, query_key, "blocked", message)
            connection.execute("INSERT INTO requests(query_key, reserved_at, automatic) VALUES (?, ?, ?)",
                               (query_key, timestamp, int(bool(automatic))))
            if automatic:
                # Commit the batch clock with its first quota reservation, so a
                # killed worker cannot immediately start a duplicate batch.
                connection.execute('''
                    UPDATE automation SET last_batch_epoch = ? WHERE id = 1
                    AND (last_batch_epoch IS NULL OR last_batch_epoch <= ?)
                ''', (timestamp, timestamp - AUTOMATIC_CACHE_SECONDS))
            connection.execute("""
                INSERT INTO searches(query_key, hub, travel_date, state, message, lease_token, lease_until)
                VALUES (?, ?, ?, 'running', 'Checking available feeder fares.', ?, ?)
                ON CONFLICT(query_key) DO UPDATE SET state='running', message=excluded.message,
                    lease_token=excluded.lease_token, lease_until=excluded.lease_until
            """, (query_key, hub, travel_date, token, timestamp + LEASE_SECONDS))

        try:
            if fetcher is None:
                from feeder_provider import fetch_serpapi_offers
                fetcher = fetch_serpapi_offers
            raw_offers = fetcher(hub, travel_date, api_key, now=current)
            if not isinstance(raw_offers, list):
                raise ValueError("The fare provider returned an unexpected response.")
            unique_offers = {}
            for raw in raw_offers:
                if not raw.get("observed_at"):
                    raise ValueError("The fare observation time is missing.")
                offer = normalize_offer({**raw, "source": PROVIDER_SOURCE}, now=current)
                if (offer["origin"] != "MAN" or offer["destination"] != hub
                        or _departure_date(offer) != travel_date):
                    raise ValueError("The fare does not match the requested journey.")
                identity = _offer_id(offer, query_key)
                previous = unique_offers.get(identity)
                if previous is None or Decimal(offer["price_gbp"]) < Decimal(previous["price_gbp"]):
                    unique_offers[identity] = offer
            offers = list(unique_offers.values())
            state = "complete"
            message = (f"Found {len(offers)} matching feeder fare{'s' if len(offers) != 1 else ''}."
                       if offers else "No matching fares were returned for this search.")
        except ProviderError as exc:
            # Only the provider's allowlisted message is safe to persist. Raw
            # response text, exception arguments and URLs may contain the key.
            offers = None
            state = "error"
            follow_up = ("Automatic fare checks will wait for the next scheduled batch."
                         if automatic else "Try again after five minutes.")
            message = f"{exc.safe_message} Saved quotes are unchanged. {follow_up}"
        except Exception:
            # Unexpected errors still have no safe diagnostic representation.
            offers = None
            state = "error"
            message = ("The fare check failed. Saved quotes are unchanged; automatic fare checks will wait for the next scheduled batch."
                       if automatic else "The fare check failed. Saved quotes are unchanged; try again after five minutes.")

        finished = current if now is not None else _utc_now()
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT lease_token FROM searches WHERE query_key = ?", (query_key,)).fetchone()
            if row is None or row["lease_token"] != token:
                return self._result(connection, query_key, "running", "A newer fare check has taken over this request.", True)
            if offers is not None:
                connection.execute("DELETE FROM offers WHERE provider_query_key = ?", (query_key,))
                for offer in offers:
                    self._put_offer(connection, offer, query_key)
            connection.execute("""
                UPDATE searches SET state=?, checked_at=?, checked_epoch=?, message=?,
                    lease_token=NULL, lease_until=NULL WHERE query_key=? AND lease_token=?
            """, (state, finished.isoformat(), finished.timestamp(), message, query_key, token))
            return self._result(connection, query_key, state, message)
