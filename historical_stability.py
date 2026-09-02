"""Import and score third-party AYCF history without mixing it with live scan truth.

The historical bootstrap is intentionally advisory. Current topology and live AYCF
scan data remain authoritative. Historical rows are sourced from the MIT-licensed
markvincevarga/wizzair-aycf-availability archive.
"""
from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from route_history import history_db_path

SOURCE_ID = "markvincevarga/wizzair-aycf-availability"
WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _connect(path: Optional[str] = None) -> sqlite3.Connection:
    target = path or history_db_path()
    Path(target).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript("""
      CREATE TABLE IF NOT EXISTS external_snapshot_days (
        source TEXT NOT NULL,
        snapshot_date TEXT NOT NULL,
        generated_at TEXT,
        imported_at TEXT NOT NULL,
        PRIMARY KEY(source,snapshot_date)
      );
      CREATE TABLE IF NOT EXISTS external_route_appearances (
        source TEXT NOT NULL,
        snapshot_date TEXT NOT NULL,
        origin TEXT NOT NULL,
        destination TEXT NOT NULL,
        availability_start TEXT,
        availability_end TEXT,
        generated_at TEXT,
        context TEXT NOT NULL DEFAULT 'normal',
        PRIMARY KEY(source,snapshot_date,origin,destination,availability_start,availability_end)
      );
      CREATE INDEX IF NOT EXISTS idx_external_route_pair
        ON external_route_appearances(source,origin,destination,snapshot_date);
    """)
    return conn


def _parse_date(value: str) -> Optional[date]:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _easter_sunday(year: int) -> date:
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def travel_context(day: date) -> str:
    """Broad demand context, deliberately descriptive rather than predictive."""
    easter = _easter_sunday(day.year)
    if date(day.year, 12, 20) <= day or day <= date(day.year, 1, 5):
        return "christmas_new_year"
    if easter - timedelta(days=7) <= day <= easter + timedelta(days=7):
        return "easter"
    if date(day.year, 7, 15) <= day <= date(day.year, 8, 31):
        return "summer_peak"
    if day.weekday() >= 4:
        return "weekend"
    return "normal"


def import_archive(data_dir: str, *, days: int = 365, path: Optional[str] = None, today: Optional[date] = None) -> Dict[str, Any]:
    root = Path(data_dir)
    if not root.is_dir():
        raise ValueError(f"Historical data directory not found: {root}")
    today = today or date.today()
    cutoff = today - timedelta(days=max(1, int(days)) - 1)
    files = sorted(root.glob("*.csv"))
    selected = []
    for file in files:
        snapshot_day = _parse_date(file.stem.split("T", 1)[0])
        if snapshot_day and cutoff <= snapshot_day <= today:
            selected.append((file, snapshot_day))

    imported_days = 0
    imported_rows = 0
    skipped_files = 0
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    with _connect(path) as conn:
        for file, snapshot_day in selected:
            try:
                with file.open(newline="", encoding="utf-8") as handle:
                    reader = csv.DictReader(handle)
                    required = {"departure_from", "departure_to"}
                    if not required.issubset(set(reader.fieldnames or [])):
                        skipped_files += 1
                        continue
                    rows = list(reader)
            except (OSError, csv.Error):
                skipped_files += 1
                continue
            generated_at = (rows[0].get("data_generated") or "").strip() if rows else ""
            conn.execute(
                "INSERT OR REPLACE INTO external_snapshot_days(source,snapshot_date,generated_at,imported_at) VALUES(?,?,?,?)",
                (SOURCE_ID, snapshot_day.isoformat(), generated_at, now),
            )
            conn.execute(
                "DELETE FROM external_route_appearances WHERE source=? AND snapshot_date=?",
                (SOURCE_ID, snapshot_day.isoformat()),
            )
            payload = []
            for row in rows:
                origin = (row.get("departure_from") or "").strip()
                destination = (row.get("departure_to") or "").strip()
                if not origin or not destination:
                    continue
                start = (row.get("availability_start") or "").strip()
                end = (row.get("availability_end") or "").strip()
                travel_day = _parse_date(start) or snapshot_day
                payload.append((SOURCE_ID, snapshot_day.isoformat(), origin, destination, start, end, (row.get("data_generated") or generated_at).strip(), travel_context(travel_day)))
            conn.executemany(
                "INSERT OR REPLACE INTO external_route_appearances(source,snapshot_date,origin,destination,availability_start,availability_end,generated_at,context) VALUES(?,?,?,?,?,?,?,?)",
                payload,
            )
            imported_days += 1
            imported_rows += len(payload)
        conn.commit()
    return {
        "source": SOURCE_ID,
        "cutoff": cutoff.isoformat(),
        "through": today.isoformat(),
        "files_considered": len(selected),
        "snapshot_days": imported_days,
        "route_rows": imported_rows,
        "skipped_files": skipped_files,
    }


def _recency_weight(snapshot_day: date, latest_day: date) -> float:
    age = max(0, (latest_day - snapshot_day).days)
    if age <= 30:
        return 1.0
    if age <= 90:
        return 0.65
    if age <= 180:
        return 0.35
    return 0.15


def _presence_rate(days: List[date], seen_days: set[date]) -> Optional[float]:
    if not days:
        return None
    return round(100.0 * sum(1 for d in days if d in seen_days) / len(days), 1)


def _trend_label(recent_30d: Optional[float], previous_30d: Optional[float]) -> str:
    if recent_30d is None or previous_30d is None:
        return "insufficient"
    delta = recent_30d - previous_30d
    if delta >= 15:
        return "improving"
    if delta <= -15:
        return "declining"
    return "steady"


def archive_scores(path: Optional[str] = None, limit: int = 2000) -> List[Dict[str, Any]]:
    """Return explainable, recency-weighted AYCF appearance baselines."""
    with _connect(path) as conn:
        day_rows = conn.execute(
            "SELECT snapshot_date FROM external_snapshot_days WHERE source=? ORDER BY snapshot_date",
            (SOURCE_ID,),
        ).fetchall()
        if not day_rows:
            return []
        snapshot_days = [date.fromisoformat(r["snapshot_date"]) for r in day_rows]
        latest_day = snapshot_days[-1]
        rows = conn.execute(
            "SELECT snapshot_date,origin,destination,context FROM external_route_appearances WHERE source=? ORDER BY snapshot_date",
            (SOURCE_ID,),
        ).fetchall()

    presence: Dict[tuple[str, str], set[date]] = defaultdict(set)
    for row in rows:
        presence[(row["origin"], row["destination"])].add(date.fromisoformat(row["snapshot_date"]))

    output: List[Dict[str, Any]] = []
    for (origin, destination), seen_days in presence.items():
        first_seen = min(seen_days)
        eligible_days = [d for d in snapshot_days if d >= first_seen]
        denominator = sum(_recency_weight(d, latest_day) for d in eligible_days)
        numerator = sum(_recency_weight(d, latest_day) for d in seen_days)
        score = 100.0 * numerator / denominator if denominator else 0.0
        recent_days = [d for d in eligible_days if d >= latest_day - timedelta(days=29)]
        previous_days = [d for d in eligible_days if latest_day - timedelta(days=59) <= d <= latest_day - timedelta(days=30)]
        recent_score = _presence_rate(recent_days, seen_days)
        previous_score = _presence_rate(previous_days, seen_days)

        context_scores: Dict[str, Optional[float]] = {}
        for context in ("normal", "weekend", "summer_peak", "easter", "christmas_new_year"):
            context_days = [d for d in eligible_days if travel_context(d) == context]
            context_scores[context] = _presence_rate(context_days, seen_days)

        weekday_scores = {
            WEEKDAYS[i]: _presence_rate([d for d in eligible_days if d.weekday() == i], seen_days)
            for i in range(7)
        }
        output.append({
            "origin": origin,
            "destination": destination,
            "archive_score": round(score, 1),
            "recent_30d": recent_score,
            "previous_30d": previous_score,
            "trend": _trend_label(recent_score, previous_score),
            "first_seen": first_seen.isoformat(),
            "last_seen": max(seen_days).isoformat(),
            "observed_days": len(seen_days),
            "eligible_days": len(eligible_days),
            "context_scores": context_scores,
            "weekday_scores": weekday_scores,
        })
    output.sort(key=lambda r: (-r["archive_score"], -r["observed_days"], r["origin"], r["destination"]))
    return output[: max(1, min(int(limit), 5000))]


def route_intelligence(origin: str, destination: str, path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Detailed archive intelligence for one route, including a 12-month heatmap."""
    with _connect(path) as conn:
        day_rows = conn.execute(
            "SELECT snapshot_date FROM external_snapshot_days WHERE source=? ORDER BY snapshot_date",
            (SOURCE_ID,),
        ).fetchall()
        if not day_rows:
            return None
        route_rows = conn.execute(
            """SELECT snapshot_date,availability_start,availability_end,context
               FROM external_route_appearances
               WHERE source=? AND origin=? AND destination=? ORDER BY snapshot_date""",
            (SOURCE_ID, origin, destination),
        ).fetchall()
    if not route_rows:
        return None

    snapshot_days = [date.fromisoformat(r["snapshot_date"]) for r in day_rows]
    latest_day = snapshot_days[-1]
    seen_days = {date.fromisoformat(r["snapshot_date"]) for r in route_rows}
    first_seen = min(seen_days)
    eligible = [d for d in snapshot_days if d >= first_seen]

    monthly = []
    month_keys = sorted({(d.year, d.month) for d in eligible})
    for year, month in month_keys:
        days = [d for d in eligible if d.year == year and d.month == month]
        monthly.append({"label": date(year, month, 1).strftime("%b %Y"), "score": _presence_rate(days, seen_days), "observations": len(days)})

    heatmap = []
    for d in eligible:
        heatmap.append({"date": d.isoformat(), "present": d in seen_days, "weekday": d.weekday(), "context": travel_context(d)})

    scores = next((r for r in archive_scores(path, limit=5000) if r["origin"] == origin and r["destination"] == destination), None)
    if not scores:
        return None
    scores["monthly"] = monthly
    scores["heatmap"] = heatmap
    scores["latest_snapshot"] = latest_day.isoformat()
    scores["days_since_seen"] = (latest_day - max(seen_days)).days
    scores["coverage_span_days"] = (max(seen_days) - first_seen).days + 1
    return scores


def external_stats(path: Optional[str] = None) -> Dict[str, Any]:
    with _connect(path) as conn:
        day = conn.execute("SELECT MIN(snapshot_date) first_day,MAX(snapshot_date) last_day,COUNT(*) days FROM external_snapshot_days WHERE source=?", (SOURCE_ID,)).fetchone()
        rows = conn.execute("SELECT COUNT(*) rows,COUNT(DISTINCT origin||'→'||destination) routes FROM external_route_appearances WHERE source=?", (SOURCE_ID,)).fetchone()
    return {
        "source": SOURCE_ID,
        "first_day": day["first_day"],
        "last_day": day["last_day"],
        "snapshot_days": int(day["days"] or 0),
        "route_rows": int(rows["rows"] or 0),
        "routes": int(rows["routes"] or 0),
    }
