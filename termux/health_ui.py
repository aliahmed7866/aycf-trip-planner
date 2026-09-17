"""Termux-only Flask health/status console for AYCF."""

from __future__ import annotations

import hmac
import json
import os
import subprocess
import sys
import time
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, Response, abort, flash, jsonify, redirect, render_template, request, send_file, session, url_for

from termux.diagnostic_downloads import LOG_FILES, log_export, make_bundle

ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf")))
LOG_DIR = STATE_DIR / "logs"

bp = Blueprint("system_health", __name__)


def _json_file(name: str) -> dict:
    try:
        value = json.loads((STATE_DIR / name).read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _csrf_ok() -> bool:
    expected = str(session.get("csrf_token") or "")
    supplied = str(request.form.get("csrf_token") or request.headers.get("X-CSRF-Token") or "")
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


def _age(ts) -> int | None:
    try:
        return max(0, int(time.time()) - int(ts))
    except (TypeError, ValueError):
        return None


def _tail_log(name: str, lines: int = 60) -> dict:
    path = LOG_DIR / name
    if not path.exists():
        return {"name": name, "exists": False, "updated_at": None, "age": None, "lines": []}
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            start = max(0, handle.tell() - 64 * 1024)
            handle.seek(start)
            data = handle.read(64 * 1024)
            if start:
                data = data.partition(b"\n")[2]  # Drop the partial first line.
            text = data.decode("utf-8", errors="replace").splitlines()[-max(1, min(200, lines)):]
        updated = int(path.stat().st_mtime)
        return {
            "name": name,
            "exists": True,
            "updated_at": updated,
            "age": _age(updated),
            "lines": [line.rstrip("\n") for line in text],
        }
    except Exception as exc:
        return {"name": name, "exists": True, "updated_at": None, "age": None, "lines": [f"Unable to read log: {exc}"]}


def _current_logs() -> dict:
    return {key: _tail_log(name) for key, name in LOG_FILES.items()}


def _shared_rate_limit_status() -> dict:
    """Read the local request budget; a status refresh never contacts Wizz."""
    try:
        from wizz_rate_limit import rate_limit_status, request_budget_status
        return {**rate_limit_status(), "request_budget": request_budget_status()}
    except (ImportError, OSError, ValueError, TypeError, sqlite3.Error):
        return {}


def _retry_timestamp(value) -> float:
    try:
        if isinstance(value, str) and 'T' in value:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            timestamp = parsed.replace(tzinfo=timezone.utc).timestamp() if parsed.tzinfo is None else parsed.timestamp()
        else:
            timestamp = float(value)
        return timestamp if math.isfinite(timestamp) and timestamp > 0 else 0
    except (ValueError, TypeError, OverflowError):
        return 0


def rate_limit_summary(scan=None, supervisor=None, now=None) -> dict:
    """Describe saved 429 state without changing the session or request budget."""
    if scan is None:
        from termux.run_state import read_status
        scan = read_status()
    supervisor = _json_file('supervisor-status.json') if supervisor is None else supervisor
    shared = _shared_rate_limit_status()
    current = time.time() if now is None else float(now)
    records = [scan, supervisor]
    # A completed/new scan supersedes an older supervisor pause message.
    if (scan.get('state') and scan.get('state') != 'rate_limited' and
            _retry_timestamp(scan.get('updated_at')) >= _retry_timestamp(supervisor.get('updated_at'))):
        records = [scan]
    records = [row for row in records if row.get('state') == 'rate_limited' or str(row.get('http_status')) == '429']
    deadlines = [_retry_timestamp(row.get(key)) for row in records + [shared]
                 for key in ('cooldown_until', 'retry_at')]
    deadline = max(deadlines, default=0)
    blocked = deadline > current or bool(shared.get('blocked') and not deadline)
    notice = blocked or bool(records)
    intervals = []
    for row in records + [shared]:
        try:
            interval = float(row.get('effective_request_interval', 0))
            if math.isfinite(interval) and interval > 0:
                intervals.append(interval)
        except (ValueError, TypeError):
            pass
    interval = max(intervals, default=0)
    try:
        effective = float(shared.get('effective_request_interval', 0))
        if math.isfinite(effective) and effective > 0:
            interval = effective
    except (ValueError, TypeError):
        pass
    try:
        retry_at = datetime.fromtimestamp(deadline, timezone.utc).isoformat() if deadline else None
        retry_label = datetime.fromtimestamp(deadline, timezone.utc).strftime('%d %b %Y, %H:%M:%S UTC') if deadline else None
    except (ValueError, OverflowError, OSError):
        retry_at = retry_label = None
    pacing = f'Resumes more slowly, with at least {interval:g}s between requests.' if interval else 'Resumes with slower request pacing.'
    if blocked:
        label = 'Wizz scan paused'
        when = f'until {retry_label}' if retry_label else 'while the request cooldown is active'
        guidance = f'Wizz returned HTTP 429. AYCF pauses requests {when}. {pacing} Saved flights remain searchable and the encrypted session is retained. Authentication repair does not clear this cooldown.'
    elif notice:
        label = 'Cooldown finished' if deadline else 'Wizz retry pending'
        guidance = ('The Wizz cooldown has finished; pending checks can resume on the next supervisor wake. ' if deadline else
                    'Wizz reported HTTP 429; the next retry time is not available in the saved status. ')
        guidance += f'{pacing} Saved flights and the encrypted session are retained; authentication repair is not needed for a rate limit.'
    else:
        label = guidance = ''
    from wizz_rate_limit import diagnostic_message
    diagnostic = diagnostic_message(shared.get('last_limit'))
    if guidance and diagnostic:
        guidance += ' ' + diagnostic
    budget = shared.get('request_budget') or {}
    budget_text = (f"{int(budget.get('requests_60s', 0))} / {int(budget.get('limit_per_minute', 40))} managed attempts in the last minute; "
                   f"{int(budget.get('requests_15m', 0))} in 15 minutes; {int(budget.get('requests_24h', 0))} in 24 hours.")
    if not budget:
        budget_text = 'Request history is unavailable.'
    if budget.get('wait_seconds', 0) > 0 and not blocked:
        budget_text += f" Local budget pause: up to {math.ceil(budget['wait_seconds'])}s."
    return {'blocked': blocked, 'notice': notice, 'label': label, 'guidance': guidance,
            'request_budget': budget, 'budget_text': budget_text, 'diagnostic': diagnostic,
            'last_limit': shared.get('last_limit'),
            'retry_at': retry_at, 'retry_label': retry_label, 'cooldown_until': deadline or None,
            'effective_request_interval': interval or None,
            'remaining_seconds': max(0, int(deadline - current)) if deadline else None}


def _browser_bridge(supervisor: dict, wizz: dict) -> dict:
    """Summarise the most recent Android Chrome/ADB recovery state without polling ADB on every UI refresh."""
    repair_rc = supervisor.get("last_repair_rc")
    try:
        repair_rc = int(repair_rc) if repair_rc is not None else None
    except (TypeError, ValueError):
        repair_rc = None

    session_age = _age(wizz.get("updated_at"))
    repair_age = _age(supervisor.get("last_repair_attempt_at"))
    newer_session = session_age is not None and repair_age is not None and session_age <= repair_age
    if wizz.get("ok") is True and (supervisor.get("health_ok") is True or newer_session):
        return {
            "state": "not_needed",
            "label": "Browser fallback standby",
            "detail": "The encrypted Wizz session is healthy, so Chrome/ADB is not currently needed.",
            "severity": "neutral",
        }

    if repair_rc == 21:
        return {
            "state": "pairing_lost",
            "label": "ADB pairing lost",
            "detail": "Wireless debugging is enabled, but Termux cannot reach a paired ADB endpoint. Re-pair this phone with Termux.",
            "severity": "warning",
        }
    if repair_rc == 22:
        return {
            "state": "devtools_forward_failed",
            "label": "Chrome bridge unavailable",
            "detail": "ADB is connected, but Chrome DevTools could not be exposed to AYCF.",
            "severity": "warning",
        }
    if repair_rc == 23:
        return {
            "state": "chrome_unavailable",
            "label": "Chrome unavailable",
            "detail": "ADB is connected, but Chrome DevTools did not recover automatically.",
            "severity": "warning",
        }
    if repair_rc == 0 and supervisor.get("health_ok") is True:
        return {
            "state": "ready",
            "label": "Browser fallback ready",
            "detail": "The latest automatic authentication repair completed successfully.",
            "severity": "success",
        }
    return {
        "state": "unknown",
        "label": "Browser fallback unknown",
        "detail": "No recent ADB/browser recovery result is available yet.",
        "severity": "neutral",
    }


def _snapshot(include_logs: bool = False) -> dict:
    from termux.run_state import read_status

    scan = read_status()
    wizz = _json_file("wizz-session-status.json")
    supervisor = _json_file("supervisor-status.json")
    rate_limit = rate_limit_summary(scan, supervisor)
    bridge = _browser_bridge(supervisor, wizz)
    if rate_limit['notice']:
        bridge = {'state': 'not_needed', 'label': 'Session retained', 'severity': 'neutral',
                  'detail': 'HTTP 429 is a request limit. Browser authentication repair does not clear the cooldown.'}
    health_ok = bool(supervisor.get("health_ok")) and bool(wizz.get("ok"))
    needs_attention = (
        rate_limit['notice']
        or scan.get("state") in {"attention_required", "failed", "auth_failed", "service_unavailable", "wizz_authentication_required", "request_rejected", "request_repair_required", "partial", "interrupted"}
        or supervisor.get("state") in {"attention_required", "repair_failed", "unhealthy", "scan_retry_pending", "request_rejected", "request_repair_required"}
        or (bool(wizz) and not bool(wizz.get("ok")))
        or bridge.get("state") in {"pairing_lost", "devtools_forward_failed", "chrome_unavailable"}
    )
    result = {
        "ok": health_ok and not needs_attention,
        "status_label": rate_limit['label'] if rate_limit['notice'] else ('All systems operational' if health_ok and not needs_attention else 'Attention required'),
        "rate_limit": rate_limit,
        "guidance": rate_limit['guidance'] or {
            "request_rejected": "Automatic requests are paused after Wizz rejected a request. Check availability in your normal Wizz browser, then deliberately retry the scan when ready.",
            "request_repair_required": "Automatic requests are paused because the captured request did not verify availability. Recapture a successful availability request in Chrome, then run the scan again.",
            "partial": "Saved verified flights remain searchable. Some airport checks are still unknown; review the scan message before retrying.",
            "interrupted": "The scan was interrupted. Completed checks are preserved; the supervisor can resume pending work.",
        }.get(scan.get("state"), ""),
        "scan": scan,
        "wizz": wizz,
        "supervisor": supervisor,
        "browser_bridge": bridge,
        "ages": {
            "scan": _age(scan.get("updated_at")),
            "wizz": _age(wizz.get("updated_at")),
            "supervisor": _age(supervisor.get("updated_at")),
            "health": _age(supervisor.get("last_health_at")),
            "health_success": _age(supervisor.get("last_health_success_at")),
            "wake": _age(supervisor.get("last_wake_at")),
        },
    }
    if include_logs:
        result["logs"] = _current_logs()
    return result


def _spawn(label: str, args: list[str], log_name: str, *, allow_local_reset: bool = False) -> None:
    rate_limit = rate_limit_summary()
    if rate_limit['blocked'] and not allow_local_reset:
        flash(rate_limit['guidance'], 'warning')
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_DIR / log_name, "ab", buffering=0) as log:
        subprocess.Popen(
            args, cwd=str(ROOT), env=os.environ.copy(), stdout=log,
            stderr=subprocess.STDOUT, start_new_session=True,
        )
    if allow_local_reset and rate_limit['blocked']:
        flash("Reset requested. Wizz requests stay paused until the cooldown ends.", "info")
    else:
        flash(f"{label} started. This page will update automatically.", "info")


@bp.get("/system")
def page():
    return render_template("system_health.html", health=_snapshot(include_logs=True))


@bp.get("/system/status.json")
def status_json():
    include_logs = request.args.get("logs") == "1"
    return jsonify(_snapshot(include_logs=include_logs)), 200, {"Cache-Control": "no-store"}


@bp.get("/system/logs/<key>/download")
def download_log(key):
    if key not in LOG_FILES:
        abort(404)
    metadata, text = log_export(LOG_DIR, key)
    if not metadata['exists']:
        abort(404, description='This log is missing or unreadable.')
    return Response(text, mimetype='text/plain', headers={
        'Content-Disposition': f'attachment; filename="aycf-{key}.log.txt"',
        'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'})


@bp.get("/system/diagnostics/download")
def download_diagnostics():
    from wizz_rate_limit import rate_limit_events
    try:
        events = rate_limit_events()
    except (OSError, sqlite3.Error):
        events = []
    bundle = make_bundle(LOG_DIR, ROOT, _shared_rate_limit_status(), events)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    response = send_file(bundle, mimetype='application/zip', as_attachment=True,
                         download_name=f'aycf-diagnostics-{stamp}.zip', max_age=0)
    response.headers['Cache-Control'] = 'no-store'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response


@bp.post("/system/run-scan")
def run_scan():
    if not _csrf_ok():
        flash("Your form expired. Please try again.", "warning")
        return redirect(url_for("system_health.page"))
    env_python = sys.executable
    _spawn(
        "AYCF scan",
        [env_python, str(ROOT / "termux" / "runtime.py"), "morning"],
        "manual-morning.log",
    )
    return redirect(url_for("system_health.page"))


@bp.post("/system/fresh-scan")
def fresh_scan():
    if not _csrf_ok():
        flash("Your form expired. Please try again.", "warning")
        return redirect(url_for("system_health.page"))
    _spawn(
        "Fresh AYCF scan",
        [sys.executable, str(ROOT / "termux" / "runtime.py"), "fresh"],
        "manual-morning.log",
        allow_local_reset=True,
    )
    return redirect(url_for("system_health.page"))


@bp.post("/system/repair-auth")
def repair_auth():
    if not _csrf_ok():
        flash("Your form expired. Please try again.", "warning")
        return redirect(url_for("system_health.page"))
    _spawn(
        "Wizz authentication repair",
        [sys.executable, str(ROOT / "termux" / "runtime.py"), "repair"],
        "auth-repair.log",
    )
    return redirect(url_for("system_health.page"))


@bp.post("/system/check-now")
def check_now():
    if not _csrf_ok():
        flash("Your form expired. Please try again.", "warning")
        return redirect(url_for("system_health.page"))
    _spawn(
        "Supervisor health check",
        [sys.executable, str(ROOT / "termux" / "supervisor.py")],
        "supervisor.log",
    )
    return redirect(url_for("system_health.page"))
