"""Termux-only Flask health/status console for AYCF."""

from __future__ import annotations

import hmac
import json
import os
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

from flask import Blueprint, flash, jsonify, redirect, render_template, request, session, url_for

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
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            text = list(deque(handle, maxlen=max(1, min(200, lines))))
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
    return {
        "supervisor": _tail_log("supervisor.log"),
        "scan": _tail_log("manual-morning.log"),
        "auth": _tail_log("auth-repair.log"),
    }


def _browser_bridge(supervisor: dict, wizz: dict) -> dict:
    """Summarise the most recent Android Chrome/ADB recovery state without polling ADB on every UI refresh."""
    repair_rc = supervisor.get("last_repair_rc")
    try:
        repair_rc = int(repair_rc) if repair_rc is not None else None
    except (TypeError, ValueError):
        repair_rc = None

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
    if wizz.get("ok") is True:
        return {
            "state": "not_needed",
            "label": "Browser fallback standby",
            "detail": "The encrypted Wizz session is healthy, so Chrome/ADB is not currently needed.",
            "severity": "neutral",
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
    bridge = _browser_bridge(supervisor, wizz)
    health_ok = bool(supervisor.get("health_ok")) and bool(wizz.get("ok"))
    needs_attention = (
        scan.get("state") in {"attention_required", "failed", "wizz_authentication_required"}
        or supervisor.get("state") in {"attention_required", "repair_failed", "unhealthy"}
        or (bool(wizz) and not bool(wizz.get("ok")))
        or bridge.get("state") in {"pairing_lost", "devtools_forward_failed", "chrome_unavailable"}
    )
    result = {
        "ok": health_ok and not needs_attention,
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


def _spawn(label: str, args: list[str], log_name: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = open(LOG_DIR / log_name, "ab", buffering=0)
    env = os.environ.copy()
    subprocess.Popen(
        args,
        cwd=str(ROOT),
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    flash(f"{label} started. This page will update automatically.", "info")


@bp.get("/system")
def page():
    return render_template("system_health.html", health=_snapshot(include_logs=True))


@bp.get("/system/status.json")
def status_json():
    include_logs = request.args.get("logs") == "1"
    return jsonify(_snapshot(include_logs=include_logs))


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
