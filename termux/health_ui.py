"""Termux-only Flask health/status console for AYCF."""

from __future__ import annotations

import hmac
import json
import os
import subprocess
import sys
import time
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


def _snapshot() -> dict:
    from termux.run_state import read_status

    scan = read_status()
    wizz = _json_file("wizz-session-status.json")
    supervisor = _json_file("supervisor-status.json")
    health_ok = bool(supervisor.get("health_ok")) and bool(wizz.get("ok"))
    needs_attention = (
        scan.get("state") in {"attention_required", "failed", "wizz_authentication_required"}
        or supervisor.get("state") in {"attention_required", "repair_failed", "unhealthy"}
        or (bool(wizz) and not bool(wizz.get("ok")))
    )
    return {
        "ok": health_ok and not needs_attention,
        "scan": scan,
        "wizz": wizz,
        "supervisor": supervisor,
        "ages": {
            "scan": _age(scan.get("updated_at")),
            "wizz": _age(wizz.get("updated_at")),
            "supervisor": _age(supervisor.get("updated_at")),
            "health": _age(supervisor.get("last_health_at")),
            "health_success": _age(supervisor.get("last_health_success_at")),
            "wake": _age(supervisor.get("last_wake_at")),
        },
    }


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
    return render_template("system_health.html", health=_snapshot())


@bp.get("/system/status.json")
def status_json():
    return jsonify(_snapshot())


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
