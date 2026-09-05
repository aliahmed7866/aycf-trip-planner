"""Run the tiered morning scan with on-demand Wizz session renewal."""

import os
import subprocess
from pathlib import Path

import requests

from cache_db import ScanCacheDB
from route_history import snapshot_latest_run
from scanner import WizzIntegrationChanged, WizzSessionExpired
from stability_cache import refresh_stability_cache
import tiered_morning
from termux.run_state import single_scan_lock, write_status
from watch_service import check_watches

ROOT = Path(__file__).resolve().parent.parent
REFRESH = ROOT / "termux" / "auto-refresh-wizz.sh"


def _refresh(reason: str) -> bool:
    if os.environ.get("AYCF_AUTO_REFRESH_WIZZ_SESSION", "true").lower() != "true":
        return False
    write_status("renewing_auth", reason)
    print(f"[AYCF] Automatic Wizz session refresh: {reason}", flush=True)
    try:
        result = subprocess.run(["bash", str(REFRESH)], cwd=str(ROOT), env=os.environ.copy(), timeout=max(20, min(180, int(os.environ.get("AYCF_WIZZ_REFRESH_TIMEOUT", "90")))), check=False)
    except Exception as exc:
        print(f"[AYCF] Automatic Wizz refresh could not run: {exc}", flush=True)
        write_status("auth_failed", str(exc))
        return False
    if result.returncode == 0:
        write_status("running", "Wizz session ready; continuing scan.")
        return True
    print(f"[AYCF] Automatic Wizz refresh was not available (exit {result.returncode}).", flush=True)
    write_status("auth_failed", f"Automatic Wizz renewal exited {result.returncode}.", refresh_exit_code=result.returncode)
    return False


def _is_server_error(exc: requests.HTTPError) -> bool:
    response = exc.response
    return response is not None and 500 <= int(response.status_code) < 600


def _is_session_expiry(exc: BaseException) -> bool:
    if isinstance(exc, WizzSessionExpired):
        return True
    if not isinstance(exc, WizzIntegrationChanged):
        return False
    text = str(exc).casefold()
    return "session" in text and "expired" in text


def _renewal_required(reason: str) -> dict:
    message = f"Wizz authentication renewal required; scan progress preserved. {reason}".strip()
    print(f"[AYCF] {message}", flush=True)
    write_status("attention_required", message, scan_performed=False)
    return {"ok": False, "state": "wizz_authentication_required", "scan_performed": False, "message": message}


def _service_unavailable(reason: str) -> dict:
    message = f"Wizz service is temporarily unavailable; scan progress preserved. {reason}".strip()
    print(f"[AYCF] {message}", flush=True)
    write_status("service_unavailable", message, scan_performed=False)
    return {"ok": False, "state": "wizz_service_unavailable", "scan_performed": False, "message": message}


def _max_auth_recoveries() -> int:
    try:
        value = int(os.environ.get("AYCF_MAX_AUTH_RECOVERIES_PER_SCAN", "3"))
    except ValueError:
        value = 3
    return max(1, min(8, value))


def _run_once(force: bool):
    recoveries = 0
    max_recoveries = _max_auth_recoveries()
    while True:
        try:
            return tiered_morning.run(force=force)
        except (WizzSessionExpired, WizzIntegrationChanged) as exc:
            if not _is_session_expiry(exc):
                raise
            if recoveries >= max_recoveries:
                return _renewal_required(f"Wizz session expired again after {recoveries} automatic renewal(s): {exc}")
            recoveries += 1
            if not _refresh(f"Wizz session expired during scan; preserving completed checks and resuming ({recoveries}/{max_recoveries})"):
                return _renewal_required(str(exc))
            print(f"[AYCF] Wizz session renewed; resuming preserved scan progress ({recoveries}/{max_recoveries}).", flush=True)
        except requests.HTTPError as exc:
            if not _is_server_error(exc):
                raise
            # The client has already exhausted its bounded 5xx retries. A Wizz
            # outage is not evidence that credentials expired, so do not launch
            # browser/session repair or tell the user to reconnect their account.
            return _service_unavailable(str(exc))


def _snapshot_history_after_scan():
    try:
        summary = snapshot_latest_run(ScanCacheDB())
        print(f"[AYCF] Route history: {summary}", flush=True)
        return summary
    except Exception as exc:
        print(f"[AYCF] Route history snapshot failed safely: {type(exc).__name__}: {exc}", flush=True)
        return {"ok": False, "error": str(exc)}


def _refresh_stability_after_scan():
    try:
        summary = refresh_stability_cache()
        print(f"[AYCF] Stability cache: {summary['rows']} routes materialized at {summary['generated_at']}", flush=True)
        return summary
    except Exception as exc:
        # Analytics must never cause a successful live scan to fail.
        print(f"[AYCF] Stability cache refresh failed safely: {type(exc).__name__}: {exc}", flush=True)
        return {"ok": False, "error": str(exc)}


def _check_watches_after_scan():
    try:
        summary = check_watches(ScanCacheDB(), notify=True)
        print(f"[AYCF] Watches: checked {summary['checked']} | new {summary['new_matches']} | notifications {summary['notifications']} | uncovered {summary.get('uncovered', 0)} | errors {summary['errors']}", flush=True)
        return summary
    except Exception as exc:
        print(f"[AYCF] Watch check failed safely: {type(exc).__name__}: {exc}", flush=True)
        return {"checked": 0, "new_matches": 0, "notifications": 0, "errors": 1}


def run(force: bool = False):
    with single_scan_lock() as acquired:
        if not acquired:
            message = "A scan is already running; duplicate launch ignored."
            print(f"[AYCF] {message}", flush=True)
            return {"ok": True, "state": "already_running", "scan_performed": False, "message": message}

        write_status("running", "Preparing AYCF scan.", force=bool(force))
        try:
            result = _run_once(force=force)
        except Exception as exc:
            write_status("failed", str(exc), error_type=type(exc).__name__)
            raise

        if isinstance(result, dict) and result.get("state") in {
            "wizz_authentication_required",
            "wizz_service_unavailable",
        }:
            return result

        if isinstance(result, dict) and (result.get("state") == "already_running" or
                (result.get("skipped") and "already running" in result.get("reason", "").lower())):
            write_status("already_running", "A scan is already running; this launch did not complete a scan.", scan_performed=False)
            return result
        if isinstance(result, dict) and (result.get("ok") is False or
                (result.get("skipped") and result.get("state") != "already_current" and
                 "already scanned" not in result.get("reason", "").lower())):
            write_status("failed", result.get("reason", "Scan did not complete."), scan_performed=False)
            return result

        history_summary = _snapshot_history_after_scan()
        stability_summary = _refresh_stability_after_scan()
        watch_summary = _check_watches_after_scan()
        if isinstance(result, dict):
            result["history"] = history_summary
            result["stability_cache"] = stability_summary
            result["watches"] = watch_summary
        scan_performed = True
        if isinstance(result, dict):
            scan_performed = bool(result.get("scan_performed", not result.get("skipped", False)))
        write_status(
            "complete",
            "AYCF scan completed successfully." if scan_performed else "AYCF scan was already current; maintenance checks completed.",
            scan_performed=scan_performed,
            watches=watch_summary,
            history=history_summary,
            stability_cache=stability_summary,
        )
        return result
