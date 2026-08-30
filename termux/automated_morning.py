"""Run the tiered morning scan with on-demand Wizz session renewal."""

import os
import subprocess
from pathlib import Path

import requests

from cache_db import ScanCacheDB
from scanner import WizzIntegrationChanged, WizzSessionExpired
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
        result = subprocess.run(
            ["bash", str(REFRESH)],
            cwd=str(ROOT),
            env=os.environ.copy(),
            timeout=max(20, min(180, int(os.environ.get("AYCF_WIZZ_REFRESH_TIMEOUT", "90")))),
            check=False,
        )
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
    """Recognize Wizz's explicit HTTP-400 session-expired response.

    The availability API sometimes returns an HTTP 400 body saying the
    session expired instead of redirecting to login. morning_scan correctly
    preserves completed SQLite checks, so this condition should drive the same
    renewal/resume path as WizzSessionExpired rather than aborting the scan.
    """
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
    return {
        "ok": False,
        "state": "wizz_authentication_required",
        "scan_performed": False,
        "message": message,
    }


def _max_auth_recoveries() -> int:
    try:
        value = int(os.environ.get("AYCF_MAX_AUTH_RECOVERIES_PER_SCAN", "3"))
    except ValueError:
        value = 3
    return max(1, min(8, value))


def _run_once(force: bool):
    """Run until complete, resuming preserved checks after recoverable auth loss."""
    recoveries = 0
    max_recoveries = _max_auth_recoveries()

    while True:
        try:
            return tiered_morning.run(force=force)
        except (WizzSessionExpired, WizzIntegrationChanged) as exc:
            if not _is_session_expiry(exc):
                raise
            if recoveries >= max_recoveries:
                return _renewal_required(
                    f"Wizz session expired again after {recoveries} automatic renewal(s): {exc}"
                )
            recoveries += 1
            if not _refresh(
                f"Wizz session expired during scan; preserving completed checks and resuming ({recoveries}/{max_recoveries})"
            ):
                return _renewal_required(str(exc))
            print(
                f"[AYCF] Wizz session renewed; resuming preserved scan progress "
                f"({recoveries}/{max_recoveries}).",
                flush=True,
            )
        except requests.HTTPError as exc:
            if not _is_server_error(exc):
                raise
            if recoveries >= max_recoveries:
                return _renewal_required(
                    f"Wizz server/runtime recovery limit reached after {recoveries} repair(s): {exc}"
                )
            recoveries += 1
            if not _refresh(
                f"persistent Wizz server error; repairing endpoint/session and resuming ({recoveries}/{max_recoveries})"
            ):
                return _renewal_required(str(exc))
            print(
                f"[AYCF] Wizz endpoint/session repaired; resuming preserved scan progress "
                f"({recoveries}/{max_recoveries}).",
                flush=True,
            )


def _check_watches_after_scan():
    try:
        summary = check_watches(ScanCacheDB(), notify=True)
        print(
            f"[AYCF] Watches: checked {summary['checked']} | "
            f"new {summary['new_matches']} | notifications {summary['notifications']} | errors {summary['errors']}",
            flush=True,
        )
        return summary
    except Exception as exc:
        # Watch notifications must never turn a successful flight scan into a failed scan.
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

        if isinstance(result, dict) and result.get("state") == "wizz_authentication_required":
            return result

        watch_summary = _check_watches_after_scan()
        if isinstance(result, dict):
            result["watches"] = watch_summary
        write_status("complete", "AYCF scan completed successfully.", scan_performed=True, watches=watch_summary)
        return result
