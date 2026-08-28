"""Run the tiered morning scan with on-demand Wizz session renewal."""

import os
import subprocess
from pathlib import Path

import requests

from scanner import WizzSessionExpired
import tiered_morning
from termux.run_state import single_scan_lock, write_status

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


def _renewal_required(reason: str) -> dict:
    message = f"Wizz authentication renewal required; no scan performed. {reason}".strip()
    print(f"[AYCF] {message}", flush=True)
    write_status("attention_required", message, scan_performed=False)
    return {
        "ok": False,
        "state": "wizz_authentication_required",
        "scan_performed": False,
        "message": message,
    }


def _run_once(force: bool):
    try:
        return tiered_morning.run(force=force)
    except WizzSessionExpired as exc:
        if not _refresh("Wizz reported that the saved session expired"):
            return _renewal_required(str(exc))
        print("[AYCF] Wizz session renewed; retrying the morning scan once.", flush=True)
        try:
            return tiered_morning.run(force=force)
        except (WizzSessionExpired, requests.HTTPError) as retry_exc:
            return _renewal_required(str(retry_exc))
    except requests.HTTPError as exc:
        if not _is_server_error(exc):
            raise
        if not _refresh("persistent Wizz server error; repairing captured availability endpoint"):
            return _renewal_required(str(exc))
        print("[AYCF] Wizz endpoint/session repaired; resuming the morning scan once.", flush=True)
        try:
            return tiered_morning.run(force=force)
        except (WizzSessionExpired, requests.HTTPError) as retry_exc:
            return _renewal_required(str(retry_exc))


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
        write_status("complete", "AYCF scan completed successfully.", scan_performed=True)
        return result
