"""Run the tiered morning scan with on-demand Wizz session renewal."""

import os
import subprocess
from pathlib import Path

import requests

from scanner import WizzSessionExpired
import tiered_morning

ROOT = Path(__file__).resolve().parent.parent
REFRESH = ROOT / "termux" / "auto-refresh-wizz.sh"
BASH = os.environ.get("AYCF_BASH", "/data/data/com.termux/files/usr/bin/bash")


def _refresh(reason: str) -> bool:
    if os.environ.get("AYCF_AUTO_REFRESH_WIZZ_SESSION", "true").lower() != "true":
        return False
    print(f"[AYCF] Automatic Wizz session refresh: {reason}", flush=True)
    try:
        result = subprocess.run(
            [BASH, str(REFRESH)],
            cwd=str(ROOT),
            env=os.environ.copy(),
            timeout=max(20, min(120, int(os.environ.get("AYCF_WIZZ_REFRESH_TIMEOUT", "60")))),
            check=False,
        )
    except Exception as exc:
        print(f"[AYCF] Automatic Wizz refresh could not run: {exc}", flush=True)
        return False
    if result.returncode == 0:
        return True
    print(
        f"[AYCF] Automatic Wizz refresh was not available (exit {result.returncode}); "
        "the saved encrypted session will be tried if possible.",
        flush=True,
    )
    return False


def _is_server_error(exc: requests.HTTPError) -> bool:
    response = exc.response
    return response is not None and 500 <= int(response.status_code) < 600


def run(force: bool = False):
    # Normal scans use the saved encrypted Wizz session directly. Do not touch
    # Android Chrome/ADB unless the saved session or captured endpoint actually
    # fails. This keeps unattended scans independent of Wireless Debugging.
    try:
        return tiered_morning.run(force=force)
    except WizzSessionExpired:
        # Retry exactly once after on-demand renewal to avoid loops/account
        # hammering. The refresh helper first tries the encrypted saved session
        # and only falls back to Android Chrome when authentication is expired.
        if not _refresh("Wizz reported that the saved session expired"):
            raise
        print("[AYCF] Wizz session renewed; retrying the morning scan once.", flush=True)
        return tiered_morning.run(force=force)
    except requests.HTTPError as exc:
        # The core client already performs bounded retries for transient 5xx.
        # If they all fail, the captured Multipass endpoint may be stale. The
        # refresh helper attempts endpoint rediscovery with the encrypted saved
        # session before involving Chrome/ADB.
        if not _is_server_error(exc):
            raise
        if not _refresh("persistent Wizz server error; repairing captured availability endpoint"):
            raise
        print("[AYCF] Wizz endpoint repaired; resuming the morning scan once.", flush=True)
        return tiered_morning.run(force=force)
