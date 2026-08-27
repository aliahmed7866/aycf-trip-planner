"""Run the tiered morning scan with best-effort automatic Wizz session renewal."""

import os
import subprocess
from pathlib import Path

from scanner import WizzSessionExpired
import tiered_morning

ROOT = Path(__file__).resolve().parent.parent
REFRESH = ROOT / "termux" / "auto-refresh-wizz.sh"


def _refresh(reason: str) -> bool:
    if os.environ.get("AYCF_AUTO_REFRESH_WIZZ_SESSION", "true").lower() != "true":
        return False
    print(f"[AYCF] Automatic Wizz session refresh: {reason}", flush=True)
    try:
        result = subprocess.run(
            [str(REFRESH)],
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


def run(force: bool = False):
    # Proactively refresh when Chrome/ADB is available. Failure is deliberately
    # non-fatal because the existing encrypted session may still be valid.
    _refresh("pre-scan best effort")
    try:
        return tiered_morning.run(force=force)
    except WizzSessionExpired:
        # This is the one condition where a refresh can genuinely self-heal the
        # job. Retry exactly once to avoid loops/account hammering.
        if not _refresh("Wizz reported that the saved session expired"):
            raise
        print("[AYCF] Wizz session renewed; retrying the morning scan once.", flush=True)
        return tiered_morning.run(force=force)
