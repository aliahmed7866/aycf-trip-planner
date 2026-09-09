"""Shared repair budget for scheduled, in-scan and manual authentication repair."""

import os


def refresh_timeout() -> int:
    try:
        seconds = int(os.environ.get("AYCF_WIZZ_REFRESH_TIMEOUT", "300"))
    except ValueError:
        seconds = 300
    return max(30, min(900, seconds))
