# Automatic scan recovery

The morning publication window starts normal scan attempts. It does not stop
recovery: failed, interrupted or authentication-blocked scans remain pending
across supervisor wakes and process restarts, including after the window closes.
Completed route checks are reused by the existing resumable scanner.

When a scan reports an authentication failure, the supervisor invalidates its
cached healthy result. It repairs the session before retrying pending work.
A newer successful repair from the Repair Auth button is recognised immediately;
that button also wakes the supervisor after success. It does not force an extra
heavy refresh when the scan is already complete. Retry cooldowns still apply.

Successful manual scans clear pending work. An exit code of zero alone does not:
the supervisor requires a fresh `complete` scan status. Status now retains
`last_scan_outcome` and `last_scan_failure`, so a later successful manual scan
does not erase the supervisor's record of an earlier scheduled failure.

Defaults (existing explicit environment overrides are respected):

| Setting | Default | Purpose |
| --- | --- | --- |
| `AYCF_SCAN_WINDOW_UTC` | `6,7,8,9,10` | Start ordinary publication checks |
| `AYCF_SCAN_RETRY_SECONDS` | `900` | Minimum interval between scan attempts |
| `AYCF_AUTH_REPAIR_COOLDOWN_SECONDS` | `900` | Minimum interval between supervisor repair attempts |
| `AYCF_WIZZ_REFRESH_TIMEOUT` | `300` | Shared repair timeout in seconds, bounded to 30–900 |
| `AYCF_AUTH_HEALTH_SECONDS` | `21600` | Background session validation interval |

Scheduled, in-scan and manual repairs use the same timeout. A missing saved
session enters the existing renewal path. Server outages remain service failures
and do not trigger unnecessary login attempts when authentication is healthy.

Android must still wake the registered scheduler job. Interactive challenges or
invalid credentials can still require attention; unsuccessful work remains pending.
Inspect `python termux/runtime.py status` for `scan_pending`, the last outcome,
repair exit code and most recent wake. No scheduler reinstallation is required.
