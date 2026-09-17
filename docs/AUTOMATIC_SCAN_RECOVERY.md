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
| `AYCF_SCAN_RETRY_SECONDS` | `900` | General failure retry interval; Wizz rate limits use their persisted deadline |
| `AYCF_AUTH_REPAIR_COOLDOWN_SECONDS` | `900` | Minimum interval between supervisor repair attempts |
| `AYCF_WIZZ_REFRESH_TIMEOUT` | `300` | Shared repair timeout in seconds, bounded to 30–900 |
| `AYCF_AUTH_HEALTH_SECONDS` | `21600` | Background session validation interval |

Scheduled, in-scan and manual repairs use the same timeout. A missing saved
session enters the existing renewal path. Server outages remain service failures
and do not trigger unnecessary login attempts when authentication is healthy.

HTTP 429 has its own `rate_limited` state. The first response stops new requests
and persists a cooldown of at least 15 minutes, respecting any longer Wizz
`Retry-After`. Repeated episodes increase the wait up to six hours locally;
server-requested waits have no such cap. Manual/forced scans and auth probes
cannot bypass the deadline. The supervisor checks it before health or repair
work, retains the prior authentication result, and resumes pending scans once
due. The deadline is based on the response time, not the previous scan start.

Scans default to three workers and a shared one-second minimum request interval.
Recovery pacing survives restarts: two seconds after the first episode, then
3, 5, 10, 20 and 30 seconds, respecting any slower configuration. It resets only
after 24 hours without a new 429 and no active cooldown. State is stored in
`AYCF_STATE_DIR/wizz-rate-limit.sqlite3`; the default directory is
`~/.local/share/aycf`, with an optional `AYCF_WIZZ_RATE_LIMIT_PATH` override.
Existing stored episode levels automatically use the new pacing when the updated
scanner starts, without changing their cooldown deadline or clearing saved work.
Use the same path for every AYCF process. Cached results and SerpApi checks are
independent of this Wizz cooldown.

Android must still wake the registered scheduler job. Interactive challenges or
invalid credentials can still require attention; unsuccessful work remains pending.
Inspect `python termux/runtime.py status` for `scan_pending`, the last outcome,
repair exit code and most recent wake. No scheduler reinstallation is required.

## Preventive request budget and diagnostics

Managed HTTP attempts also share a rolling 60-second budget: 40 normally and 20
while a prior 429 episode remains in recovery memory. Workers wait for a slot
without marking the scan failed or changing its cooldown. The allowance is an
AYCF precaution, not a claimed Wizz limit. All managed scan/session/auth callers
and retries count; cached results and separate fare-provider requests do not.
Browser requests are outside this accounting. A reserved attempt can count even
if a newly recorded cooldown prevents its final send.

The existing SQLite file gains `request_starts` and `rate_limit_events` tables
on its next write. Older cooldown rows and deadlines are preserved. Status reads
support both schemas without migrating them. Recent counts persist across
processes; request timestamps older than 24 hours are ignored and pruned on the
next admission. Only the latest 20 429 events are retained.

Each event records its operation category, parsed Retry-After type and seconds,
policy/effective wait, deadline source, and request counts at the failure.
No raw header, endpoint URL, token, cookie or payload is retained. A longer Wizz
Retry-After still wins. A local budget pause is labelled explicitly and is not
counted as a new 429. Inspect System health or the scan log to see the difference.
