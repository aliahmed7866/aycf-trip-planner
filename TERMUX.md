# AYCF Termux deployment

This deployment is designed to run unattended on Android/Termux using the `deploy/termux` branch.

## Normal operation

Source the private environment once in your shell/session:

```bash
source ~/.config/aycf/env
```

Then use the unified runtime commands:

```bash
python termux/runtime.py morning   # normal scheduled/manual scan
python termux/runtime.py status    # current scan + Wizz session state
python termux/runtime.py repair    # force Wizz session repair only
python termux/runtime.py web       # launch the local web UI
```

A forced manual refresh scan is still available with:

```bash
AYCF_FORCE_MORNING_SCAN=true python termux/runtime.py morning
```

The scan process now owns a single-run file lock. Repeated web clicks, scheduled launches, and manual launches cannot create overlapping scans; duplicates return `already_running` and exit cleanly.

## Wizz authentication lifecycle

The scanner is intentionally browser-independent during normal operation.

1. A normal scan uses the encrypted saved Wizz session.
2. If the saved availability endpoint has rotated, the scanner first tries to rediscover it using the saved authenticated session.
3. If authentication has actually expired, `termux/refresh_wizz_direct.py` follows the live Wizz login redirects/forms using the encrypted credential vault and saves a newly validated encrypted cookie session.
4. Android Chrome/ADB is only a fallback for first-time capture, changed/unsupported login flows, or interactive Wizz security challenges such as CAPTCHA/MFA/passkeys.
5. Ordinary network failures do not restart Chrome; the scan exits cleanly and a later run can retry.

The browser fallback deliberately does not bypass Wizz security challenges.

## Persistent status

Runtime status lives under:

```text
~/.local/share/aycf/scan-status.json
~/.local/share/aycf/wizz-session-status.json
```

Typical scan states are:

```text
running
renewing_auth
auth_failed
attention_required
complete
failed
```

Use `python termux/runtime.py status` instead of inspecting these files manually.

## Credentials and encrypted session

Secrets stay outside the repository. The deployment expects:

```text
~/.config/aycf/env
~/.config/aycf/wizz_credentials.enc
```

The saved Wizz browser/session state is encrypted using `AYCF_SESSION_ENCRYPTION_KEY` and the configured `WIZZ_SESSION_FILE` path.

Do not commit environment files, credential vaults, or encrypted session files.

## Local database and logs

Default Termux state paths:

```text
~/.local/share/aycf/aycf.sqlite3
~/.local/share/aycf/logs/morning.log
~/.local/share/aycf/logs/manual-morning.log
```

The scanner uses the current official AYCF PDF catalogue. The Wizz sitemap may be used as reference data but is not allowed to expand the live AYCF scan scope beyond the official PDF.

## Web-triggered scans

The local Flask UI can launch the same `termux/runtime.py morning` path. The same file lock applies, so multiple button presses are safe. Web-triggered and scheduled scans therefore share authentication repair, route scope, database storage, and scan status rather than maintaining separate execution paths.

## Updating the deployment

The live deployment branch is `deploy/termux`:

```bash
cd ~/aycf-trip-planner
git checkout deploy/termux
git pull --ff-only origin deploy/termux
bash termux/finish-full-deployment.sh
```

The handoff launches AYCF and Admin Hub directly and disables the legacy
`aycf`, `aycf-admin`, and `aycf-deploy` runit services. Use
`bash termux/finish-full-deployment.sh` (or `bash termux/aycf restart`) for subsequent restarts too;
`sv restart aycf` does not restart this deployment. The handoff also checks
Sunscape health on port 8081.

Validate proposed changes with isolated test databases before merging. Repository
CI cannot confirm the phone's checked-out commit, Wizz authentication, runtime,
live database readiness, or Android notifications.

## When manual attention is genuinely required

Manual action should now be exceptional. Check `python termux/runtime.py status` if a scan does not complete. Manual browser attention is only expected when Wizz presents CAPTCHA/MFA/passkey/security verification, credentials have changed, the initial request template has never been captured, or Android Wireless Debugging is required for the exceptional Chrome fallback and is disabled.

Do not manually cycle `adb forward`, kill Chrome, or probe `/json/version` during normal operation; those are diagnostic steps, not part of the routine workflow.
