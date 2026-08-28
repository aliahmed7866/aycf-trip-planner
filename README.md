# AYCF Trip Planner

AYCF flight scanning and trip-planning tooling for the Wizz Air All You Can Fly programme.

## Termux deployment

The Android/Termux deployment is designed to be low-touch and resilient. Normal scans use the encrypted Wizz session and official AYCF PDF route catalogue. When authentication or a captured Wizz availability endpoint expires, the runtime attempts automatic repair before requiring browser attention.

Common commands:

```bash
source ~/.config/aycf/env
python termux/runtime.py morning
python termux/runtime.py status
python termux/runtime.py repair
python termux/runtime.py web
```

For a forced manual scan:

```bash
AYCF_FORCE_MORNING_SCAN=true python termux/runtime.py morning
```

A convenience wrapper is also available without relying on executable file permissions:

```bash
bash termux/aycf run
bash termux/aycf status
bash termux/aycf repair
bash termux/aycf logs
```

### Automated Wizz renewal

The renewal order is deliberately browser-independent:

1. reuse/validate the encrypted saved Wizz session;
2. repair a rotated availability endpoint from the authenticated wallet session when possible;
3. perform direct HTTP login using encrypted credentials and Wizz's live redirect/form flow;
4. fall back to Android Chrome/ADB only for unsupported/changed login flows, initial capture, or interactive security challenges such as CAPTCHA/MFA/passkeys.

Chrome DevTools is therefore not a prerequisite for routine scans.

### Scan coordination

All Termux scan entry points share a single-run lock and persistent status. Scheduled, manual, and web-triggered scans cannot overlap; duplicate launches return cleanly instead of spawning more workers.

Runtime state is written under `~/.local/share/aycf/`, including `scan-status.json`, `wizz-session-status.json`, the SQLite cache, and logs.

See `TERMUX.md` for deployment and troubleshooting details.
