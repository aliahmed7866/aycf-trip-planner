# AYCF Live Trip Scanner

A personal Flask scanner for Wizz Air All You Can Fly (AYCF). The normal user-facing search is database-first: shortly after Wizz publishes the official daily AYCF PDF, a scheduled worker checks every advertised route/date against your authenticated Multipass session and stores the normalized results in SQLite. Interactive searches then build direct and one-stop itineraries from that morning cache instead of repeating hundreds of Wizz requests.

## Morning architecture

1. `morning_scan.py` downloads Wizz's official `https://multipass.wizzair.com/aycf-availability.pdf` directly.
2. `direct_pdf.py` extracts the PDF's `Last run`, departure window and advertised route table.
3. The PDF publication is fingerprinted. If that exact run was already scanned, the worker exits immediately.
4. For each advertised route on each date in the PDF departure window, the authenticated Multipass availability endpoint is checked sequentially with throttling/retry protection.
5. Both positive results and zero-flight checks are stored in SQLite.
6. The Flask UI reads the newest completed morning cache first. Optional live fallback can fill missing cache entries.

Wizz's PDF normally identifies a 07:00 CET publication and a four-day departure period. Do not rely only on a single exact cron minute: run the lightweight worker repeatedly around the publication window. Once it sees and completes a new PDF run, later invocations skip automatically.

## Railway setup

Mount a persistent volume at `/data`, then configure:

```bash
FLASK_SECRET_KEY=<random-long-secret>
AYCF_APP_PASSWORD=<password-for-the-personal-web-ui>
AYCF_ADMIN_TOKEN=<random-long-admin-token-used-only-by-login_wizz.py>
AYCF_SESSION_ENCRYPTION_KEY=<fernet-key>
SESSION_COOKIE_SECURE=true
WIZZ_SESSION_FILE=/data/wizz_session.enc
AYCF_CACHE_DIR=/data/aycf-cache
AYCF_DB_PATH=/data/aycf.sqlite3
```

Generate a Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### Scheduled worker

Create a separate Railway Cron service from the same repository/branch with command:

```bash
python morning_scan.py
```

Use this UTC cron expression:

```text
*/15 6-8 * * *
```

That invokes the worker every 15 minutes from 06:00 through 08:59 UTC. It is intentionally safe to run repeatedly: once the current PDF publication has `scanned_at` recorded in the database, subsequent runs return without calling Wizz. This also gives the system multiple chances if Wizz publishes late or the first attempt encounters a temporary network problem.

A full sweep can contain thousands of route/date checks. Keep the worker sequential and use a persistent service/runtime that allows a long-running cron execution. If your hosting plan imposes short job timeouts, split the worker into resumable batches before increasing concurrency.

## Database cache

SQLite is used by default because this is a personal single-user tool. The database stores:

- `pdf_runs`: Wizz PDF publication timestamp/window and completion state.
- `scan_runs`: morning job status, counts and errors.
- `route_checks`: proof that a route/date was checked, including zero-flight results.
- `route_flights`: normalized flights returned by Multipass.

For a multi-instance deployment, replace SQLite with Postgres rather than sharing a SQLite file over multiple writers.

## Connect your Wizz account

Run the connector on your own computer. Your password and MFA/CAPTCHA are entered only on Wizz's website.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium

export AYCF_APP_URL="https://your-deployed-app.example"
export AYCF_ADMIN_TOKEN="the-same-admin-token-configured-on-the-server"
python login_wizz.py
```

`login_wizz.py` requires HTTPS for remote deployments. The server validates the resulting authenticated session before encrypting it with Fernet. Your Wizz username/password are never stored by this app unless you explicitly opt into the Termux encrypted credential vault described below.

## Interactive search

The web UI normally uses the latest morning database snapshot, so searches should make **zero live Wizz requests**. It supports:

- direct routes;
- one-stop self-transfer combinations;
- Anywhere searches from an origin;
- a separate return start date;
- minimum connection-time filtering.

If the morning job was incomplete, `AYCF_ALLOW_LIVE_FALLBACK=true` (default) permits a live authenticated fallback. Set it to `false` for strict database-only behavior.

## Optional tuning

```bash
AYCF_PDF_URL=https://multipass.wizzair.com/aycf-availability.pdf
AYCF_REFRESH_SECONDS=21600
AYCF_LIVE_CACHE_SECONDS=300
AYCF_MIN_REQUEST_DELAY=1.0
AYCF_MAX_RESULTS=100
AYCF_MAX_PATHS_PER_DAY=250
AYCF_ALLOW_LIVE_FALLBACK=true
```

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium

export FLASK_SECRET_KEY="dev-secret"
export AYCF_APP_PASSWORD="dev-web-password"
export AYCF_ADMIN_TOKEN="dev-admin-token"
export AYCF_SESSION_ENCRYPTION_KEY="<generated-fernet-key>"
python app.py
```

To run the morning job manually:

```bash
python morning_scan.py
```

To force rescanning the same PDF while testing:

```bash
AYCF_FORCE_MORNING_SCAN=true python morning_scan.py
```

## Streamlined Termux operation

The Android/Termux deployment is designed to be low-touch and resilient. Normal scans use the encrypted Wizz session and official AYCF PDF route catalogue. When authentication or a captured Wizz availability endpoint expires, the runtime attempts automatic repair before requiring browser attention.

Common commands:

```bash
source ~/.config/aycf/env
python termux/runtime.py morning
python termux/runtime.py status
python termux/runtime.py repair
python termux/runtime.py web
```

A convenience wrapper is also available without relying on executable file permissions:

```bash
bash termux/aycf run
bash termux/aycf status
bash termux/aycf repair
bash termux/aycf logs
```

The renewal order is deliberately browser-independent: reuse/validate the encrypted session, repair a rotated endpoint, perform direct HTTP login from the encrypted credential vault, and use Android Chrome/ADB only for initial capture, unsupported login changes, or interactive security challenges such as CAPTCHA/MFA/passkeys.

All Termux scan entry points share a single-run lock and persistent status. Scheduled, manual, and web-triggered scans cannot overlap; duplicate launches return cleanly instead of spawning more workers. Runtime state is written under `~/.local/share/aycf/`, including `scan-status.json`, `wizz-session-status.json`, the SQLite cache, and logs. See `TERMUX.md` for details.

## Tests

```bash
python -m unittest discover -s tests -v
```

GitHub Actions runs the regression suite on the feature branch and pull requests.

## Security model

The app stores Wizz browser/session state encrypted with Fernet. The optional Termux unattended-login path may also store Wizz credentials in a separate encrypted credential vault when explicitly configured. The web UI is password protected, login/scan forms use CSRF protection, redirects are restricted to local paths, and sensitive files are written with restrictive permissions. Never commit your Fernet key, app password, admin token, database, encrypted credentials, or encrypted Wizz session.

## Important limitations

- The official PDF itself warns that its information is correct at publication time and availability may change later; the morning database is therefore a fast snapshot, not a booking guarantee.
- Wizz may require CAPTCHA, MFA, passkeys, or another interactive security challenge; automation deliberately stops rather than bypassing those controls.
- The full morning scan is intentionally throttled to reduce rate-limit pressure and can take a substantial amount of time.
- One-stop itineraries are self-transfers; baggage, immigration, delays and missed connections remain your responsibility.
- The current route builder supports direct and one-stop itineraries, not two-stop routing.
- This project is not affiliated with Wizz Air.

## Install AYCF and the Admin Hub as phone apps

The Termux deployment can now be installed as two standalone Android apps while the existing services continue to run in Termux:

1. Open the AYCF local URL in Chrome and choose the in-app **Install app** button (or Chrome's **Install app / Add to Home screen** menu).
2. Open the Admin Hub local URL and repeat the same step.

No extra install command is required after auto-deploy. The icons are launchers for the existing Flask services; ports, passwords, scan data, service controls and the deploy/termux workflow are unchanged. If a backend is stopped, its installed app shows a short offline message. Start it from the installed Admin Hub, then reopen it. Private pages and live scan responses are not cached.
