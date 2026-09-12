# AYCF Live Trip Scanner

## Scan exclusions

Use **Planner → Choose scan exclusions** to tick countries, cities/airports or
individual airport pairs you do not want scanned. Each exclusion applies in both
directions, including hub legs, automatic priority-region coverage and watched
routes. Airport codes and aliases match the same airport; excluding Gatwick does
not exclude Luton. Country rules apply to the offline mapped airport catalogue;
unknown names remain available under **Other / unmapped** for individual exclusion.

The page previews airport/date request counts and approximate time for the PDF's
actual departure window, without making Wizz requests. If the window is unavailable,
it labels a four-day estimate. Save, or untick and save to re-enable later.
Saved exclusions remain editable if an airport or route disappears from today's
PDF. Existing “All except selected” choices are included in this editor.

Airport aliases share one checkbox. Inherited country and city exclusions are shown
beside affected airports and routes; use **Show excluded places and routes only**
to review them quickly. Unsaved choices survive a failed save, and a stale form
cannot overwrite exclusions saved since that page was opened.

Changes apply to the **next scan**; a running scan keeps its starting settings.
Changing exclusions gives the scan a new cache identity, so run a fresh scan before
using the updated flight results. Watches pause instead of reporting excluded or
stale coverage. Recommendations filter every leg; Stability hides excluded routes
by default with an option to view their retained history. Preferred destinations
and historical observations are never deleted by exclusion changes.

Both morning workers now queue by departure date, selected/watched priority,
regional priority, then base/hub tier and route name. Preferred hub legs for today
no longer wait behind all future-day base checks. Estimates and workers share the
same concrete airport requests. Settings are stored in the existing local
`scan_scope.json`, alongside origins, hubs and worker count.

A personal Flask scanner for Wizz Air All You Can Fly (AYCF). The normal user-facing search is database-first: shortly after Wizz publishes the official daily AYCF PDF, a scheduled worker checks the configured route/date scope against your authenticated Multipass session and stores the normalized results in SQLite. Interactive searches then build direct, one-stop and two-stop itineraries from that morning cache instead of repeating hundreds of Wizz requests.

## Morning architecture

1. The Termux runtime runs `tiered_morning.py`; the standalone worker is `morning_scan.py`. Both download Wizz's official `https://multipass.wizzair.com/aycf-availability.pdf` directly.
2. `direct_pdf.py` extracts the PDF's `Last run`, departure window and advertised route table.
3. The PDF publication is fingerprinted. If that exact run was already scanned, the worker exits immediately.
4. Both workers share the route plan, exclusions and priorities. Termux uses bounded parallel workers with global throttling; the standalone worker checks sequentially.
5. Both positive results and zero-flight checks are stored in SQLite.
6. The Flask UI uses the completed cache for the selected PDF and scope. Interactive search makes no live Wizz requests.

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

export AYCF_APP_URL="https://your-deployed-app.example"
export AYCF_ADMIN_TOKEN="the-same-admin-token-configured-on-the-server"
python login_wizz.py
```

`login_wizz.py` requires HTTPS for remote deployments. The server validates the resulting authenticated session before encrypting it with Fernet. Your Wizz username/password are never stored by this app unless you explicitly opt into the Termux encrypted credential vault described below.

## Interactive search

The web UI normally uses the latest morning database snapshot, so searches should make **zero live Wizz requests**. It supports:

- direct routes;
- one-stop and two-stop self-transfer combinations;
- multiple starts/destinations, Anywhere, and destination-only discovery in the full Termux console;
- a separate return start date;
- default 48-hour maximum individual layover and any total journey duration;
- results filters that narrow loaded journeys without rescanning; Reset restores the defaults.

Search requires a completed cache matching the current scope. Changing exclusions, watches or preferred destinations can require a new scan. Result and path limits still bound discovery; the results page warns when its loaded result limit is reached.

## Optional tuning

```bash
AYCF_PDF_URL=https://multipass.wizzair.com/aycf-availability.pdf
AYCF_REFRESH_SECONDS=21600
AYCF_LIVE_CACHE_SECONDS=300
AYCF_MIN_REQUEST_DELAY=1.0
AYCF_MAX_RESULTS=100
AYCF_MAX_PATHS_PER_DAY=250
```

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

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

Inbound checks run before equivalent outbound checks within each released date and destination priority. Smart refresh treats returns to configured origin airports as high-value checks. The Short trips return-coverage panel separates successful empty checks from unverified checks, by date, before trip-time filters. Malformed availability responses and wallet redirects remain unknown instead of being saved as empty results. Wallet redirects first warm the authenticated session and rediscover the endpoint, then retry once. Session repair requires a validated availability response; a wallet-only response is insufficient. Before a full scan, up to three distinct scoped routes are probed. If none yields validated availability, the app requests a fresh successful browser search through `bash termux/connect-wizz-chrome.sh` and does not start the full workload; persistent route redirects leave checks pending while the scan continues, and the run is reported as partial rather than complete. Login redirects still require authentication recovery. Grouped routes attempt every configured airport and retain successful flight results even when another airport remains unknown. Partial groups remain eligible for retry, and Short trips can use their verified flights with an incomplete-coverage notice. Existing cache rows migrate as complete; this update does not reset the scan identity. Auth preflight validates flight data and uses today for an undated probe; workers inherit the endpoint and cookies recovered by preflight.

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
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

GitHub Actions runs the regression suite on pull requests and pushes to main or deploy/termux. See [the application review](docs/APPLICATION_REVIEW.md) for findings and remaining opportunities.

## Security model

The app stores Wizz browser/session state encrypted with Fernet. The optional Termux unattended-login path may also store Wizz credentials in a separate encrypted credential vault when explicitly configured. The web UI is password protected, login/scan forms use CSRF protection, redirects are restricted to local paths, and sensitive files are written with restrictive permissions. Never commit your Fernet key, app password, admin token, database, encrypted credentials, or encrypted Wizz session.

## Important limitations

- The official PDF itself warns that its information is correct at publication time and availability may change later; the morning database is therefore a fast snapshot, not a booking guarantee.
- Wizz may require CAPTCHA, MFA, passkeys, or another interactive security challenge; automation deliberately stops rather than bypassing those controls.
- The full morning scan is intentionally throttled to reduce rate-limit pressure and can take a substantial amount of time.
- One-stop itineraries are self-transfers; baggage, immigration, delays and missed connections remain your responsibility.
- Journey discovery supports at most two stops and uses only flights in the selected scan cache.
- This project is not affiliated with Wizz Air.

## Install AYCF and the Admin Hub as phone apps

The Termux deployment can now be installed as two standalone Android apps while the existing services continue to run in Termux:

1. Open the AYCF local URL in Chrome and choose the in-app **Install app** button (or Chrome's **Install app / Add to Home screen** menu).
2. Open the Admin Hub local URL and repeat the same step.

No extra install command is required after auto-deploy. The icons are launchers for the existing Flask services; ports, passwords, scan data, service controls and the deploy/termux workflow are unchanged. If a backend is stopped, its installed app shows a short offline message. Start it from the installed Admin Hub, then reopen it. Private pages and live scan responses are not cached.

## My places

Open **My places** in the planner navigation (or `/places/`) to keep a personal
travel journal. Add countries/territories, towns, regions or sights; mark entries
visited or wishlist; and keep optional visit dates and notes. Search and filter
the journal, click countries on the bundled world map, or use Add a place for
small islands. Country entries count as one visited place, as do individual
city/sight entries. Country and continent totals are deduplicated; wishlist
entries do not count as visits. No travel history is populated automatically.

Records use `travel-journal.sqlite3` beside the configured `AYCF_DB_PATH`, so
Termux stores them in the existing persistent state directory. Override with
`AYCF_JOURNAL_DB_PATH` if needed. Back up that SQLite database with your other
personal data; **Download records** also exports the entries as JSON. The page
uses the existing AYCF login and CSRF protections. It needs the local AYCF server
running, but the map and country catalogue do not require an external map API.
See `static/places-map-source.txt` for map provenance and coverage limitations.

### Bounded UK connection coverage

In Scan exclusions, nominate **connection airports** and set an extra-check budget
from 0 to 100 (default 100; no extra coverage until airports are selected). An airport
can stay excluded as a place to visit while being permitted for transit. Country
and route exclusions always remain hard vetoes. Other excluded airports remain
blocked for both visits and transit.

Extra candidates require two directed edges in the current PDF: a selected UK
airport to/from the connection airport, and a link to a preferred destination or
configured hub. Preferred destinations rank first, then inbound before outbound.
The planner adds complete candidate pairs, counts concrete airport requests across
the released dates, reuses routes already in the base plan, and defers candidates
that do not fit. The budget counts checks, not HTTP retries or login requests.
The preview and worker log report extra checks and deferred candidates.

This version plans a bounded set across the released window in the same scan;
it does not yet schedule connector checks dynamically after a UK flight response.
Flight times are validated by Short trips, including transfer limits and a stay
longer than 24 hours. Select at least one stop to see connections. Excluded
connection-only airports cannot become the trip's destination.

### Airport route directory and scan progress

The Chrome connection importer now saves directed airport pairs from the authenticated Multipass route menu. Open **Airport routes** from Short trips to inspect the snapshot. Run `bash termux/connect-wizz-chrome.sh` and perform the requested successful Wizz search to capture or refresh it. Existing captures do not contain this directory.

A valid snapshot narrows current-PDF route variants for departure airports with complete, non-empty route-menu rows. For a generic PDF city such as London, departure expansion uses only directory-covered members when at least one selected member is covered. This avoids inventing Stansted routes from a London label. Missing members are unconfirmed, not verified unavailable. Explicitly named departure airports and wholly uncovered cities retain fallback checks. Missing origins, malformed rows, failed captures and snapshots older than seven days do not establish exclusions for explicit airport requests. Return directions are evaluated independently; the directory never proves seat availability or adds unrelated PDF routes. Route mappings and the planning policy participate in scan identity so changed coverage cannot reuse an incompatible completed scan. Capturing identical mappings does not reset the scan.

The offline airport catalog mirrors the physical departure airports in Wizz's station picker captured on 12 September 2026. City-wide entries labelled **All Airports** are deliberately excluded. A resolved airport removed from that picker is not expanded into scan requests; an unknown PDF label remains eligible so a catalog gap cannot silently discard it. The authenticated directed route snapshot remains the finer source for deciding which pairs exist.

Parallel progress reports processed route/date groups, complete and partial groups, verified and unknown airport checks, and actual HTTP requests separately. A partial group counts as processed, and throughput measures processed groups. Pending messages include route/date context. After a worker has already recovered its wallet session, further wallet redirects remain pending without repeating the identical request immediately; later scans can retry them.
