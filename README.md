# AYCF Live Trip Scanner

## Manchester connections

Open **More → Manchester connections** to combine a Ryanair/easyJet flight
from MAN costing **less than £40 per adult** with up to two onward AYCF flights
to Egypt, Saudi Arabia, Georgia or Armenia. These are outbound, separate-ticket
opportunities; return flights and baggage/stopover costs need their own checks.
Only exact-airport connections with recent saved availability are matched.

The page works with manually checked fares. Optional Google Flights searches
use a free SerpApi account: add `export AYCF_SERPAPI_KEY='your-own-free-key'` to
the existing `$HOME/.config/aycf/env` (or your `AYCF_CONFIG_DIR` environment file)
and restart AYCF through the Hub. Keep the key private and use the free plan;
this integration never upgrades an account or purchases credits. Hosting users
can set the same environment variable in their service configuration.

Automatic checks are enabled once your key is configured. The existing Termux
supervisor checks whether a batch is due on its approximately 15-minute wakes,
and a completed or usable partial scan also triggers a due check. Each batch
checks up to two useful hub/date pairs, with at least six hours between batches.
Automatic requests are capped at six per rolling 24 hours, within the shared
limits of eight attempts per 24 hours and 220 per 31 days. This leaves room for
manual **Check fares** requests. No new Android schedule or always-running web
thread is required. Android can delay scheduled wakes.

The automatic shortlist uses only current, recently verified AYCF connections
to the four target countries. Direct Wizz legs, repeated recent availability,
and hubs serving more target countries get priority. A stable-history label
requires at least three distinct positive observation days and availability on
at least 60% of checked days for each leg, using recent 30–60 day exact-airport
history. Repeated scans/reused cache rows do not add observation days; grouped
city evidence cannot establish a physical-airport success rate. With limited
history, useful current connections remain eligible but are labelled accordingly.
This measures route recurrence, not an on-time record or guaranteed connection.
When available, one slot in each batch explores an unchecked hub/date so repeated
refreshes of the highest-ranked hubs do not consume the whole allowance.

Pause/resume background checks on **Manchester connections**, where the last
batch, next eligible time and selection reasons are visible. Opening/filtering
the page never makes fare requests. Manual responses are cached for an hour;
automatic checks wait six hours before retrying positive/error checks and a day
before repeating an empty result. Provider errors stop the batch and preserve
saved quotes. Pausing does not reset usage or cooldowns. Other uses of the same
SerpApi account consume its shared allowance too. Live provider access requires
your key and has not been verified by the fixture tests.

If a fare check fails, use **Check the useful hubs → Test SerpApi connection**
on the same page. This checks the key loaded by the running app and reports the
remaining account searches through SerpApi's [free Account API](https://serpapi.com/account-api),
without using a flight-search credit. It never shows the key or account email.
Restart AYCF after changing the environment file. A successful account check
does not guarantee flight results for a particular route/date.

Fare errors distinguish rejected keys, account access, quota/rate limits,
connection timeouts and provider failures. Saved quotes survive failed checks;
manual retries have a five-minute cooldown, while automatic retries retain their
six-hour schedule. A completed search with no matching fares is an empty result,
not a failed request or proof that the route has no flights. Flight requests
allow 10 seconds to connect and 45 seconds to read a response, without retries.

Cash quotes live in `feeder_quotes.sqlite3` beside the existing AYCF database.
They never become Wizz availability or change scan statistics. Quotes older
than six hours and AYCF records older than 24 hours are omitted from matching;
advanced deployments may configure `AYCF_FEEDER_FARE_MAX_AGE_HOURS` and
`AYCF_FEEDER_WIZZ_MAX_AGE_HOURS`. Recheck both airlines before buying: a cheap
feeder does not reserve the onward AYCF seat.

See [free-data research and limitations](docs/FREE_FLIGHT_DATA_RESEARCH.md) for
Google's official API, scraping, Ryanair/easyJet sources and provider comparisons.

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
Changing exclusions gives the scan a new cache identity, so resume a scan before
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

Inbound checks run before equivalent outbound checks within each released date and destination priority. Return routes retain their scan priority while successful checks are reused for the UTC day. The Short trips return-coverage panel separates successful empty checks from unverified checks, by date, before trip-time filters. Malformed availability responses and wallet redirects remain unknown instead of being saved as empty results. Wallet redirects first warm the authenticated session and rediscover the endpoint, then retry once. Session repair requires a validated availability response; a wallet-only response is insufficient. Before a full scan, up to three distinct scoped routes are probed. If none yields validated availability, the app requests a fresh successful browser search through `bash termux/connect-wizz-chrome.sh` and does not start the full workload; persistent route redirects leave checks pending while the scan continues, and the run is reported as partial rather than complete. Login redirects still require authentication recovery. Grouped routes attempt every configured airport and retain successful flight results even when another airport remains unknown. Partial groups remain eligible for retry, and Short trips can use their verified flights with an incomplete-coverage notice. Existing cache rows migrate as complete; this update does not reset the scan identity. Auth preflight validates flight data and uses today for an undated probe; workers inherit the endpoint and cookies recovered by preflight.

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

## Standalone Places

Open **Places** from the Personal Hub. The old My Places page and API are no
longer included in AYCF. Existing `travel-journal.sqlite3` files are preserved;
removing the page does not delete or modify your travel records.

The Hub's Places installer can migrate the old database into the separate app.
For a custom source, run `places migrate --source /path/to/travel-journal.sqlite3`.
Migration is read-only on the source and safe to repeat. Keep your original
backup until you have checked the records in Places.

### Bounded UK connection coverage

In Scan exclusions, nominate **connection airports** and set an extra-check budget
from 0 to 100 (default 100; no extra coverage until airports are selected). Airport, country and route exclusions always take precedence, including for
connection airports. Remove an airport exclusion to allow its connections.
Saved connection selections are retained while excluded, but generate no requests.

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

A valid snapshot narrows current-PDF route variants for departure airports with complete, non-empty route-menu rows. For a generic PDF city such as London, departure expansion uses only directory-covered members when at least one selected member is covered. This avoids inventing Stansted routes from a London label. Missing members are unconfirmed, not verified unavailable. Explicitly named departure airports and wholly uncovered cities retain fallback checks. Missing origins and malformed rows do not establish exclusions for explicit airport requests. A successful snapshot is persistent structural data: it remains active regardless of age and failed maintenance refreshes preserve it. A later successful capture replaces it only with validated route-menu data. Return directions are evaluated independently; the directory never proves seat availability or adds unrelated PDF routes. Route mappings and the planning policy participate in scan identity so changed coverage cannot reuse an incompatible completed scan. Each topology has a fingerprint and revision; capturing identical mappings refreshes maintenance metadata without resetting the scan.

The offline airport catalog mirrors the physical departure airports in Wizz's station picker captured on 12 September 2026. City-wide entries labelled **All Airports** are deliberately excluded. A resolved airport removed from that picker is not expanded into scan requests; an unknown PDF label remains eligible so a catalog gap cannot silently discard it. The authenticated directed route snapshot remains the finer source for deciding which pairs exist.

Parallel progress reports processed route/date groups, complete and partial groups, verified and unknown airport checks, and actual HTTP requests separately. A partial group counts as processed, and throughput measures processed groups. Pending messages include route/date context. After a worker has already recovered its wallet session, further wallet redirects remain pending without repeating the identical request immediately; later scans can retry them.

### Wizz rate limits and automatic recovery

An HTTP 429 pauses Wizz work immediately and saves a `rate_limited` status with
the earliest retry time. The first cooldown is at least 15 minutes, or longer
when Wizz supplies a longer `Retry-After`. Further rate-limit episodes increase
the local wait to 30 minutes, one hour and onwards up to six hours; a longer
server deadline is never shortened. Responses from requests already in flight
can extend the deadline without counting as separate retry episodes.

Scans default to three workers, with request starts spaced one second apart
across all workers and processes, subject to the rolling request budget below. After a rate limit, the minimum recovery
spacing is two seconds, increasing to 3, 5, 10, 20 and 30 seconds on subsequent
episodes. A slower configured interval remains respected. This pacing survives
restarts and stays elevated until 24 hours without a new 429 and no active
cooldown. Restarting, forcing a scan or repairing authentication cannot reset it.

A persistent rolling budget prevents sustained bursts across restarts: at most
40 managed attempts in any 60 seconds normally, or 20 during the existing
24-hour recovery period. These are local precautions, **not published Wizz
quotas** or a guarantee against throttling. Availability, session preflight,
authentication and retries share the budget. When it is full, workers wait for a
slot, rechecking cooldowns every second. This is logged as a local budget pause;
it never invents a 429 or extends the provider cooldown. Three workers still
handle responses concurrently, but cannot multiply the shared allowance.

System health shows managed attempts over 1 minute, 15 minutes and 24 hours;
the Hub shows the current request budget. Logs and cooldown guidance distinguish
valid numeric/date `Retry-After`, missing/invalid headers, and whether the wait
came from Wizz, our backoff, both, or a retained earlier deadline. The last 20
429 diagnostics are retained in the same SQLite file. Only operation labels,
parsed timing and counters are saved, never URLs, request bodies, credentials,
cookies or raw headers. History starts after this update and excludes browser
traffic. An admission reserved immediately before dispatch counts conservatively
even if another process records a cooldown before the HTTP send. Request history
is pruned to 24 hours on admission; expired rows are always excluded from counts.

To keep the phone on three workers even when an older saved scope selected more,
add `export AYCF_SCAN_WORKERS='3'` to `~/.config/aycf/env` (or the `env` file in
your configured `AYCF_CONFIG_DIR`). It applies when the next scan process starts.
An existing cooldown is retained; no scan cache or rate-limit state reset is
needed to adopt the new recovery spacing after updating. Three workers overlap
response handling; they do not each receive a separate request allowance.

The Hub and System health show the retry time. Scheduled/manual scans, saved
session probes and automatic browser renewal all wait during the cooldown.
Throttling does not mark the saved login as expired. The next eligible supervisor
wake resumes unfinished work, including outside the morning window. Completed
checks and known flights stay in SQLite; cached planning and separate SerpApi
fare checks remain available. Android can delay the next wake.

The shared state is `wizz-rate-limit.sqlite3` in `AYCF_STATE_DIR` (default
`~/.local/share/aycf`). Advanced deployments may set `AYCF_WIZZ_RATE_LIMIT_PATH`;
every AYCF process must use the same path. No scheduler reinstallation is needed.
Fixture tests verify recovery without sending live Wizz requests; these waits
cannot guarantee that Wizz will accept the next request.


### Personal Hub and standalone Places on Termux

The phone's integration branch is `deploy/termux`. The Hub provides an Apps launcher at `/` and service controls, custom actions and device summaries at `/manage`. Local loopback access, password overrides and the installed Hub PWA remain supported. `/health` checks the Hub itself without waiting for downstream apps.

Places is a separate app. Its Start action installs the current Places `main`, migrates the existing AYCF journal when present, and starts the service. Merge the Places runtime/migration fixes before using this handoff. Existing AYCF journal data remains available in AYCF; the two journals are not continuously synchronized.

The Hub adds missing default apps while preserving local registry entries and custom actions. If Media Hub or another app already uses port 8084, install Places with a free port (`PLACES_PORT=8094 bash termux/install.sh` from the Places checkout); Places registers the selected port with the Hub.

### Admin Hub app controls and manual updates

Open **System management** in the local Admin Hub for Start, Stop, Restart and **Pull latest & restart** on each built-in app. The AYCF update also updates/restarts the Admin Hub. Updates run in a detached worker, survive the Hub restarting, and show a persisted result and an update log. The management page refreshes while a job runs. Duplicate update clicks are locked per app, and runtime controls wait for the app's update to finish.

The updater uses each app's existing deployment script and branch: AYCF/Hub `deploy/termux`, Places/Pocketwise/Sunscape `main`, and Media Hub `master`. It checks for local tracked edits before updating and never resets them. AYCF's existing active-scan and deployment-lock deferrals remain in effect. Failed commands and failed post-update health checks are shown as errors rather than successful updates. Custom registry ports and checkout directories are retained. These controls manage the local service; Stop does not disable an app's independent boot or automatic deployment schedule.

Runit-managed apps use supervisor status and explicit service directory paths for their controls, avoiding stale process-name detection. Places uses its installed launcher, which loads its saved configuration. Local authentication and CSRF protection apply to update actions, just as they do to start/stop.

### Scan reliability and diagnostics

All managed Wizz requests share a five-second minimum interval and a local
12-attempt rolling minute budget, including authentication and retries. These are
local precautions, not published provider quotas. Repeated 429 episodes retain
their increasing cooldowns and can further slow requests. A restart, login repair
or quiet day cannot reduce pacing below five seconds.

Automatic Termux scanning stops after one successful scan per UTC calendar day
(the same timezone as the publication window). A manual completion also satisfies
that day's automatic scan. This survives restarts, status changes, expired cache
entries and changes to the PDF or scope; those changes wait for tomorrow unless
you request a manual rerun. Failed, partial or interrupted scans retry until they
succeed, including outside the morning window. Explicit manual/fresh reruns remain
available after success and can resume after failures or provider cooldowns.

Normal manual and automatic scans reuse successful airport/date checks for the
UTC verification day, including confirmed empty results. Short freshness timers
and the legacy `AYCF_MANUAL_REFRESH_TTL_SECONDS` setting do not override this rule.
An additive SQLite ledger retains each concrete airport check immediately, so a
restart or a partial city-group retry requests only unfinished airports. Existing
complete checks with proven physical coverage seed the ledger without changing
observation timestamps. Current-PDF membership and exclusions still decide which
pairs are eligible; matching verified pairs can be reused after a scope or PDF
change. Yesterday's observations, failed requests and unknown responses cannot
establish today's availability. Old partial rows without per-airport proof must
be verified once; the new ledger preserves successes thereafter.

**Clear pending & resume** backs up the database, clears failed/pending scheduling
records and retains all verification markers and saved flights. During a cooldown
it queues the resume without changing Wizz's deadline. The legacy clear-button
URL also uses this safe behavior. CLI: `python termux/runtime.py pending`.
**Full rescan** is separate and requires an explicit confirmation checkbox; its
CLI equivalent is `python termux/runtime.py fresh`. Only this deliberate action
invalidates current/future verification, once, while retaining saved flights and
creating a backup. It does not bypass cooldowns or active scan locks.

An isolated exhausted 5xx request, or the specific HTTP 400 `PASS-0000` /
`wallet.error.generic` / `cyf.flights.FlightSearchException:` backend failure,
leaves that airport check pending while other
airports and routes continue. Preflight also tries another distinct scoped route
(up to three probes) after a server error. Logs identify the route, date and HTTP
status without exposing captured URLs or response bodies. Failed checks are never
saved as verified empty availability. Three consecutive exhausted server failures,
or five among the last ten checked requests, pause the scan; the threshold is
shared across workers, with already-started requests allowed to settle. An
unverified preflight with server errors pauses for service recovery rather than
assuming the login expired. HTTP 429, authentication and request-rejection stops
retain their existing behavior.

Partial scans and service pauses preserve results and display a next-retry time
(15 minutes by default). The supervisor waits for that deadline without launching
authentication repair. Use **Run AYCF morning scan** to resume unfinished work;
**Clear pending & resume** also clears stale retry records without repeating successful checks.
System status shows live airport-based ETA,
completed groups and flight counts. Diagnostic exports include the latest run's
ID, worker revision, start/end state, progress and effective pacing, separately
from the installed revision and historical logs.

The airport directory is refreshed from an authenticated wallet response during
scheduled session health checks when it is at least a day old. Captures from
normal bootstrap and login flows also update it. Invalid/absent menu data leaves
the previous valid directory intact; the existing seven-day expiry remains.
System status and diagnostics show its age and whether refresh is due.
