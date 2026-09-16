# AYCF planner and Personal Hub review — 16 September 2026

Reviewed the Termux deployment line at `b3d12b9` and the pending Hub changes at
`49b9764` (PR #111). The phone uses `deploy/termux`; `main` is not the deployment
baseline. This review extends the existing PR so its update buttons and the
reliability fixes can be installed together.

## Findings fixed

| Priority | Finding and evidence | Result |
| --- | --- | --- |
| High | `itinerary_search._flight_options` enumerated only fully completed route checks. Known flights in a partial airport group could appear in Flights but disappear as onward connections. Extra cached routes were also restricted to complete groups. | Searchable routes now include partial groups containing saved flights. Onward searches inspect dates within the current release and allowed layover. Completion markers and verified-empty semantics remain separate. |
| High | Missing onward dates were only inspected when no onward flights existed, and then only for the arrival day and following day. A 48-hour connection could hide an unchecked third calendar date. | Each eligible route/date lookup contributes to the existing memoized missing-date set. Verified empty dates remain empty rather than missing, and dates outside the release window are excluded. |
| High | Failed captured-request verification became generic `attention_required`, which the supervisor interpreted as authentication failure. This could schedule unnecessary browser repair and repeat requests without fixing their cause. | Persist `request_repair_required` and pause automatic network work until a deliberate retry. Genuine authentication failures retain automatic recovery. HTTP 418 and request-diagnosis outcomes are reflected immediately after the scan, rather than temporarily appearing as retry-pending. |
| High | System health did not include `partial`, `interrupted`, `request_rejected`, or request-diagnosis states. An earlier healthy authentication result could leave “All systems operational” visible. | Show attention status and specific recovery guidance. Saved flights remain usable. Live status responses are not cached. |
| High | A runit status timeout escaped page rendering. A control timeout or OS error returned false, permitting process-based fallback. Start also consulted obsolete process patterns before the supervisor. | Keep Apps and Manage available with an explicit unavailable status. Installed supervisor failures stop the requested action. Supervised start/stop no longer depend on stale process matching. |
| High | Runtime controls checked update status before acting, leaving a race with a simultaneously launched update. Custom action dispatch did not check updates. | Runtime dispatch, restart-all, custom action dispatch and detached updates share the same per-app OS lock. Updating cards disable conflicting buttons; the server enforces the lock independently. |
| Medium | Hub service probes ran serially, accumulating delays from unavailable applications. | Probe up to five apps concurrently while retaining registry order. The Hub health endpoint still avoids downstream probes. |
| Medium | Update-log viewing read the entire file before slicing its tail. | Read at most 16,000 bytes. Dynamic Hub pages use `no-store` so health and control forms are refreshed. |

The planner changes only read saved availability. They do not increase scanner
requests, alter exclusions, merge city airports, change watch delivery, or mark
partial scans complete. Existing authentication checks, CSRF enforcement and
non-loopback password requirements remain in place.

## Review coverage

- **Search and cache:** graph expansion, physical airport compatibility, current
  run isolation, first/onward date windows, missing/empty/partial semantics,
  result limits, SQLite writes and retained inventory.
- **Scanning:** tiered priority jobs, resume/freshness choices, preflight request
  verification, shared rate-limit handling and interrupted scan locks.
- **Automation:** persisted supervisor state, manual repair adoption, retry
  cooldowns, request-rejection handling, scan outcomes and status presentation.
- **Hub:** registry/default merging, command and runit control paths, per-app
  updates, detached worker locks, authentication, CSRF, logs and phone layout.
- **Deployment:** the actual Termux branch, fast-forward/local-change checks,
  scan/deployment deferrals, service health checks and existing shell tests.
- **Adjacent workflows:** existing regression coverage for exclusions, short
  trips, watches, route history, Stability, PWA/navigation and Places handoff.

## Remaining priorities

1. **Unify time interpretation before changing historical data.** The general
   scanner can parse clock-only values as naive local times and converts explicit
   offsets to naive UTC. Termux anchors dates to the requested day. Short Trips
   separately normalizes airport-local clocks. These different paths need shared
   timestamp provenance and real response fixtures, especially overnight flights,
   daylight-saving changes and cross-zone total durations. This review does not
   claim to certify all real-world journey durations or migrate ambiguous rows.
2. **Reuse unchanged checks when scope changes.** Same-run resume works, but a
   new scope identity can repeat overlapping requests. Reuse requires identical
   publication, concrete airport request coverage and freshness; copying by city
   name alone is unsafe. This remains a potentially valuable scan-time reduction.
3. **Reduce repeated analytics maintenance.** Already-current scans still invoke
   history, Stability and watch maintenance. Materialized analytics can be keyed
   to input/schema changes and daily freshness; notification retries must remain
   independent so failed deliveries are not lost.
4. **Bound search expansion and add pagination.** Result/path limits exist and
   the UI warns when the result count reaches its cap. Leg combinations are still
   assembled before result truncation. Instrument realistic phone datasets before
   introducing lazy expansion and pagination; do not promise all possible routes
   from the currently loaded result set.
5. **Improve runtime configuration and deployment diagnostics further.** Several
   service scripts still have their own environment/port conventions, and
   Sunscape's legacy migration can override registry values without a manifest.
   Test custom installations against their actual repositories before broadening
   that migration. Expose installed versions and deployment results consistently.
6. **Backups and device constraints.** Add verified restore support before cache
   or history retention. Wi-Fi-only scheduled work needs an explicit policy and
   Android connectivity tests, including unknown network state; no such policy
   is silently enabled here.

## Validation and limits

- Baseline: 409 tests passed, four skipped without browser support.
- Added regression coverage for partial onward flights at the exact 48-hour
  boundary, missing dates, run isolation, service timeout/OS failures, concurrent
  control exclusion, bounded log reads, truthful health and request-diagnosis
  pauses.
- Added real Chromium checks for the Hub at 390px and 1280px: update buttons,
  disabled runtime controls, service-unavailable display and horizontal overflow.
- Local non-browser suite, Python syntax, shell syntax and diff whitespace checks
  pass. Exact latest counts and GitHub CI results are recorded on PR #111.
- The local Chromium download failed because its installer could not maintain its
  cache lock. Browser execution is therefore delegated to the existing GitHub CI
  job; this is not a claim of local screenshot verification.
- The user's physical Termux device, installed app versions, Android background
  scheduling and live Wizz availability were not accessible. No live scan, phone
  deployment or PR merge was performed by this review.
