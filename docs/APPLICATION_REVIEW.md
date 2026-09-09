# AYCF application review — 9 September 2026

Reviewed against `deploy/termux` at `cedd782`, including the merged 48-hour journey search.
The review covers planner/search, scan scope and cache identity, scheduled scanning,
Termux launch/deploy flows, status/logging, templates, and documentation. Verification
uses isolated databases, mocked Wizz responses, and local HTTP servers; it does not
establish the phone's installed version, Android scheduler behaviour, or live Wizz access.

## Changes in this refinement

| Finding | Change and practical effect |
| --- | --- |
| `/scan` and `/multi-scan` repeated validation, defaults, presentation and ranking; multi-search's numeric score could place a short tight connection ahead of a longer safe one. | Share those helpers. Both use two stops, 48 hours per layover and any total duration; ranking consistently prioritises fewer stops, then safer connections, then shorter journeys, matching the browser. |
| Invalid multi-search origins could silently become destination-only discovery; malformed limit environment values could cause a server error. | Reject invalid submitted airports before discovery. Parse limits with bounded defaults in both endpoints. |
| PDF/scope cache identity was independently constructed in four places. | One helper preserves the existing identity bytes and route order, so this refactor itself does not require a new scan. |
| The planner's inventory could show another scope or fall to zero when a refresh failed despite retained cached flights. | Count retained flights for the selected PDF/scope instead of using the latest scan attempt's counter. |
| `runtime.py web` created a smaller app than the usual `watch_app.py` launcher. | Both use one full-console factory, including Watches, Stability, Places, scan settings and multi-search. Runtime patches install once per process. |
| Handled Wizz authentication/service failures returned JSON but exited successfully. | Failed scan results now return a nonzero process exit so the supervisor records a pending retry rather than success. |
| Overlapping supervisor launches could duplicate health/repair work; wake timestamps were only updated on some paths. | Serialize supervisor cycles with an OS lock and record each acquired wake, including cooldowns and scan-busy states. |
| Deployment used an orphanable directory lock, ignored only the admin's health on a current checkout, and treated executable-bit changes as local edits. | Use an OS-released lock, load configured ports, verify both services, start a missing planner on a current checkout, and ignore permission-only differences while protecting content changes. |
| Health UI read entire logs and could prioritise an obsolete Chrome/ADB failure over a healthy saved Wizz session. | Read at most 64 KiB per log and show browser fallback as unnecessary when the saved session is healthy. A service outage no longer appears operational. |
| Background launch log handles remained open in the parent web process. | Close the parent's handles immediately after spawning; the child retains its own descriptors. |
| Five root HTML copies were unused by Flask; home-page cards repeated counts and explanations already shown above. | Remove obsolete copies and repeated cards. Keep active templates under `templates/`. Rename the flight-specific Admin tab to System. |
| README described one-stop-only routing, optional interactive live fallback and incomplete test commands. | Update instructions to match the actual cached search and test runner. |

## What still has a distinct purpose

- **Planner:** combine cached flights into journeys.
- **Flights:** inspect individual flights and route/date coverage without itinerary constraints.
- **Watches:** notify about individual routes/date ranges, independently of journey search.
- **Stability / Recommendations:** historical likelihood and suggested trips, not current-seat confirmation.
- **Scan exclusions:** control what network requests are made. Result filters only change displayed journeys.
- **System:** scan/authentication health. **Admin Hub:** start and manage multiple local applications.
- **My places:** a travel journal, separate from live availability and scanning.

These are related but not interchangeable. Removing Flights, Watches, or Stability
would remove useful capabilities rather than just reduce clutter.

## Recommended next work

1. **Reuse unchanged route checks after a scope change.** Currently a changed scope
   gets a new identity and may repeat many unchanged checks. A safe migration should
   copy only same-publication, sufficiently fresh checks with identical concrete-airport
   request coverage; it must preserve zero-flight evidence, exclusions and watch semantics.
   This offers a direct way to reduce scan time, but needs its own migration tests.
2. **Paginate cached journeys.** Current filters operate on the loaded result set,
   bounded by configured result/path limits. Pagination or a cached search session
   would expose more matches without raising phone memory use or making Wizz requests.
   A filter yielding no loaded matches is not proof that no matching itinerary exists.
3. **Avoid repeated unchanged analytics work.** Scheduled already-current scans still
   check history, rebuild Stability scores and process watches. Keep watch delivery
   retries, but key analytics refreshes to scan/history/schema changes and a daily
   freshness boundary. Do not skip a failed previous rebuild or a new archive import.
4. **Unify scan settings gradually.** Preferred destinations, base/hub coverage and
   exclusions serve different purposes, but the legacy destination-exclusion selector
   overlaps the dedicated checkbox editor. Migrate that setting before removing it;
   silently changing existing destination coverage would be incorrect.
5. **Backup and retention.** Local scan/history databases and logs grow over time.
   Define a recoverable backup and retention policy before pruning data. The travel
   journal and encrypted session/credentials must be kept outside any cache cleanup.
6. **Retire compatibility code after checking external use.** `morning_scan.py` remains
   the documented standalone worker while Termux uses `tiered_morning.py`.
   `cache_db.cached_scan_itineraries`, older planner code and recovery scripts also
   remain potential compatibility entrypoints. Consolidate them after validating their
   callers and deployment paths; this change only removes proven-unused HTML copies.

## Limits of this review

The updater verifies local service health; its “validated branch” label does not
itself enforce GitHub CI success. Branch protection or an explicit CI gate would
be needed to guarantee that every deployable commit passed checks. Deployment
still defers during a detected morning scan, and some legacy scripts retain their
own process-management logic. A full service-manager consolidation is separate work.

## Validation of this change

- Python 3.12: **261 tests passed, 1 skipped**. The skipped module requires Playwright;
  real-browser screenshot validation was not run in this environment.
- New regressions exercise search validation/ranking, HTTP defaults, current-scope
  counts, full-console startup, runtime exit codes, supervisor serialization, status
  freshness, bounded log reads, and the actual deployment shell against a disposable
  Git checkout and local health servers.
- Existing exclusions, exact-airport, 48-hour boundary, two-stop journey, notification,
  history, auth-recovery and Termux tests pass.
- Python compilation, Termux shell syntax checks and `git diff --check` pass.
