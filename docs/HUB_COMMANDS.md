# Personal Hub commands and Places separation

The Hub runs in the AYCF repository on `deploy/termux`. Places is a separate
application; AYCF no longer registers its old `/places/` page or API. The old
journal database is preserved for backup and the existing read-only migration.

## Verified command contracts

These match the source launchers reviewed on 16 September 2026. `$APP_ROOT`
means the AYCF checkout; app directories and ports come from the merged registry.
The Manage page exposes the effective commands under **Commands & paths**.

| App | Start / stop / restart | Update | Default port |
| --- | --- | --- | --- |
| AYCF | Start `bash termux/run-web.sh`; the legacy process manager stops/restarts `watch_app.py` | `bash termux/auto-deploy.sh`, `deploy/termux`; includes Hub | 8080 |
| Sunscape | Runit `up` / `down` / `restart` using the full `sunscape` service path; trusted installer when absent | `bash termux/update-service.sh`, `main` | 8081 |
| Pocketwise | Runit `expense-manager`; trusted installer when absent | `bash termux/auto-deploy.sh --once`, `main` | 8082 |
| Media Hub | Runit `mediahub`; trusted installer when absent | `bash termux/update.sh`, `master` | 8083 |
| Places | Registered launcher with `status`, `start`, `stop`, `restart` | The same launcher with `update`, `main` | 8084 |

Status, direct start, command-manager controls, installers, custom actions and
updates now receive a shared environment. It includes the registered checkout,
app-specific port and the Hub registry path. AYCF and Sunscape also receive
`PORT`, as expected by their web launchers. Supervised services retain their
persisted service configuration; changing a registry port alone does not rewrite
an already installed runit service.

The original app-specific launchers remain authoritative for stored data and
service settings. Places loads its saved `.config/places/env`; Pocketwise and
Media Hub load their own saved environment files. Their installers should be
used when changing those persisted service settings. The Hub shows health and
command errors rather than claiming a mismatched service is healthy.

Custom Sunscape folders, endpoints and commands are preserved. A genuinely old
Node/port-3000 entry still migrates to Flask. A port-only registry override moves
the default open/health URLs together. Explicit custom URLs are retained. Custom
update scripts are not replaced by default update commands.

## Triggers

- **Run morning scan** dispatches `python termux/runtime.py morning` from AYCF's
  registered directory. The scanner's existing OS lock rejects duplicate work.
- Job **2608** invokes `morning-gate.sh`, which invokes `supervisor.py` every
  approximately 15 minutes. Publication windows, pending recovery, request pauses
  and network pacing remain owned by the supervisor/scanner.
- Job **2610** invokes `auto-deploy.sh` approximately every 15 minutes. It preserves
  tracked local changes and defers during active scans.
- Launch and scheduling scripts derive their checkout from their own file
  location unless `AYCF_APP_DIR` is explicitly configured. Custom installations
  no longer fall back silently to `~/aycf-trip-planner`.
- Runtime dispatch and detached updates share per-app locks. Restart-all skips
  stopped apps and cannot interfere with an updating app. Long-running custom
  jobs keep their own existing lifecycle locks after dispatch.

Hub scheduling does not add a new scan cadence, remove request pauses or enable
Wi-Fi-only operation. Android may delay jobs according to device conditions.

## Validation

Tests cover environment propagation for all five apps, custom Sunscape and
Places launchers, effective endpoints, actual scheduling shell arguments,
custom-checkout web launches, retained legacy data and removed Places endpoints.
Chromium tests cover the launcher, app search, navigation, update locks and
horizontal overflow at 390px and 1280px. Existing authentication, CSRF, update,
service-error and scan regressions remain part of the suite.

The physical phone, saved configurations and Android scheduler could not be
queried from this workspace. Passing contract tests verifies application code;
it does not assert that every service is currently running on the phone.
