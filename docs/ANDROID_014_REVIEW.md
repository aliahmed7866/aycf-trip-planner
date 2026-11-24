# AYCF 0.1.4 refinement

## User-facing changes

- A consistent dark teal theme, labelled navigation icons and a single Android
  header. Phone and tablet navigation remains available at every viewport width.
- Two clear home actions for short trips and Manchester connections; scan setup,
  saved availability and scan settings are grouped without duplicate sections.
- Destination suggestions support airport codes, accented names, keyboard
  navigation, inline validation and clear no-match feedback.
- UK-time weekend shortcuts and local airport/journey preferences. Expired dates,
  destination filters and airports no longer eligible for the scan are not restored.
- Short-trip results show departure, UK arrival and destination time before the
  expandable itinerary. The itinerary can be copied with observation timestamps
  and a reminder to recheck seats. Historical ideas remain labelled unverified.
- Manchester transfer options, scan request diagnostics and logs are collapsed
  until needed. Keyless manual fare checking remains supported.
- Fixed the empty Flights screen's JavaScript error, the PWA install button's
  navigation overlap, and stale status after changing native login settings.
- Native dark styling, page-load progress and a recoverable page-error banner.

## Verification

Unit and browser checks cover the existing planner/scanner/authentication logic,
weekend shortcuts across both UK clock changes, invalid/corrupt preferences,
remembered-login status, navigation, empty data, copying itineraries and responsive
layouts at 320, 390, 820 and 1280 pixels. Browser fixtures use synthetic flight
observations and never contact Wizz or a fare provider.

The Android release build includes the new Python module, templates, scripts and
styles. Live Wizz credentials and the installation's release signing key were not
available during this refinement. An unsigned release is not an installable update;
it must be signed with the existing key. Never uninstall the existing app merely to
work around a signing mismatch, because its local data would be removed.
