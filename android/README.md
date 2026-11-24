# AYCF for Android

The APK runs the AYCF planner, scanner and SQLite databases on the phone. Minimum Android version is 8.0. The default build targets ARM64 phones.

## First launch

Choose **Import Termux backup** on the welcome screen to bring your saved data over, or choose **Start fresh**. Open **Menu → Wizz account**, enter your Wizz email and password, and tap **Save and connect**. AYCF encrypts the login on this phone, checks the wallet, then queues a scan for your selected routes. Flight availability appears under Scan status. If Wizz requests verification, complete it on the Wizz page. **Menu → Flight alerts** opens Android's permission or notification settings. You can check delivery from **Watches → Test notification**.

The planner opens offline. A new installation starts with an empty route list. The first scan downloads the official route PDF. Cached availability keeps its original observation dates. Android schedules network-connected checks approximately every 15 minutes. The scanner uses the existing morning window and saves completed checks to SQLite. Android can defer or interrupt work. Open **Menu → Scan status** to review progress. AYCF renews an expired session with the saved login when Wizz permits it.

## Remember Wizz login

Open **Menu → Wizz account**, enter the Wizz email and password, and choose **Save and connect**. The credentials are encrypted using the app's Android Keystore-protected key. You can remove them with **Forget saved login**.

Before a due scan, AYCF checks the saved session and follows Wizz's normal sign-in flow when renewal is needed. Updated session cookies are saved for subsequent scans. Connection failures retry with a delay. Rejected credentials or a verification challenge pause recovery until you update the login or complete Wizz sign-in. Notifications open your Wizz account screen. **Reconnect with saved login** retries without asking for the password again, while respecting Wizz cooldowns. **Change login** opens the credential fields only when needed. **Complete Wizz verification** opens Wizz and fills saved credentials into supported HTTPS POST forms on the known Wizz login origins. It never submits the browser form automatically, fills a different origin or iframe, or overwrites an edited field. Complete any verification and tap the Wizz sign-in button, then **Connect wallet**.

## Plan from Manchester

Open **Menu → Manchester connections**. Choose the four priority countries, a single country or **Anywhere in my scan**. The default UK airports are Manchester, Liverpool and Leeds Bradford. Pass flights, hub arrival windows and direct UK returns after a 24-hour stay use recent scan observations. Nearby airports include links for checking surface transport separately. Where a cash flight is needed, open its date-specific fare search and save the fare and local times to see the full connection. Automatic cash prices use the optional SerpApi key.

## Move from Termux

Download the Android branch's `transfer.py` to **Downloads**. In Termux, run the following command. If Termux has never accessed Downloads, run `termux-setup-storage` and allow access first.

```sh
python ~/storage/downloads/transfer.py
```

This produces **Downloads/AYCF-transfer.zip**. Select that ZIP on the APK's welcome screen. The exporter reads consistent SQLite snapshots, the route catalogue and planner preferences. Wizz login and optional fare API keys are set up separately inside the APK. The original Termux installation stays available. If you keep both installations, pause scheduled scans in one to avoid duplicating Wizz requests.

## Build

Use JDK 17, Python 3.12 and the Android SDK. From this directory, run `./gradlew :app:assembleDebug`. For an Intel emulator add `-Pabi=x86_64`. A release build uses `:app:assembleRelease`; sign the unsigned APK with the installation's release keystore to preserve update compatibility.

If the original signing key is unavailable, build with `-Palongside` and sign
with a separate retained release key. This installs **AYCF** (named **AYCF Refined** in 0.1.4) with package
`uk.aycf.mobile.refined` beside the original app. It has its own data and requires
fresh Wizz setup or an import during first launch. The 0.1.5 update keeps that package and signing key, preserving existing app data. Keep the signing backup
for subsequent updates. Pause scans in the older app when using the new
installation to avoid duplicate Wizz requests.

## 0.1.5 account setup

One Wizz account screen replaces the two separate sign-in paths. A saved account hides the password fields and offers reconnection and verification without re-entry. Login settings and encrypted credentials survive process restarts and compatible APK updates. Changing credentials invalidates the old scanner session before checking the new login. Network failures retain the credentials and retry later; verification pauses until the user completes it or explicitly retries. Provider login has fixture and browser tests; live Wizz account acceptance requires verification on the user's device.

## 0.1.4 planner refinement

The home screen gives short trips and Manchester connections their own entry
points. Bottom navigation stays available on Android tablets as well as phones.
Wizz connection status distinguishes a remembered login from a saved session;
returning from changed login settings opens scan status.

Short trips remembers valid airport and journey choices on this device. Dates
and destinations reset on a new visit. **This weekend** and **Next weekend** fill
Friday 16:00 to Monday 09:00 in UK time (or from now if the weekend has begun).
These shortcuts do not predict seats beyond the current scan's released dates.
Expand **Flight details** to inspect each leg, or use **Copy itinerary** to paste
it into notes or a message. Search coverage and past patterns remain available
in expandable sections.

The interface uses larger touch targets, keyboard-operable destination search
with airport-code matching, compact result cards and one scan-settings entry.
Manchester's transfer preferences and system diagnostics are expandable. The
web install control no longer floats over navigation or results. Android shows
page-loading progress and a recovery action if a local page cannot load.

All scanning, saved login encryption, exclusions, watches and keyless fare
workflows remain local. The release artifact must be signed with the original
installation's key before it can update an installed AYCF app.
