# AYCF for Android

The APK runs the AYCF planner, scanner and SQLite databases on the phone. Minimum Android version is 8.0. The default build targets ARM64 phones.

## First launch

Choose **Import Termux backup** on the welcome screen to bring your saved data over, or choose **Start fresh**. Open **Menu → Sign in to Wizz**, sign in on the Wizz Multipass page and tap **Connect after signing in to Wizz**. AYCF validates availability and queues a scan. If Wizz requests verification, complete it on the Wizz page. **Menu → Flight alerts** opens Android's permission or notification settings. You can check delivery from **Watches → Test notification**.

The planner opens offline. A new installation starts with an empty route list. The first scan downloads the official route PDF. Cached availability keeps its original observation dates. Android schedules network-connected checks approximately every 15 minutes. The scanner uses the existing morning window and saves completed checks to SQLite. Android can defer or interrupt work. Open **Menu → Scan status** to review progress. An expired Wizz session requires sign-in again.

## Move from Termux

Download the Android branch's `transfer.py` to **Downloads**. In Termux, run the following command. If Termux has never accessed Downloads, run `termux-setup-storage` and allow access first.

```sh
python ~/storage/downloads/transfer.py
```

This produces **Downloads/AYCF-transfer.zip**. Select that ZIP on the APK's welcome screen. The exporter reads consistent SQLite snapshots, the route catalogue and planner preferences. Wizz login and optional fare API keys are set up separately inside the APK. The original Termux installation stays available. If you keep both installations, pause scheduled scans in one to avoid duplicating Wizz requests.

## Build

Use JDK 17, Python 3.12 and the Android SDK. From this directory, run `./gradlew :app:assembleDebug`. For an Intel emulator add `-Pabi=x86_64`. A release build uses `:app:assembleRelease`; sign the unsigned APK with the installation's release keystore to preserve update compatibility.
