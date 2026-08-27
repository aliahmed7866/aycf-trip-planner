# Android / Termux deployment

This mode keeps the AYCF web app, encrypted Wizz session, morning PDF cache and SQLite database on one Android phone. The Flask server binds to `127.0.0.1` by default, so it is reachable only from that phone unless you explicitly change `AYCF_BIND_HOST`.

## 1. Install the Android apps

Install **Termux**, **Termux:API**, and **Termux:Boot** from the same signing source (recommended: F-Droid). Do not mix a Play Store/GitHub/F-Droid Termux build with add-ons signed from another source.

Open Termux:API and Termux:Boot once after installation. In Android battery settings, set Termux to unrestricted/background allowed where your device exposes that option. Android may otherwise defer scheduled work aggressively.

## 2. Clone and run the installer

In Termux:

```bash
pkg update -y
pkg install -y git
git clone --branch feature/live-aycf-scanner https://github.com/aliahmed7866/aycf-trip-planner.git ~/aycf-trip-planner
bash ~/aycf-trip-planner/termux/setup.sh
```

The setup script installs Python, `python-cryptography`, Poppler (`pdftotext`), Termux:API support and the Termux User Repository version of pandas. It creates:

```text
~/.config/aycf/env
~/.local/share/aycf/aycf.sqlite3
~/.local/share/aycf/wizz_session.enc
~/.local/share/aycf/cache/
~/.local/share/aycf/logs/
```

Secrets are generated locally and `~/.config/aycf/env` is chmod `600`.

Show your local scanner password with:

```bash
grep AYCF_APP_PASSWORD ~/.config/aycf/env
```

## 3. Schedule the morning scan

Run:

```bash
~/aycf-trip-planner/termux/schedule-morning.sh
```

This registers Android JobScheduler job `2608` approximately every 15 minutes. The job script exits immediately outside `06:00-08:59 UTC`, so network/PDF/Wizz work happens only around the expected AYCF publication window.

Inspect scheduled jobs:

```bash
termux-job-scheduler --pending
```

Cancel all Termux jobs if needed:

```bash
termux-job-scheduler --cancel-all
```

Morning logs are written to:

```text
~/.local/share/aycf/logs/morning.log
```

Watch them with:

```bash
tail -f ~/.local/share/aycf/logs/morning.log
```

The morning worker is idempotent and resumable: if today's PDF has already been scanned it exits; if a previous scan failed after some route/date checks, those completed checks are reused.

## 4. Import the Wizz login session

The scanner never stores your Wizz password. The current safest login bootstrap uses Playwright on a normal Windows/macOS/Linux computer because Playwright's bundled desktop Chromium is not a supported native Termux dependency.

On that computer, clone the same branch and install Playwright:

```bash
git clone --branch feature/live-aycf-scanner https://github.com/aliahmed7866/aycf-trip-planner.git
cd aycf-trip-planner
python -m venv .venv
# activate the venv
pip install -r requirements.txt
playwright install chromium
```

Export a temporary Wizz browser session:

macOS/Linux:

```bash
AYCF_EXPORT_STATE=./wizz-storage-state.json python login_wizz.py
```

PowerShell:

```powershell
$env:AYCF_EXPORT_STATE=".\wizz-storage-state.json"
python login_wizz.py
```

Log into Wizz in the opened Chromium window and complete MFA/CAPTCHA normally. The script writes `wizz-storage-state.json` locally.

Transfer that file privately to the Android phone (USB cable, Nearby Share/Quick Share, or another direct method). Do not email it or upload it to a public cloud folder.

If it lands in Downloads, first allow Termux storage access:

```bash
termux-setup-storage
```

Then import it:

```bash
source ~/.config/aycf/env
cd ~/aycf-trip-planner
python import_wizz_state.py ~/storage/downloads/wizz-storage-state.json
```

The importer validates the session against Wizz, encrypts it into `~/.local/share/aycf/wizz_session.enc`, and deletes the plaintext JSON after a successful import by default. Also delete the original plaintext copy from the computer.

If Wizz later expires the session, repeat only this section.

## 5. Start the local app

Run:

```bash
~/aycf-trip-planner/termux/run-web.sh
```

Then open Android Chrome and visit:

```text
http://127.0.0.1:8080
```

Use the `AYCF_APP_PASSWORD` shown from your env file.

The server binds only to localhost. Other phones/computers on your Wi-Fi cannot access it.

## 6. Start automatically after phone reboot

The setup script creates:

```text
~/.termux/boot/10-aycf-web
```

Termux:Boot runs scripts from `~/.termux/boot/` after Android boot. Open the Termux:Boot app once to grant its boot integration. The boot script acquires a Termux wake lock and starts the local Flask service.

After a reboot, test:

```text
http://127.0.0.1:8080
```

If your phone vendor has aggressive battery/process killing, you may still need to open Termux once after reboot or whitelist Termux in the vendor-specific battery manager.

## 7. Test the morning worker manually

Before relying on automation:

```bash
source ~/.config/aycf/env
cd ~/aycf-trip-planner
python morning_scan.py
```

A successful run prints counts for routes, route/date checks, live Wizz requests and flights found. Running it again against the same PDF should return `skipped: true`.

Check the database/cache status from the app home page or:

```bash
sqlite3 ~/.local/share/aycf/aycf.sqlite3 'select * from pdf_runs order by generated_at desc limit 3;'
```

(`pkg install sqlite` if you want the SQLite CLI; Python itself does not require it.)

## 8. Updating the app

In Termux:

```bash
cd ~/aycf-trip-planner
git pull --ff-only origin feature/live-aycf-scanner
chmod 700 termux/*.sh
```

Restart the local server by closing its Termux process/session and rerunning:

```bash
~/aycf-trip-planner/termux/run-web.sh
```

Your encrypted Wizz session, DB and cache live outside the Git checkout and are preserved across updates.

## Privacy / network behaviour

This is not an offline flight scanner: the phone still makes outbound HTTPS connections to Wizz to download the official AYCF PDF and query authenticated Multipass availability. However, the user interface itself does **not** need to be exposed to the internet or your LAN.

Local data stays under `~/.local/share/aycf`, including the encrypted Wizz session and SQLite cache. Back up `~/.config/aycf/env` securely if you want to preserve the Fernet key; without that key, an existing encrypted Wizz session cannot be decrypted.

## Android reliability notes

Android JobScheduler timings are approximate rather than exact. The Termux API exposes a minimum periodic interval of 15 minutes on Android N and newer, so this project schedules a persisted 15-minute gate and performs real work only during the morning window. Keeping Termux exempt from battery optimisation substantially improves reliability, but no Android phone provides the same unattended scheduling guarantees as an always-on Linux server.
