# AYCF Live Trip Scanner

A Flask app for Wizz Air All You Can Fly (AYCF) subscribers. The app uses the newest parsed Wizz AYCF availability PDF as the route eligibility graph, then checks the authenticated Multipass availability search for actual flights over the next 1–4 days.

This branch replaces historical probability scoring with live AYCF checks.

## How it works

1. The app refreshes the public AYCF availability dataset generated from Wizz's current PDF.
2. Only routes advertised in the newest AYCF snapshot are considered.
3. Your authenticated Multipass browser session is imported securely.
4. The app checks direct and one-stop route legs against the live AYCF availability endpoint.
5. One-stop combinations are kept only when the connection meets your minimum self-transfer time.
6. Repeated route/date checks are cached briefly and requests are throttled to reduce Wizz rate limiting.

## Security model

The app does **not** store your Wizz username or password. Login happens in a local Playwright browser directly on Wizz's website. The resulting Playwright `storage_state` is uploaded over HTTPS to the app and encrypted with Fernet before it is written to disk.

Never commit the encryption key, admin token, or encrypted session file to Git.

Required server secrets:

```bash
FLASK_SECRET_KEY=<random-long-secret>
AYCF_ADMIN_TOKEN=<random-long-admin-token>
AYCF_SESSION_ENCRYPTION_KEY=<fernet-key>
```

Generate a Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

For Railway or another ephemeral container host, mount a persistent volume and point the encrypted session file at it:

```bash
WIZZ_SESSION_FILE=/data/wizz_session.enc
```

Optional tuning:

```bash
AYCF_CACHE_DIR=/data/aycf-cache
AYCF_REFRESH_SECONDS=21600
AYCF_LIVE_CACHE_SECONDS=300
AYCF_MIN_REQUEST_DELAY=1.0
AYCF_MAX_RESULTS=100
```

## Connect your Wizz account

Run the connector on your own computer. Your password and any MFA/CAPTCHA are entered only on Wizz's website.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium

export AYCF_APP_URL="https://your-deployed-app.example"
export AYCF_ADMIN_TOKEN="the-same-admin-token-configured-on-the-server"
python login_wizz.py
```

A Chromium window opens. Log into Wizz normally, wait until the private Multipass page is visible, return to the terminal and press Enter. The server validates the session before encrypting it.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium

export FLASK_SECRET_KEY="dev-secret"
export AYCF_ADMIN_TOKEN="dev-admin-token"
export AYCF_SESSION_ENCRYPTION_KEY="<generated-fernet-key>"
python app.py
```

Open `http://127.0.0.1:8080`.

## Deployment notes

The supplied Dockerfile installs Chromium because `login_wizz.py` can also be used in a desktop/container environment, although normal server-side scanning itself uses `requests` and does not launch Chromium.

The Wizz Multipass availability endpoint is session-bound and is discovered from the authenticated private page rather than hard-coded. If Wizz changes the private page structure, update the discovery logic in `scanner.py`.

## Important limitations

- AYCF inventory changes quickly and a result is not a booking guarantee.
- Wizz may expire sessions at any time; rerun `login_wizz.py` when that happens.
- Large "Anywhere" scans create many requests. The scanner intentionally runs sequentially and throttles requests rather than attempting high-concurrency scraping.
- One-stop itineraries are self-transfers. The app applies a minimum connection threshold, but baggage, immigration, airport changes, delays and missed-connection risk remain your responsibility.
- This project is not affiliated with Wizz Air.
