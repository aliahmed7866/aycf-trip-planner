# AYCF Live Trip Scanner

A Flask app for Wizz Air All You Can Fly (AYCF) subscribers. It uses the newest parsed Wizz AYCF availability PDF as the eligibility graph, then checks the authenticated Multipass availability search for actual flights over a 1–4 day window.

This branch replaces historical probability scoring with live AYCF checks.

## How it works

1. The app refreshes the public AYCF availability dataset generated from Wizz's current PDF.
2. Only routes advertised in the newest AYCF snapshot are considered.
3. Your authenticated Multipass browser session is imported securely.
4. The app checks direct and one-stop route legs against the live AYCF availability endpoint.
5. One-stop combinations are kept only when the connection meets your minimum self-transfer time.
6. Live route/date results are cached briefly across scans and requests are sent sequentially with throttling.
7. Return scans can use a separate return start date.

## Security model

The app does **not** store your Wizz username or password. Login happens in a local Playwright browser directly on Wizz's website. The resulting Playwright `storage_state` is uploaded over HTTPS to the app and encrypted with Fernet before it is written to disk.

The web UI itself can be password-protected, so somebody who discovers the public Railway URL cannot use your saved Wizz session. The login and scan forms also use per-session CSRF tokens, and login redirects are restricted to local app paths.

Never commit the encryption key, app password, admin token, or encrypted session file to Git.

Required server secrets:

```bash
FLASK_SECRET_KEY=<random-long-secret>
AYCF_APP_PASSWORD=<password-for-the-personal-web-ui>
AYCF_ADMIN_TOKEN=<random-long-admin-token-used-only-by-login_wizz.py>
AYCF_SESSION_ENCRYPTION_KEY=<fernet-key>
SESSION_COOKIE_SECURE=true
```

`FLASK_SECRET_KEY` is required on Railway so browser sessions remain valid across restarts.

Generate a Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

For Railway or another ephemeral container host, mount a persistent volume and point the encrypted session file and AYCF cache at it:

```bash
WIZZ_SESSION_FILE=/data/wizz_session.enc
AYCF_CACHE_DIR=/data/aycf-cache
```

Optional tuning:

```bash
AYCF_REFRESH_SECONDS=21600
AYCF_LIVE_CACHE_SECONDS=300
AYCF_MIN_REQUEST_DELAY=1.0
AYCF_MAX_RESULTS=100
AYCF_MAX_PATHS_PER_DAY=200
```

Large `Anywhere` scans can fan out into many candidate routes. `AYCF_MAX_PATHS_PER_DAY` limits route expansion before live requests are made. Direct routes are generated before one-stop candidates.

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

`login_wizz.py` requires HTTPS for remote deployments; plain HTTP is accepted only for `localhost`/`127.0.0.1` development.

A Chromium window opens. Log into Wizz normally, wait until the private Multipass page is visible, return to the terminal and press Enter. The server validates the session by discovering the live AYCF endpoint before encrypting it.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium

export FLASK_SECRET_KEY="dev-secret"
export AYCF_APP_PASSWORD="dev-web-password"
export AYCF_ADMIN_TOKEN="dev-admin-token"
export AYCF_SESSION_ENCRYPTION_KEY="<generated-fernet-key>"
python app.py
```

Open `http://127.0.0.1:8080`. Leave `SESSION_COOKIE_SECURE` unset for local HTTP development; set it to `true` on an HTTPS deployment.

## Tests

Run locally:

```bash
python -m unittest discover -s tests -v
```

The repository also includes a GitHub Actions workflow that runs the regression suite on the feature branch and pull requests.

## Deployment notes

The supplied Dockerfile installs Chromium because `login_wizz.py` can also be used in a desktop/container environment, although normal server-side scanning itself uses `requests` and does not launch Chromium.

The Wizz Multipass availability endpoint is session-bound and is discovered from the authenticated private page rather than hard-coded. The client accepts several plausible response wrappers/flight-list field names and retries one transient 5xx or 429 response conservatively. If Wizz materially changes the private page or API schema, update `scanner.py` and its tests.

## Important limitations

- AYCF inventory changes quickly and a result is not a booking guarantee.
- Wizz may expire sessions at any time; rerun `login_wizz.py` when that happens.
- Large `Anywhere` scans create many requests. Keep the route window narrow when possible.
- One-stop itineraries are self-transfers. The app applies a minimum connection threshold, but baggage, immigration, airport changes, delays and missed-connection risk remain your responsibility.
- The current scanner supports direct and one-stop itineraries, not two-stop routing.
- This project is not affiliated with Wizz Air.
