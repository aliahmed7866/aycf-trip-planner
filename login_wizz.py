"""One-time secure Wizz session connector.

Run this on your own computer, not on the server:
  pip install playwright requests
  playwright install chromium
  AYCF_APP_URL=https://your-app.example \
  AYCF_ADMIN_TOKEN=... \
  python login_wizz.py

Your password is entered only into Wizz's own login page. The script sends the
resulting browser storage state to your app over HTTPS; the server validates and
encrypts it before writing it to persistent storage.
"""

import json
import os
from urllib.parse import urlsplit

import requests
from playwright.sync_api import sync_playwright

APP_URL = os.environ.get("AYCF_APP_URL", "").rstrip("/")
ADMIN_TOKEN = os.environ.get("AYCF_ADMIN_TOKEN", "")
PRIVATE_PAGE = "https://multipass.wizzair.com/w6/subscriptions/spa/private-page/wallets"


def _validate_config() -> None:
    if not APP_URL or not ADMIN_TOKEN:
        raise SystemExit("Set AYCF_APP_URL and AYCF_ADMIN_TOKEN first.")
    parsed = urlsplit(APP_URL)
    local_http = parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost"}
    if parsed.scheme != "https" and not local_http:
        raise SystemExit("AYCF_APP_URL must use HTTPS (HTTP is allowed only for localhost development).")


def main():
    _validate_config()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        try:
            context = browser.new_context()
            page = context.new_page()
            page.goto(PRIVATE_PAGE, wait_until="domcontentloaded", timeout=60000)
            print("Log into Wizz in the browser window. Complete MFA/CAPTCHA if requested.")
            print("When the private Multipass wallet/search page has loaded, return here and press Enter.")
            input()
            page.goto(PRIVATE_PAGE, wait_until="domcontentloaded", timeout=60000)
            if "openid-connect/auth" in page.url or "/login" in page.url.lower():
                raise SystemExit("Wizz still shows the login page; login was not completed.")
            state = context.storage_state()
        finally:
            browser.close()

    response = requests.post(
        APP_URL + "/admin/wizz/session",
        headers={"X-AYCF-Admin-Token": ADMIN_TOKEN, "Content-Type": "application/json"},
        data=json.dumps(state),
        timeout=60,
    )
    try:
        payload = response.json()
    except Exception:
        payload = {"body": response.text[:1000]}
    if not response.ok:
        print(json.dumps(payload, indent=2))
        raise SystemExit(f"Server rejected session: HTTP {response.status_code}")
    print("Wizz account connected securely.")
    probe = payload.get("probe") if isinstance(payload, dict) else None
    if probe:
        print(f"Availability endpoint discovered; station mappings detected: {probe.get('stations', 0)}")


if __name__ == "__main__":
    main()
