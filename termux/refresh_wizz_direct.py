"""Renew the Wizz Multipass session without Android Chrome/CDP.

This follows Wizz's real HTTP login redirects and HTML forms dynamically using
stored encrypted credentials. It deliberately stops for CAPTCHA/MFA/passkey or
other interactive security challenges rather than attempting to bypass them.
"""

from __future__ import annotations

import html
import os
import re
import sys
import time
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from credential_vault import CredentialVault  # noqa: E402
from session_vault import SessionVault  # noqa: E402
from termux.refresh_wizz_from_chrome import (  # noqa: E402
    CONFIG_DIR,
    PRIVATE_PAGE,
    RUNTIME_FILE,
    _status,
    _validate_candidate,
    _auth_request,
    _rate_limited,
)
from wizz_rate_limit import WizzRateLimited, check_cooldown
from scanner import WizzRequestRejected

from wizz_login_forms import (CHALLENGE_RE, AUTH_ERROR_RE, LoginFormParser, VisibleTextParser,
                              _visible_text, _login_form, _looks_authenticated, _field_name, _cookie_state)


def _save_runtime(runtime: dict) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    tmp = RUNTIME_FILE.with_suffix(".tmp")
    tmp.write_text(__import__("json").dumps(runtime, indent=2) + "\n", encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(RUNTIME_FILE)
    os.chmod(RUNTIME_FILE, 0o600)


def _main() -> int:
    creds = CredentialVault().load()
    if not creds:
        print("[AYCF] Direct Wizz renewal unavailable: no encrypted login credentials configured.")
        return 10
    if not RUNTIME_FILE.exists():
        print("[AYCF] Direct Wizz renewal unavailable: no verified runtime template exists.")
        return 2

    import json

    runtime = json.loads(RUNTIME_FILE.read_text(encoding="utf-8"))
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 16) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/140.0 Mobile Safari/537.36"
            ),
            "Accept-Language": "en-GB,en;q=0.9",
        }
    )

    try:
        response = _auth_request(session.get, PRIVATE_PAGE, timeout=30, allow_redirects=True)
    except requests.RequestException as exc:
        print(f"[AYCF] Direct Wizz renewal could not open login flow: {exc}")
        return 20

    if _looks_authenticated(response):
        pass
    else:
        text = _visible_text(response.text or "")
        if CHALLENGE_RE.search(text):
            print("[AYCF] Wizz requires an interactive security challenge; Chrome/manual attention is required.")
            return 12
        form = _login_form(response)
        if not form:
            print(f"[AYCF] Direct Wizz login form was not found ({response.url}).")
            return 13

        inputs = form.get("inputs") or []
        username_name = _field_name(inputs, "username")
        password_name = _field_name(inputs, "password")
        if not username_name or not password_name:
            print("[AYCF] Direct Wizz login form fields could not be identified safely.")
            return 13

        payload = {}
        for item in inputs:
            name = item.get("name")
            typ = (item.get("type") or "").lower()
            if name and typ in {"hidden", "submit"}:
                payload[name] = item.get("value", "")
        payload[username_name] = creds["username"]
        payload[password_name] = creds["password"]
        action = urljoin(response.url, form.get("action") or response.url)

        try:
            if (form.get("method") or "post").lower() == "get":
                response = _auth_request(session.get, action, params=payload, timeout=30, allow_redirects=True)
            else:
                response = _auth_request(session.post, action, data=payload, timeout=30, allow_redirects=True)
        except requests.RequestException as exc:
            print(f"[AYCF] Direct Wizz login submission failed: {exc}")
            return 20

        text = _visible_text(response.text or "")
        if CHALLENGE_RE.search(text):
            print("[AYCF] Wizz login reached an interactive security challenge; Chrome/manual attention is required.")
            return 12
        if AUTH_ERROR_RE.search(text):
            print("[AYCF] Wizz rejected the encrypted credentials. Update the credential vault.")
            return 14

        if not _looks_authenticated(response):
            try:
                response = _auth_request(session.get, PRIVATE_PAGE, timeout=30, allow_redirects=True)
            except requests.RequestException:
                pass
        if not _looks_authenticated(response):
            print(f"[AYCF] Direct Wizz login did not reach the authenticated wallet ({response.url}).")
            return 15

    candidate = _cookie_state(session)
    if not candidate["cookies"]:
        print("[AYCF] Direct Wizz login succeeded but no reusable Wizz cookies were returned.")
        return 16

    try:
        client, preflight = _validate_candidate(candidate, runtime)
    except (WizzRateLimited, WizzRequestRejected):
        raise
    except Exception as exc:
        print(f"[AYCF] Direct Wizz session validation failed: {exc}")
        return 17

    SessionVault().save(candidate)
    from route_directory import capture_from_html
    capture_from_html(response.text)
    runtime["availability_url"] = client.dynamic_url
    runtime["session_refreshed_at"] = int(time.time())
    runtime["session_refreshed_from"] = "direct-http-login"
    _save_runtime(runtime)
    _status(True, "refreshed_direct", f"Validated {len(candidate['cookies'])} Wizz cookies without Chrome/CDP.")
    print(
        f"[AYCF] Wizz session renewed directly without Chrome/CDP "
        f"({len(candidate['cookies'])} encrypted cookies; {preflight.get('response')})."
    )
    return 0


def main() -> int:
    try:
        check_cooldown()
        return _main()
    except WizzRateLimited as exc:
        return _rate_limited(exc)
    except WizzRequestRejected as exc:
        _status(False, 'request_rejected', str(exc))
        from termux.run_state import write_status
        write_status('request_rejected', str(exc), scan_performed=False, http_status=418)
        print(f'[AYCF] {exc}', flush=True)
        return 5


if __name__ == "__main__":
    raise SystemExit(main())
