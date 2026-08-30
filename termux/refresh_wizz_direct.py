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
)

CHALLENGE_RE = re.compile(
    r"captcha|verify you are human|security check|verification code|one[- ]time|"
    r"passkey|two[- ]factor|multi[- ]factor|authenticator|enter.*code",
    re.I,
)
AUTH_ERROR_RE = re.compile(
    r"incorrect|invalid password|wrong password|login failed|invalid username|invalid email",
    re.I,
)


class LoginFormParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.forms: list[dict] = []
        self.current: dict | None = None

    def handle_starttag(self, tag, attrs):
        data = {str(k): str(v or "") for k, v in attrs}
        if tag.lower() == "form":
            self.current = {
                "action": html.unescape(data.get("action", "")),
                "method": data.get("method", "post").lower(),
                "inputs": [],
            }
            self.forms.append(self.current)
        elif tag.lower() == "input" and self.current is not None:
            self.current["inputs"].append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "form":
            self.current = None


class VisibleTextParser(HTMLParser):
    """Extract user-visible page text, excluding scripts/styles/templates."""

    HIDDEN = {"script", "style", "template", "noscript", "svg"}

    def __init__(self):
        super().__init__()
        self.hidden_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.HIDDEN:
            self.hidden_depth += 1

    def handle_endtag(self, tag):
        if tag.lower() in self.HIDDEN and self.hidden_depth:
            self.hidden_depth -= 1

    def handle_data(self, data):
        if self.hidden_depth == 0 and str(data or "").strip():
            self.parts.append(str(data))


def _visible_text(value: str) -> str:
    parser = VisibleTextParser()
    try:
        parser.feed(str(value or ""))
        return " ".join(parser.parts)
    except Exception:
        return ""


def _login_form(response: requests.Response) -> dict | None:
    parser = LoginFormParser()
    try:
        parser.feed(response.text or "")
    except Exception:
        return None
    for form in parser.forms:
        inputs = form.get("inputs") or []
        has_password = any((x.get("type") or "").lower() == "password" for x in inputs)
        if has_password:
            return form
    return None


def _looks_authenticated(response: requests.Response) -> bool:
    url = str(response.url or "").lower()
    text = _visible_text(response.text or "")
    if CHALLENGE_RE.search(text):
        return False
    if "private-page" in url and not _login_form(response):
        return response.status_code == 200
    return False


def _field_name(inputs: list[dict], kind: str) -> str | None:
    if kind == "password":
        for item in inputs:
            if (item.get("type") or "").lower() == "password" and item.get("name"):
                return item["name"]
        return None
    candidates = []
    for item in inputs:
        typ = (item.get("type") or "text").lower()
        if typ not in {"email", "text", "username"} or not item.get("name"):
            continue
        haystack = " ".join(
            str(item.get(k) or "") for k in ("name", "id", "placeholder", "autocomplete")
        ).lower()
        score = sum(token in haystack for token in ("email", "user", "login", "identifier"))
        candidates.append((score, item["name"]))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _cookie_state(session: requests.Session) -> dict:
    cookies = []
    for cookie in session.cookies:
        domain = str(cookie.domain or "").lstrip(".").lower()
        if domain != "wizzair.com" and not domain.endswith(".wizzair.com"):
            continue
        item = {
            "name": str(cookie.name),
            "value": str(cookie.value),
            "domain": str(cookie.domain or ".wizzair.com"),
            "path": str(cookie.path or "/"),
            "expires": int(cookie.expires) if cookie.expires else -1,
            "httpOnly": bool(cookie.has_nonstandard_attr("HttpOnly")),
            "secure": bool(cookie.secure),
            "sameSite": "Lax",
        }
        cookies.append(item)
    return {"cookies": cookies, "origins": []}


def _save_runtime(runtime: dict) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    tmp = RUNTIME_FILE.with_suffix(".tmp")
    tmp.write_text(__import__("json").dumps(runtime, indent=2) + "\n", encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(RUNTIME_FILE)
    os.chmod(RUNTIME_FILE, 0o600)


def main() -> int:
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
        response = session.get(PRIVATE_PAGE, timeout=30, allow_redirects=True)
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
                response = session.get(action, params=payload, timeout=30, allow_redirects=True)
            else:
                response = session.post(action, data=payload, timeout=30, allow_redirects=True)
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
                response = session.get(PRIVATE_PAGE, timeout=30, allow_redirects=True)
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
    except Exception as exc:
        print(f"[AYCF] Direct Wizz session validation failed: {exc}")
        return 17

    SessionVault().save(candidate)
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


if __name__ == "__main__":
    raise SystemExit(main())
