"""Refresh the encrypted Wizz session from an already-paired Android Chrome.

Unlike the first-time importer, this does not recapture the AYCF request
endpoint/template. It reuses the previously verified runtime metadata and only
updates Wizz cookies in SessionVault. No password, MFA code, or plaintext cookie
file is stored.
"""

import json
import os
import sys
import time
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from session_vault import SessionVault  # noqa: E402
from termux.import_wizz_from_chrome import (  # noqa: E402
    CONFIG_DIR,
    RUNTIME_FILE,
    _cdp_call,
    _find_wizz_page,
    _json_get,
    _playwright_cookie,
)

PRIVATE_PAGE = "https://multipass.wizzair.com/en/w6/subscriptions/spa/private-page/wallets"
STATUS_FILE = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf"))) / "wizz-session-status.json"


def _status(ok: bool, state: str, detail: str = "") -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"ok": bool(ok), "state": state, "detail": detail, "updated_at": int(time.time())}
    tmp = STATUS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(STATUS_FILE)


def _find_or_open_wizz(browser_ws: str):
    try:
        return _find_wizz_page()
    except SystemExit:
        # The browser is reachable but no Multipass target is open. Reopen the
        # known private page without requiring touch input, then give Chrome a
        # few seconds to restore/redirect the authenticated session.
        try:
            _cdp_call(browser_ws, "Target.createTarget", {"url": PRIVATE_PAGE})
        except Exception:
            pass
        for _ in range(8):
            time.sleep(1)
            try:
                return _find_wizz_page()
            except SystemExit:
                continue
        raise SystemExit("Chrome is reachable, but Wizz requires attention/login before a session can be refreshed.")


def main() -> int:
    if not RUNTIME_FILE.exists():
        _status(False, "needs_initial_capture", "No verified Wizz runtime template exists.")
        print("[AYCF] Automatic Wizz refresh unavailable: initial capture is required.")
        return 2
    try:
        runtime = json.loads(RUNTIME_FILE.read_text(encoding="utf-8"))
    except Exception:
        _status(False, "needs_initial_capture", "Saved Wizz runtime metadata is unreadable.")
        return 2
    if not str(runtime.get("availability_url") or "").startswith("https://multipass.wizzair.com/"):
        _status(False, "needs_initial_capture", "Saved Wizz availability endpoint is missing/invalid.")
        return 2

    try:
        version = _json_get("/json/version")
        browser_ws = version.get("webSocketDebuggerUrl")
        if not browser_ws:
            raise RuntimeError("Chrome did not expose a browser DevTools WebSocket")
        target = _find_or_open_wizz(browser_ws)
        result = _cdp_call(browser_ws, "Storage.getCookies")
    except Exception as exc:
        _status(False, "chrome_unavailable", str(exc)[:240])
        print(f"[AYCF] Automatic Wizz refresh could not access Chrome: {exc}")
        return 3

    cookies = []
    for cookie in result.get("cookies") or []:
        domain = str(cookie.get("domain") or "").lstrip(".").lower()
        if domain == "wizzair.com" or domain.endswith(".wizzair.com"):
            converted = _playwright_cookie(cookie)
            if converted["name"] and converted["value"]:
                cookies.append(converted)
    if not cookies:
        _status(False, "login_required", "No Wizz cookies were exposed by Chrome.")
        print("[AYCF] Wizz login required in Chrome; no authenticated Wizz cookies were found.")
        return 4

    SessionVault().save({"cookies": cookies, "origins": []})
    # Preserve the captured request template exactly; only record refresh time.
    runtime["session_refreshed_at"] = int(time.time())
    runtime["session_refreshed_from"] = str(target.get("url") or "")
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    temp = RUNTIME_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps(runtime, indent=2), encoding="utf-8")
    os.chmod(temp, 0o600)
    temp.replace(RUNTIME_FILE)
    os.chmod(RUNTIME_FILE, 0o600)
    _status(True, "refreshed", f"Encrypted {len(cookies)} Wizz cookies from Chrome.")
    print(f"[AYCF] Wizz session refreshed automatically from Chrome ({len(cookies)} encrypted cookies).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
