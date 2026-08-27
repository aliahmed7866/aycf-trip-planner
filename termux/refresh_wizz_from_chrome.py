"""Refresh the encrypted Wizz session from an already-paired Android Chrome.

Unlike the first-time importer, this does not recapture the AYCF request
endpoint/template. It reuses the previously verified runtime metadata and only
updates Wizz cookies after a successful AYCF preflight. No password, MFA code,
or plaintext cookie file is stored.
"""

import json
import os
import sys
import time
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from morning_scan import CapturedRequestWizzClient, _apply_wizz_runtime  # noqa: E402
from scanner import WizzSessionExpired  # noqa: E402
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
        raise RuntimeError("Chrome is reachable, but Wizz requires attention/login before a session can be refreshed.")


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
        print("[AYCF] Wizz login required in Chrome; no Wizz cookies were found.")
        return 4

    candidate = {"cookies": cookies, "origins": []}
    try:
        client = CapturedRequestWizzClient(candidate, cache_ttl=30, min_delay=0.2)
        if not _apply_wizz_runtime(client):
            raise RuntimeError("Saved AYCF request template could not be applied")
        preflight = client.preflight()
        if not preflight.get("ok"):
            raise RuntimeError(str(preflight.get("reason") or "AYCF preflight did not validate"))
    except WizzSessionExpired as exc:
        _status(False, "login_required", str(exc)[:240])
        print(f"[AYCF] Chrome Wizz session requires login: {exc}")
        return 4
    except Exception as exc:
        # Do not misclassify Wizz 5xx/network/template failures as authentication
        # failures. In particular, a healthy Chrome login can coexist with a
        # transient availability endpoint error.
        _status(False, "refresh_validation_error", str(exc)[:240])
        print(f"[AYCF] Chrome cookies were captured, but AYCF validation failed without an auth redirect: {exc}")
        print("[AYCF] Existing encrypted session was left untouched; retry later or recapture the request template if this persists.")
        return 5

    SessionVault().save(candidate)
    runtime["session_refreshed_at"] = int(time.time())
    runtime["session_refreshed_from"] = str(target.get("url") or "")
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    temp = RUNTIME_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps(runtime, indent=2), encoding="utf-8")
    os.chmod(temp, 0o600)
    temp.replace(RUNTIME_FILE)
    os.chmod(RUNTIME_FILE, 0o600)
    _status(True, "refreshed", f"Validated and encrypted {len(cookies)} Wizz cookies from Chrome.")
    print(f"[AYCF] Wizz session refreshed and validated automatically ({len(cookies)} encrypted cookies).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
