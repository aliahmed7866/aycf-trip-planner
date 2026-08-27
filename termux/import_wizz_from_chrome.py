"""Import an authenticated Wizz session from Chrome on the same Android phone.

Chrome is exposed to Termux through an ADB-forwarded DevTools socket on
127.0.0.1:9222. We use the Chrome DevTools Protocol to read browser cookies,
keep only Wizz domains, validate them against Multipass, then write them
straight into SessionVault. No plaintext session file is created.
"""

import json
import os
import sys
from urllib.request import urlopen

import websocket

# Allow imports from repository root when launched as termux/import_wizz_from_chrome.py.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from scanner import WizzAYCFClient  # noqa: E402
from session_vault import SessionVault  # noqa: E402

DEVTOOLS = "http://127.0.0.1:9222"


def _json_get(path: str):
    with urlopen(DEVTOOLS + path, timeout=8) as response:
        return json.load(response)


def _cdp_call(ws_url: str, method: str, params=None):
    ws = websocket.create_connection(ws_url, timeout=10, origin="http://127.0.0.1:9222")
    try:
        request_id = 1
        ws.send(json.dumps({"id": request_id, "method": method, "params": params or {}}))
        while True:
            message = json.loads(ws.recv())
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RuntimeError(f"Chrome DevTools error: {message['error']}")
            return message.get("result", {})
    finally:
        ws.close()


def _playwright_cookie(cookie):
    same_site = str(cookie.get("sameSite") or "Lax").title()
    if same_site not in {"Strict", "Lax", "None"}:
        same_site = "Lax"
    expires = cookie.get("expires")
    if not isinstance(expires, (int, float)) or expires <= 0:
        expires = -1
    return {
        "name": str(cookie.get("name") or ""),
        "value": str(cookie.get("value") or ""),
        "domain": str(cookie.get("domain") or ""),
        "path": str(cookie.get("path") or "/"),
        "expires": expires,
        "httpOnly": bool(cookie.get("httpOnly")),
        "secure": bool(cookie.get("secure")),
        "sameSite": same_site,
    }


def main():
    try:
        version = _json_get("/json/version")
    except Exception as exc:
        raise SystemExit(
            "Could not reach Android Chrome DevTools on 127.0.0.1:9222. "
            "Make sure Wireless Debugging is connected and adb forward succeeded."
        ) from exc

    ws_url = version.get("webSocketDebuggerUrl")
    if not ws_url:
        raise SystemExit("Chrome DevTools did not expose a browser WebSocket endpoint.")

    result = _cdp_call(ws_url, "Storage.getCookies")
    all_cookies = result.get("cookies") or []
    wizz = []
    for cookie in all_cookies:
        domain = str(cookie.get("domain") or "").lstrip(".").lower()
        if domain == "wizzair.com" or domain.endswith(".wizzair.com"):
            converted = _playwright_cookie(cookie)
            if converted["name"] and converted["value"]:
                wizz.append(converted)

    if not wizz:
        raise SystemExit(
            "No Wizz cookies were found in Chrome. Open Multipass in Chrome, complete login, "
            "and leave the authenticated Wizz page open before importing."
        )

    state = {"cookies": wizz, "origins": []}
    try:
        probe = WizzAYCFClient(state).bootstrap()
    except Exception as exc:
        raise SystemExit(
            "Wizz cookies were captured, but Multipass rejected them or the AYCF endpoint could "
            f"not be discovered: {exc}"
        ) from exc

    SessionVault().save(state)
    print(f"Wizz connected. Encrypted {len(wizz)} Wizz-only cookies locally.")
    print(f"AYCF endpoint validated; station mappings detected: {probe.get('stations', 0)}")
    print("No plaintext browser-session file was written.")


if __name__ == "__main__":
    main()
