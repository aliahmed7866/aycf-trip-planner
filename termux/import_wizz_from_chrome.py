"""Import Wizz auth and discover the live AYCF API from Chrome on Android.

Chrome is exposed to Termux through an ADB-forwarded DevTools socket on
127.0.0.1:9222. Cookies are encrypted directly into SessionVault. The live
availability request metadata and non-secret station aliases are saved in
~/.config/aycf/wizz_runtime.json so Termux jobs do not need to scrape HTML.
"""

import json
import os
import socket
import sys
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit
from urllib.request import urlopen

import websocket

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

CONFIG_DIR = Path(os.environ.get("AYCF_CONFIG_DIR", str(Path.home() / ".config/aycf")))
ENV_FILE = CONFIG_DIR / "env"


def _load_termux_env():
    """Load simple export KEY='VALUE' lines from the Termux env file if needed."""
    if not ENV_FILE.exists():
        return
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line.startswith("export ") or "=" not in line:
            continue
        key, value = line[7:].split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


_load_termux_env()

from session_vault import SessionVault  # noqa: E402

DEVTOOLS = "http://127.0.0.1:9222"
RUNTIME_FILE = CONFIG_DIR / "wizz_runtime.json"


def _json_get(path: str):
    with urlopen(DEVTOOLS + path, timeout=8) as response:
        return json.load(response)


def _cdp_call(ws_url: str, method: str, params=None):
    ws = websocket.create_connection(ws_url, timeout=10, suppress_origin=True)
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


def _find_wizz_page():
    targets = _json_get("/json")
    pages = [t for t in targets if t.get("type") == "page" and t.get("webSocketDebuggerUrl")]
    wizz = [t for t in pages if "multipass.wizzair.com" in str(t.get("url") or "")]
    if not wizz:
        raise SystemExit("No open Multipass Wizz tab was found in Chrome. Open the logged-in Wizz wallet/search page first.")
    private = [t for t in wizz if "private-page" in str(t.get("url") or "")]
    return (private or wizz)[0]


def _station_aliases(page_ws: str):
    expression = "JSON.stringify((window.CVO && window.CVO.routes) || null)"
    try:
        result = _cdp_call(page_ws, "Runtime.evaluate", {"expression": expression, "returnByValue": True, "awaitPromise": True})
        raw = (((result or {}).get("result") or {}).get("value") or "")
        routes = json.loads(raw) if raw else None
    except Exception:
        routes = None
    aliases = {}
    if not isinstance(routes, list):
        return aliases
    for route in routes:
        if not isinstance(route, dict):
            continue
        stations = [route.get("departureStation")] + list(route.get("arrivalStations") or [])
        for station in stations:
            if not isinstance(station, dict) or not station.get("id"):
                continue
            sid = str(station["id"]).upper()
            aliases[sid.casefold()] = sid
            for key in ("name", "shortName", "city", "displayName", "nameWithCountry"):
                value = station.get(key)
                if value:
                    aliases[str(value).strip().casefold()] = sid
    return aliases


def _candidate_score(request, resource_type=""):
    url = str(request.get("url") or "")
    method = str(request.get("method") or "").upper()
    post_data = str(request.get("postData") or "")
    try:
        host = (urlsplit(url).hostname or "").lower()
    except Exception:
        return -1
    if host != "multipass.wizzair.com":
        return -1
    low_url = url.lower()
    low_body = post_data.lower()
    score = 0
    if str(resource_type).lower() in {"xhr", "fetch"}: score += 5
    if method == "POST": score += 4
    if "availability" in low_url: score += 8
    if "search" in low_url: score += 4
    if "flight" in low_url: score += 3
    if "subscription" in low_url: score += 1
    if "origin" in low_body and "destination" in low_body: score += 10
    if "departure" in low_body: score += 3
    return score


def _safe_template(request):
    post_data = request.get("postData")
    if not post_data:
        return None, None
    content_type = str((request.get("headers") or {}).get("Content-Type") or (request.get("headers") or {}).get("content-type") or "")
    try:
        parsed = json.loads(post_data)
        if isinstance(parsed, dict):
            return "json", parsed
    except Exception:
        pass
    if "application/x-www-form-urlencoded" in content_type.lower():
        try:
            return "form", dict(parse_qsl(post_data, keep_blank_values=True))
        except Exception:
            pass
    return None, None


def _capture_availability_request(page_ws: str, seconds: int = 60):
    """Capture a Wizz request only after Chrome confirms its response is JSON."""
    ws = websocket.create_connection(page_ws, timeout=2, suppress_origin=True)
    pending = {}
    verified = []
    try:
        ws.send(json.dumps({"id": 1, "method": "Network.enable", "params": {}}))
        while True:
            message = json.loads(ws.recv())
            if message.get("id") == 1:
                break
        print("\nNetwork capture is ready.")
        print("Switch to Chrome now and perform ONE normal AYCF flight search on Wizz.")
        print(f"Come back to Termux afterwards; capture stops automatically within {seconds} seconds.")
        deadline = time.time() + seconds
        ws.settimeout(1.0)
        while time.time() < deadline:
            try:
                message = json.loads(ws.recv())
            except (socket.timeout, websocket.WebSocketTimeoutException):
                continue
            except Exception:
                break
            method = message.get("method")
            params = message.get("params") or {}
            if method == "Network.requestWillBeSent":
                request = params.get("request") or {}
                score = _candidate_score(request, params.get("type") or "")
                if score >= 0:
                    pending[str(params.get("requestId") or "")] = (score, request, str(params.get("type") or ""))
                continue
            if method != "Network.responseReceived":
                continue
            request_id = str(params.get("requestId") or "")
            candidate = pending.get(request_id)
            if not candidate:
                continue
            score, request, resource_type = candidate
            response = params.get("response") or {}
            mime = str(response.get("mimeType") or "").lower()
            headers = response.get("headers") or {}
            content_type = str(headers.get("content-type") or headers.get("Content-Type") or "").lower()
            status = int(response.get("status") or 0)
            if 200 <= status < 300 and ("json" in mime or "json" in content_type):
                verified.append((score, request, resource_type, response))
                if score >= 15:
                    break
    finally:
        ws.close()
    if not verified:
        return None
    verified.sort(key=lambda item: item[0], reverse=True)
    return verified[0]


def main():
    try:
        version = _json_get("/json/version")
    except Exception as exc:
        raise SystemExit("Could not reach Android Chrome DevTools on 127.0.0.1:9222. Make sure Wireless Debugging is connected and adb forward succeeded.") from exc
    browser_ws = version.get("webSocketDebuggerUrl")
    if not browser_ws:
        raise SystemExit("Chrome DevTools did not expose a browser WebSocket endpoint.")
    target = _find_wizz_page()
    page_ws = target["webSocketDebuggerUrl"]
    print(f"Found authenticated Wizz tab: {target.get('url', '')}")
    result = _cdp_call(browser_ws, "Storage.getCookies")
    all_cookies = result.get("cookies") or []
    wizz = []
    for cookie in all_cookies:
        domain = str(cookie.get("domain") or "").lstrip(".").lower()
        if domain == "wizzair.com" or domain.endswith(".wizzair.com"):
            converted = _playwright_cookie(cookie)
            if converted["name"] and converted["value"]:
                wizz.append(converted)
    if not wizz:
        raise SystemExit("No Wizz cookies were found. Make sure the Multipass tab is logged in.")
    aliases = _station_aliases(page_ws)
    captured = _capture_availability_request(page_ws)
    if not captured:
        raise SystemExit("No JSON AYCF availability response was detected. Re-run the importer and perform one actual flight search in the Wizz tab during the capture window.")
    score, request, resource_type, response = captured
    endpoint = str(request.get("url") or "").strip()
    if not endpoint.startswith("https://multipass.wizzair.com/"):
        raise SystemExit("Captured request did not look like a Multipass availability endpoint.")
    template_type, template = _safe_template(request)
    method = str(request.get("method") or "GET").upper()
    state = {"cookies": wizz, "origins": []}
    SessionVault().save(state)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    runtime = {
        "availability_url": endpoint,
        "request_method": method,
        "request_template_type": template_type,
        "request_template": template,
        "station_ids": aliases,
        "captured_resource_type": resource_type,
        "captured_response_mime": str(response.get("mimeType") or ""),
        "captured_from": str(target.get("url") or ""),
        "captured_at": int(time.time()),
    }
    temp = RUNTIME_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps(runtime, indent=2), encoding="utf-8")
    os.chmod(temp, 0o600)
    temp.replace(RUNTIME_FILE)
    os.chmod(RUNTIME_FILE, 0o600)
    print(f"\nWizz connected. Encrypted {len(wizz)} Wizz-only cookies locally.")
    print("Verified JSON AYCF request captured from Chrome network traffic.")
    print(f"Request method/template: {method} / {template_type or 'none'}")
    print(f"Station aliases captured: {len(aliases)}")
    print(f"Runtime config saved to: {RUNTIME_FILE}")
    print("No plaintext browser-session file was written.")


if __name__ == "__main__":
    main()
