"""Repair or refresh the encrypted Wizz session used by Termux scans.

The normal path is intentionally browser-free: validate the existing encrypted
session and, when the captured availability UUID has gone stale, rediscover the
current endpoint from the authenticated Multipass wallet page using those saved
cookies. Android Chrome/CDP is only used when the saved session can no longer
self-heal (typically because authentication has expired).
"""

import json
import os
import re
import sys
import time
from pathlib import Path

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from morning_scan import (  # noqa: E402
    CapturedRequestWizzClient,
    WizzSessionExpired,
    _looks_like_login_html,
)
from session_vault import SessionVault  # noqa: E402
from termux.import_wizz_from_chrome import (  # noqa: E402
    CONFIG_DIR,
    RUNTIME_FILE,
    _cdp_call,
    _find_wizz_page,
    _json_get,
    _playwright_cookie,
)
from termux.wizz_runtime import apply_runtime, normalize_runtime, write_runtime  # noqa: E402

PRIVATE_PAGE = "https://multipass.wizzair.com/en/w6/subscriptions/spa/private-page/wallets"
STATUS_FILE = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf"))) / "wizz-session-status.json"

_ENDPOINT_PATTERNS = [
    re.compile(r'"searchFlight"\s*:\s*"(https:\\/\\/multipass\.wizzair\.com[^\"]+)"', re.I),
    re.compile(r'window\.CVO\.flightSearchUrlJson\s*=\s*["\']([^"\']+)["\']', re.I),
]
_PASS_ID_PATTERN = re.compile(r"\bpass_id\s*[:=]\s*['\"]?([a-f0-9-]{36})", re.I)


def _status(ok: bool, state: str, detail: str = "") -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"ok": bool(ok), "state": state, "detail": detail, "updated_at": int(time.time())}
    tmp = STATUS_FILE.with_name(f".{STATUS_FILE.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.chmod(tmp, 0o600)
    tmp.replace(STATUS_FILE)


def _write_runtime(runtime: dict) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    write_runtime(RUNTIME_FILE, runtime)


def _normalize_runtime_in_place(runtime: dict) -> bool:
    normalized, repaired = normalize_runtime(runtime)
    if normalized != runtime:
        runtime.clear()
        runtime.update(normalized)
    return repaired


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


def _extract_availability_url(page_text: str) -> str | None:
    text = str(page_text or "")
    for pattern in _ENDPOINT_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        endpoint = match.group(1).replace("\\/", "/").strip()
        if endpoint.startswith("https://multipass.wizzair.com/") and "/availability/" in endpoint:
            return endpoint
    match = _PASS_ID_PATTERN.search(text)
    if match:
        return f"https://multipass.wizzair.com/w6/subscriptions/json/availability/{match.group(1)}"
    return None


def _rediscover_endpoint(client: CapturedRequestWizzClient) -> str | None:
    try:
        response = client.http.get(PRIVATE_PAGE, timeout=25, allow_redirects=False)
    except requests.RequestException:
        return None
    if response.status_code in (401, 403):
        raise WizzSessionExpired("Wizz rejected the authenticated wallet page.")
    if 300 <= response.status_code < 400:
        location = str(response.headers.get("Location") or "")
        if "login" in location.casefold() or "keycloak" in location.casefold() or "openid-connect" in location.casefold():
            raise WizzSessionExpired("Wizz redirected the wallet page to authentication.")
        return None
    if _looks_like_login_html(response):
        raise WizzSessionExpired("Wizz returned its login page while refreshing the session.")
    if response.status_code != 200:
        return None
    return _extract_availability_url(response.text)


def _validate_candidate(candidate: dict, runtime: dict) -> tuple[CapturedRequestWizzClient, dict]:
    """Validate cookies using exactly the supplied runtime and self-heal it."""
    # Never re-read runtime metadata from a second path here. The caller has
    # already selected the active runtime file, and using a second disk lookup
    # was the source of repeated template/path disagreement during recovery.
    _normalize_runtime_in_place(runtime)
    client = CapturedRequestWizzClient(candidate, cache_ttl=30, min_delay=0.2)
    if not apply_runtime(client, runtime):
        raise RuntimeError("Saved AYCF request template could not be applied")

    try:
        preflight = client.preflight()
        if not preflight.get("ok"):
            raise RuntimeError(str(preflight.get("reason") or "AYCF preflight did not validate"))
        return client, preflight
    except WizzSessionExpired:
        raise
    except Exception as first_exc:
        # A missing template and a stale endpoint can happen together. The old
        # repair only worked when the already-saved URL was canonical; here we
        # first rediscover the live endpoint, then rebuild the POST/JSON request
        # shape and apply it to this same client before retrying.
        old_endpoint = str(client.dynamic_url or "")
        endpoint = _rediscover_endpoint(client)
        if not endpoint:
            raise first_exc

        runtime["availability_url"] = endpoint
        runtime["endpoint_rediscovered_at"] = int(time.time())
        repaired = _normalize_runtime_in_place(runtime)
        if not apply_runtime(client, runtime):
            raise first_exc

        if endpoint != old_endpoint:
            print(f"[AYCF] Saved AYCF endpoint appears stale; rediscovered {endpoint}")
        if repaired:
            print("[AYCF] Rebuilt the AYCF request template as canonical POST/JSON after endpoint rediscovery.")

        preflight = client.preflight()
        if not preflight.get("ok"):
            raise RuntimeError(str(preflight.get("reason") or "rediscovered AYCF endpoint did not validate"))
        return client, preflight


def _try_saved_session(runtime: dict) -> bool:
    """Repair/validate from the encrypted vault without touching Chrome."""
    try:
        saved = SessionVault().load()
    except Exception as exc:
        print(f"[AYCF] Saved encrypted Wizz session could not be loaded: {exc}")
        return False
    if not saved:
        return False
    try:
        client, preflight = _validate_candidate(saved, runtime)
    except WizzSessionExpired as exc:
        print(f"[AYCF] Saved encrypted Wizz session has expired: {exc}")
        return False
    except Exception as exc:
        print(f"[AYCF] Saved encrypted Wizz session could not self-repair: {exc}")
        return False

    runtime["availability_url"] = client.dynamic_url
    runtime["saved_session_validated_at"] = int(time.time())
    _write_runtime(runtime)
    _status(True, "saved_session_reused", f"Saved encrypted session validated ({preflight.get('response')}).")
    print(f"[AYCF] Saved encrypted Wizz session validated; Chrome/ADB not required ({preflight.get('response')}).")
    return True


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
    if not isinstance(runtime, dict):
        _status(False, "needs_initial_capture", "Saved Wizz runtime metadata has the wrong shape.")
        return 2
    if not str(runtime.get("availability_url") or "").startswith("https://multipass.wizzair.com/"):
        _status(False, "needs_initial_capture", "Saved Wizz availability endpoint is missing/invalid.")
        return 2

    # Repair canonical GET/no-template captures inside the same process that will
    # validate them. This makes the shell migration helper optional rather than
    # a correctness dependency.
    if _normalize_runtime_in_place(runtime):
        _write_runtime(runtime)
        print("[AYCF] Repaired the active Wizz runtime in-process to canonical POST/JSON.")

    # Fast path: existing encrypted cookies can usually validate the request or
    # rediscover a rotated availability UUID directly from the wallet page.
    if _try_saved_session(runtime):
        return 0

    # Only now involve Android Chrome/CDP. This is the exceptional path for
    # genuinely expired/invalid browser authentication.
    print("[AYCF] Saved session needs browser renewal; trying Android Chrome/ADB.")
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
        client, preflight = _validate_candidate(candidate, runtime)
    except WizzSessionExpired as exc:
        _status(False, "login_required", str(exc)[:240])
        print(f"[AYCF] Chrome Wizz session requires authentication: {exc}")
        return 4
    except Exception as exc:
        _status(False, "validation_failed", str(exc)[:240])
        print(f"[AYCF] Chrome cookies were captured, but AYCF validation failed without an auth redirect: {exc}")
        print("[AYCF] Existing encrypted session was left untouched; retry later or recapture the request template if this persists.")
        return 5

    # Only replace the vault after the fresh Chrome cookie set proves it can
    # replay the verified AYCF request template successfully.
    SessionVault().save(candidate)
    runtime["availability_url"] = client.dynamic_url
    runtime["session_refreshed_at"] = int(time.time())
    runtime["session_refreshed_from"] = str(target.get("url") or "")
    _write_runtime(runtime)
    _status(True, "refreshed", f"Validated and encrypted {len(cookies)} Wizz cookies from Chrome.")
    print(f"[AYCF] Wizz session refreshed and validated automatically ({len(cookies)} encrypted cookies; {preflight.get('response')}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
