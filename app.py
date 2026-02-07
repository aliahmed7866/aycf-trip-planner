import os
import secrets
from pathlib import Path
import json
from datetime import date, timedelta, datetime
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

import requests
import logging
from flask import Flask, render_template, request, flash, redirect, url_for

from data_updater import update_data_if_needed
from planner import AYCFPlanner

# --- Playwright debug helper (global) ---
def _dump_playwright_debug(page, tag: str) -> None:
    """Best-effort debug dump for Playwright flows (Railway-friendly)."""
    try:
        page.screenshot(path=f"/tmp/wizz_{tag}.png", full_page=True)
    except Exception:
        pass
    try:
        html = page.content()
        with open(f"/tmp/wizz_{tag}.html", "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass


# Basic stdout logging for Railway
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger("aycf")


UK_BASES = {"Liverpool", "London Luton"}

# Starter mapping for live checks (extend with WIZZ_CITY_TO_IATA_JSON in Railway Variables if needed)
DEFAULT_CITY_TO_IATA: Dict[str, str] = {
    "London Luton": "LTN",
    "London": "LTN",
    "Liverpool": "LPL",
    "Budapest": "BUD",
    "Bucharest": "OTP",
    "Warsaw": "WAW",
    "Kutaisi": "KUT",
    "Yerevan": "EVN",
    "Abu Dhabi": "AUH",
    "Dubai": "DWC",
    "Amman": "AMM",
    "Hurghada": "HRG",
    "Sharm el-Sheikh": "SSH",
}

@dataclass
class ResultRow:
    itinerary: str
    return_route: str
    score: float
    base_to_hub: float = 0.0
    hub_to_target: float = 0.0
    target_to_hub: float = 0.0
    hub_to_base: float = 0.0
    return_alternatives: list[str] = field(default_factory=list)
    return_is_predicted: bool = False


def _split_path(s: str) -> List[str]:
    return [p.strip() for p in s.split("→")]

def _has_fake_uk_domestic(path: List[str]) -> bool:
    for i in range(len(path) - 1):
        a, b = path[i], path[i + 1]
        if a in UK_BASES and b in UK_BASES and a != b:
            return True
    return False


def _split_route(route: str) -> list[str]:
    if not route:
        return []
    parts = [p.strip() for p in route.replace("->", "→").split("→")]
    return [p for p in parts if p]

def _build_return_alternatives(planner: AYCFPlanner, lookback_days: int, base: str, target: str, hub_candidates: list[str], limit: int = 5) -> list[str]:
    try:
        counts_df = planner.route_counts(lookback_days)
    except Exception:
        return []
    # dict of (from,to)->appearances
    counts = {(r["departure_from"], r["departure_to"]): int(r["appearances"]) for _, r in counts_df.iterrows()}
    alts = []
    seen = set()

    # Direct target -> base (rare but possible)
    direct = counts.get((target, base))
    if direct:
        s = f"{target} → {base}"
        alts.append((direct, s))
        seen.add(s)

    for hub in hub_candidates:
        if not hub or hub == target:
            continue
        a = counts.get((target, hub))
        b = counts.get((hub, base))
        if a and b:
            score = min(a, b)
            s = f"{target} → {hub} → {base}"
            if s not in seen:
                alts.append((score, s))
                seen.add(s)

    alts.sort(key=lambda t: t[0], reverse=True)
    return [s for _, s in alts[:limit]]


def _is_valid_single(itinerary: str, return_route: str) -> bool:
    out = _split_path(itinerary)
    ret = _split_path(return_route)
    if len(out) < 2 or len(ret) < 2:
        return False
    if out[-1] != ret[0]:
        return False
    if _has_fake_uk_domestic(out) or _has_fake_uk_domestic(ret):
        return False
    return True


def _cache_dir() -> str:
    return os.environ.get("AYCF_CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))

def _session_file() -> str:
    return os.path.join(_cache_dir(), "wizz_auto_session.json")

def _load_city_map() -> Dict[str, str]:
    raw = os.environ.get("WIZZ_CITY_TO_IATA_JSON", "").strip()
    if not raw:
        return DEFAULT_CITY_TO_IATA
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            merged = dict(DEFAULT_CITY_TO_IATA)
            for k, v in obj.items():
                if k and v:
                    merged[str(k)] = str(v).upper()
            return merged
    except Exception:
        pass
    return DEFAULT_CITY_TO_IATA

def load_auto_session() -> Optional[Dict[str, Any]]:
    p = _session_file()
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
        # Expiry check
        exp = obj.get("expires_at")
        if exp:
            try:
                if datetime.utcnow() >= datetime.fromisoformat(exp.replace("Z","")):
                    return None
            except Exception:
                pass
        return obj
    except Exception:
        return None

def save_auto_session(obj: Dict[str, Any]) -> None:
    os.makedirs(_cache_dir(), exist_ok=True)
    with open(_session_file(), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def clear_auto_session() -> None:
    p = _session_file()
    if os.path.exists(p):
        os.remove(p)


def _date_range(start: date, days: int) -> List[date]:
    return [start + timedelta(days=i) for i in range(days)]


def ensure_session() -> Dict[str, Any]:
    """Return a valid session object. If missing/expired and AYCF_AUTO_LOGIN=true, auto-login."""
    sess = load_auto_session()
    if sess:
        return sess

    if os.environ.get("AYCF_AUTO_LOGIN", "").lower() != "true":
        raise RuntimeError("Auto-login is disabled. Set AYCF_AUTO_LOGIN=true in Railway Variables.")

    email = os.environ.get("WIZZ_EMAIL", "").strip()
    password = os.environ.get("WIZZ_PASSWORD", "").strip()
    if not email or not password:
        raise RuntimeError("Missing WIZZ_EMAIL / WIZZ_PASSWORD in Railway Variables.")

    # Optional override if Wizz changes endpoints
    avail_url = os.environ.get("WIZZ_AVAILABILITY_URL", "https://www.wizzair.com/multipass/api/availability").strip()

    # Perform login via Playwright to obtain cookies (server-side).
    try:
            except Exception as e:
        raise RuntimeError("Playwright not installed. Ensure requirements include playwright and the Dockerfile installs browsers.") from e

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()
        def _find_and_fill(selectors, value):
            # Try on main page
            for sel in selectors:
                try:
                    loc = page.locator(sel).first
                    if loc.count() > 0:
                        loc.fill(value, timeout=8000)
                        return True
                except Exception:
                    pass
            # Try in frames (login sometimes renders inside an iframe)
            for fr in page.frames:
                if fr == page.main_frame:
                    continue
                for sel in selectors:
                    try:
                        loc = fr.locator(sel).first
                        if loc.count() > 0:
                            loc.fill(value, timeout=8000)
                            return True
                    except Exception:
                        pass
            return False

        def _find_and_click(selectors):
            for sel in selectors:
                try:
                    loc = page.locator(sel).first
                    if loc.count() > 0:
                        loc.click(timeout=8000)
                        return True
                except Exception:
                    pass
            for fr in page.frames:
                if fr == page.main_frame:
                    continue
                for sel in selectors:
                    try:
                        loc = fr.locator(sel).first
                        if loc.count() > 0:
                            loc.click(timeout=8000)
                            return True
                    except Exception:
                        pass
            return False

        def _debug_dump(tag):
            try:
                page.screenshot(path=f"/tmp/wizz_{tag}.png", full_page=True)
            except Exception:
                pass
            try:
                html = page.content()
                Path(f"/tmp/wizz_{tag}.html").write_text(html, encoding="utf-8", errors="ignore")
            except Exception:
                pass


        # Login flow (best-effort; Wizz UI changes can break this)
        state = secrets.token_hex(16)
        multipass_auth = (
            "https://multipass.wizzair.com/auth/realms/w6/protocol/openid-connect/auth"
            "?scope=openid%20roles%20tenant%20address%20phone%20subs%20email%20passenger"
            "&response_type=code"
            "&client_id=cvo-laravel"
            "&redirect_uri=https%3A%2F%2Fmultipass.wizzair.com%2Fen%2Fw6%2Fsubscriptions%2Fauth%2Fcallback"
            f"&state={state}"
            "&ui_locales=en&kc_locale=en"
        )

        login_urls = [
            multipass_auth,
            "https://multipass.wizzair.com/en/w6/subscriptions",
            "https://w6.wizzair.com/w6/user/login?language=en-gb",
            "https://w6.wizzair.com/w6/user/login",
            "https://wizzair.com/en-gb/#/account/login",
            "https://wizzair.com/en-gb/information-and-services/wizz-accounts/login",
        ]
        found_login_page = False
        for url in login_urls:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(2500)
                # Accept cookie banner if present
                _click_if_exists(page, ["button:has-text('Accept')", "button:has-text('I agree')", "button#onetrust-accept-btn-handler"], timeout_ms=1500)
                # give JS a moment
                page.wait_for_timeout(2500)
                if _find_any_locator(page, email_selectors, timeout_ms=1500):
                    found_login_page = True
                    break
                # also check frames
                for fr in page.frames:
                    if fr == page.main_frame:
                        continue
                    if _find_any_locator(fr, email_selectors, timeout_ms=1500):
                        found_login_page = True
                        break
                if found_login_page:
                    break
            except Exception:
                continue
        if not found_login_page:
            if "_dump_playwright_debug" in globals():
                _dump_playwright_debug(page, "no_login_form")
            raise RuntimeError("Could not find email field on Wizz login form (tried multiple URLs; page + iframes).")
        if "login" not in page.url.lower():
            page.goto("https://www.wizzair.com/en-gb", wait_until="domcontentloaded", timeout=60000)

        # Try to accept cookies if banner appears
        for sel in ["button#onetrust-accept-btn-handler", "button:has-text('Accept all')", "button:has-text('Accept')"]:
            try:
                page.locator(sel).first.click(timeout=2000)
                break
            except Exception:
                pass

        # Open login
        # Wizz changes often; try multiple selectors
        login_clicked = False
        for sel in [
            "text=Log in",
            "text=Sign in",
            "[data-test='login-button']",
            "a:has-text('Log in')",
            "button:has-text('Log in')"
        ]:
            try:
                page.locator(sel).first.click(timeout=4000)
                login_clicked = True
                break
            except Exception:
                continue

        if not login_clicked:
            # Sometimes login is already visible
            pass

        # Fill email/password
        page.wait_for_timeout(1200)
        filled = _find_and_fill([
            "input#username",
            "input[name='username']",
            "input[placeholder*='e-mail' i]",
            "input[placeholder*='email' i]",
            "input[autocomplete='username']",
            "input[type='email']",
            "input[name='email']",
            "input[id*='email' i]",
            "input[autocomplete='email']",
            "input[name='username']",
            "input[id*='username' i]",
            "input[autocomplete='username']",
            "input[placeholder*='mail' i]",
            "input[placeholder*='email' i]"
        ], email)

        if not filled:
            _debug_dump("no_email")
            raise RuntimeError("Could not find email field on Wizz login form (tried multiple URLs; page + iframes).")

        pw_filled = _find_and_fill([
            "input#password",
            "input[name='password']",
            "input[type='password']",
            "input[name='password']",
            "input[id*='password' i]",
            "input[autocomplete='current-password']",
            "input[placeholder*='password' i]"
        ], password)

        if not pw_filled:
            _debug_dump("no_password")
            raise RuntimeError("Could not find password field on Wizz login form (tried page + iframes).")
        if not pw_filled:
            raise RuntimeError("Could not find password field on Wizz login form.")

        # Submit
        submitted = _find_and_click([
            "button[type='submit']",
            "button:has-text('Log in')",
            "button:has-text('Sign in')",
            "button:has-text('Login')",
            "button:has-text('Continue')"
        ])
        if not submitted:
            _debug_dump("no_submit")
            raise RuntimeError("Could not submit Wizz login form (tried page + iframes).")

        # Wait for navigation / account state
        try:
            page.wait_for_timeout(6000)
        except Exception:
            pass

        cookies = context.cookies()
        browser.close()

    # Store cookies + availability url
    # TTL: 50 minutes (safe); if it expires earlier, we handle 401/403 by clearing and retrying once.
    expires_at = (datetime.utcnow() + timedelta(minutes=50)).isoformat() + "Z"
    sess = {
        "availability_url": avail_url,
        "cookies": cookies,
        "saved_at": datetime.utcnow().isoformat() + "Z",
        "expires_at": expires_at,
    }
    save_auto_session(sess)
    return sess


def _requests_session_from_cookies(cookies: List[Dict[str, Any]]) -> requests.Session:
    s = requests.Session()
    for c in cookies:
        try:
            s.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"), path=c.get("path"))
        except Exception:
            continue
    return s


def _live_fetch_with_cookies(sess_obj: Dict[str, Any], origin: str, dest: str, d: date) -> Dict[str, Any]:
    url = sess_obj.get("availability_url")
    cookies = sess_obj.get("cookies") or []
    if not url or not cookies:
        return {"ok": False, "error": "Missing availability_url/cookies."}

    params = {"origin": origin, "destination": dest, "departureDate": d.isoformat()}
    s = _requests_session_from_cookies(cookies)

    try:
        r = s.get(url, params=params, timeout=20, headers={"accept":"application/json"})
    except Exception as e:
        return {"ok": False, "error": f"Request failed: {e}"}

    if r.status_code in (401, 403):
        return {"ok": False, "error": f"Unauthorised ({r.status_code}). Session likely expired."}
    if r.status_code == 429:
        return {"ok": False, "error": "Rate limited (429). Try again in a minute."}
    if r.status_code >= 400:
        return {"ok": False, "error": f"HTTP error {r.status_code}"}

    try:
        data = r.json()
    except Exception:
        return {"ok": False, "error": "Could not parse JSON response."}

    flights = data.get("flightsOutbound") or data.get("flights") or []
    if not flights:
        return {"ok": True, "available": False, "date": d.isoformat(), "flights": []}

    extracted = []
    for f in flights[:6]:
        dep = f.get("departureTime") or f.get("departure") or f.get("departureDateTime")
        arr = f.get("arrivalTime") or f.get("arrival") or f.get("arrivalDateTime")
        fn = f.get("flightNumber") or f.get("flightNo") or ""
        extracted.append({"flightNumber": fn, "departure": dep, "arrival": arr})

    return {"ok": True, "available": True, "date": d.isoformat(), "flights": extracted}


def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key-change-me")

    @app.errorhandler(Exception)
    def handle_exception(e):
        # Log full traceback and show short message in UI (phone-friendly)
        logger.exception("Unhandled exception")
        msg = f"{type(e).__name__}: {e}"
        return render_template("error.html", message=msg), 500


    cache_root = _cache_dir()
    upstream_zip = os.environ.get("AYCF_UPSTREAM_ZIP", "https://github.com/markvincevarga/wizzair-aycf-availability/archive/refs/heads/main.zip")
    refresh_seconds = int(os.environ.get("AYCF_REFRESH_SECONDS", str(24*3600)))

    upd = update_data_if_needed(cache_root=cache_root, upstream_zip_url=upstream_zip, refresh_interval_seconds=refresh_seconds, force=False)
    data_dir = upd.data_dir
    planner = AYCFPlanner(data_dir=data_dir)

    @app.route("/", methods=["GET", "POST"])
    def index():
        defaults = planner.ui_defaults()
        defaults["default_bases"] = ["London Luton", "Liverpool"]
        defaults["default_hubs"] = []
        defaults["default_targets"] = []
        defaults["auto_login_enabled"] = (os.environ.get("AYCF_AUTO_LOGIN", "").lower() == "true")
        defaults["live_session_active"] = bool(load_auto_session())

        if request.method == "POST":
            form = request.form
            start_date = (form.get("start_date") or "").strip() or None
            end_date = (form.get("end_date") or "").strip() or None

            bases = request.form.getlist("bases")
            hubs = request.form.getlist("hubs")
            targets = request.form.getlist("targets")

            custom = (form.get("custom_targets") or "").strip()
            if custom:
                targets.extend([x.strip() for x in custom.split(",") if x.strip()])

            require_return_to_base = (form.get("require_return_to_base") == "on")

            try:
                top_n = max(1, min(200, int(form.get("top_n") or "25")))
            except Exception:
                top_n = 25

            try:
                lookback_days = max(7, min(730, int(form.get("lookback_days") or "180")))
            except Exception:
                lookback_days = 180

            try:
                min_transfer_minutes = max(60, min(600, int(form.get("min_transfer_minutes") or "150")))
            except Exception:
                min_transfer_minutes = 150

            if not bases or not hubs or not targets:
                flash("Please select at least one Base, one Hub, and one Target destination.", "warning")
                return render_template("index.html", **defaults, form=form)

            logger.info('Find routes: bases=%s hubs=%s targets=%s', bases, hubs, targets)
            try:
                raw = planner.suggest_itineraries(
                    lookback_days,
                    min_transfer_minutes,
                    start_date,
                    end_date,
                    bases,
                    hubs,
                    targets,
                    require_return_to_base,
                    top_n,
                )
            except Exception as e:
                logger.exception('Error while generating suggestions')
                flash('Route generation failed: ' + f"{type(e).__name__}: {e}", 'danger')
                flash('Tip: try Refresh Data; if it still fails, reduce hubs/targets to isolate.', 'warning')
                return render_template('index.html', **defaults, form=form)

            raw = [r for r in raw if _is_valid_single(r.get("itinerary",""), r.get("return",""))]
            # Weekend mode: enforce a non-empty return itinerary
            rows = []
            for r in raw:
                itinerary = r.get("itinerary","")
                return_route = (r.get("return","") or "").strip()
                parts = _split_route(itinerary)
                # expected: base → hub → target (or sometimes 2 legs)
                base_city = parts[0] if len(parts) >= 1 else (bases[0] if bases else "")
                target_city = parts[-1] if len(parts) >= 1 else ""
                # Try hubs selected as candidates for predicted returns
                hub_candidates = list(dict.fromkeys([c for c in hubs if c]))  # preserve order, unique
                alts = []
                predicted = False
                if not return_route and require_return_to_base and base_city and target_city:
                    alts = _build_return_alternatives(planner, lookback_days, base_city, target_city, hub_candidates, limit=5)
                    predicted = True if alts else False

                rows.append(ResultRow(
                    itinerary=itinerary,
                    return_route=return_route,
                    score=float(r.get("score", 0.0)),
                    base_to_hub=float(r.get("base_to_hub", 0.0)),
                    hub_to_target=float(r.get("hub_to_target", 0.0)),
                    target_to_hub=float(r.get("target_to_hub", 0.0)),
                    hub_to_base=float(r.get("hub_to_base", 0.0)),
                    return_alternatives=alts,
                    return_is_predicted=predicted,
                ))

            return render_template(
                "results.html",
                results=rows,
                start_date=start_date,
                end_date=end_date,
                lookback_days=lookback_days,
                min_transfer_minutes=min_transfer_minutes,
                require_return_to_base=require_return_to_base,
                data_dir=data_dir,
                total_runs=len(planner._load_runs()),
                live_session_active=bool(load_auto_session()),
                auto_login_enabled=(os.environ.get("AYCF_AUTO_LOGIN", "").lower() == "true"),
            )

        return render_template("index.html", **defaults, form=None)

    @app.route("/live/check", methods=["POST"])
    def live_check():
        itinerary = (request.form.get("itinerary") or "").strip()
        return_route = (request.form.get("return_route") or "").strip()
        start_date_str = (request.form.get("start_date") or "").strip()

        if not itinerary:
            flash("Missing itinerary.", "danger")
            return redirect(url_for("index"))

        try:
            start_d = date.fromisoformat(start_date_str) if start_date_str else date.today()
        except Exception:
            start_d = date.today()

        dates = _date_range(start_d, 3)
        city_map = _load_city_map()

        def city_to_iata(city: str) -> Optional[str]:
            return city_map.get(city)

        # Get or create session
        try:
            sess_obj = ensure_session()
        except Exception as e:
            flash(str(e), "danger")
            return redirect(url_for("index"))

        def check_path(path_str: str) -> List[Dict[str, Any]]:
            parts = _split_path(path_str)
            legs = [(parts[i], parts[i+1]) for i in range(len(parts)-1)]
            out = []
            for a, b in legs:
                a_iata = city_to_iata(a)
                b_iata = city_to_iata(b)
                if not a_iata or not b_iata:
                    out.append({"from": a, "to": b, "ok": False, "error": "Missing IATA mapping (set WIZZ_CITY_TO_IATA_JSON variable)."})
                    continue

                found = None
                last_err = None
                for d in dates:
                    res = _live_fetch_with_cookies(sess_obj, a_iata, b_iata, d)
                    if not res.get("ok") and "Unauthorised" in str(res.get("error","")):
                        # retry once with a fresh login
                        try:
                            clear_auto_session()
                            sess_obj2 = ensure_session()
                            res = _live_fetch_with_cookies(sess_obj2, a_iata, b_iata, d)
                            sess_obj = sess_obj2
                        except Exception:
                            pass

                    if res.get("ok") and res.get("available"):
                        found = res
                        break
                    last_err = res.get("error")

                if found is None and last_err:
                    out.append({"from": a, "to": b, "ok": False, "error": last_err})
                elif found is None:
                    out.append({"from": a, "to": b, "ok": True, "available": False, "checked_dates": [x.isoformat() for x in dates]})
                else:
                    out.append({"from": a, "to": b, "ok": True, "available": True, "match": found})
            return out

        checks = {
            "itinerary": itinerary,
            "return_route": return_route,
            "start_date": start_d.isoformat(),
            "legs_outbound": check_path(itinerary),
            "legs_return": check_path(return_route) if return_route else [],
        }

        return render_template("live_results.html", checks=checks, live_session_active=True)


    @app.route("/health", methods=["GET"])
    def health():
        try:
            runs = planner._load_runs()
            run_count = len(runs)
        except Exception:
            run_count = -1
        return {
            "ok": True,
            "data_dir": data_dir,
            "run_count": run_count,
            "auto_login_enabled": (os.environ.get("AYCF_AUTO_LOGIN","").lower()=="true"),
            "env_aycf_auto_login": os.environ.get("AYCF_AUTO_LOGIN"),
        }

    @app.route("/refresh", methods=["POST"])
    def refresh():
        update_data_if_needed(cache_root=cache_root, upstream_zip_url=upstream_zip, refresh_interval_seconds=refresh_seconds, force=True)
        flash("Data refreshed.", "success")
        return redirect(url_for("index"))

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))