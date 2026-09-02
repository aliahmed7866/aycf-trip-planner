import hashlib
import hmac
import ipaddress
import os
import secrets
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for

from cache_db import ScanCacheDB
from data_updater import update_data_if_needed
from itinerary_search import cached_scan_itineraries
from scan_scope import AIRPORT_GROUPS, load_scope, normalize_name, origin_options, save_scope, scan_plan, scope_fingerprint, scope_summary
from scanner import CurrentRouteGraph, WizzAYCFClient, _STATION_ALIASES
from session_vault import SessionVault

ROOT = Path(__file__).resolve().parent


def _cache_dir() -> str:
    return os.environ.get("AYCF_CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))


def _vault_or_none():
    try:
        return SessionVault()
    except Exception:
        return None



def _is_loopback_host(value: str) -> bool:
    host = str(value or "").strip().strip("[]")
    try:
        return host.casefold() == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _trusted_local_request() -> bool:
    """Trust only a direct loopback request to a loopback-bound AYCF service."""
    if os.environ.get("AYCF_REQUIRE_LOCAL_PASSWORD", "false").lower() == "true":
        return False
    return _is_loopback_host(os.environ.get("AYCF_BIND_HOST", "127.0.0.1")) and _is_loopback_host(request.remote_addr or "")


def _admin_ok(req) -> bool:
    expected = os.environ.get("AYCF_ADMIN_TOKEN", "")
    supplied = req.headers.get("X-AYCF-Admin-Token", "")
    return bool(expected and supplied and hmac.compare_digest(expected, supplied))


def _safe_next(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc or not parsed.path.startswith("/") or parsed.path.startswith("//"):
        return None
    return value


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _form_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(request.form.get(name) or default)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def create_app():
    app = Flask(__name__)
    bind_host = os.environ.get("AYCF_BIND_HOST", "127.0.0.1")
    if not _is_loopback_host(bind_host) and not os.environ.get("AYCF_APP_PASSWORD", ""):
        raise RuntimeError("AYCF_APP_PASSWORD is required when AYCF_BIND_HOST is not loopback.")
    configured_secret = os.environ.get("FLASK_SECRET_KEY", "")
    if not configured_secret and os.environ.get("RAILWAY_ENVIRONMENT"):
        raise RuntimeError("FLASK_SECRET_KEY must be configured in Railway.")
    app.secret_key = configured_secret or "local-dev-only-change-me"
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.environ.get("SESSION_COOKIE_SECURE", "false").lower() == "true"
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=12)

    def csrf_token() -> str:
        token = session.get("csrf_token")
        if not token:
            token = secrets.token_urlsafe(32)
            session["csrf_token"] = token
        return token

    app.jinja_env.globals["csrf_token"] = csrf_token

    def csrf_ok() -> bool:
        expected = session.get("csrf_token", "")
        supplied = request.form.get("csrf_token", "") or request.headers.get("X-CSRF-Token", "")
        return bool(expected and supplied and hmac.compare_digest(expected, supplied))

    @app.before_request
    def require_app_login():
        if request.endpoint in {"login", "health", "static", "import_wizz_session"}:
            return None
        password = os.environ.get("AYCF_APP_PASSWORD", "")
        if password and not session.get("aycf_authenticated") and not _trusted_local_request():
            if request.accept_mimetypes.accept_html:
                return redirect(url_for("login", next=request.path))
            return jsonify({"ok": False, "error": "login required"}), 401
        return None

    @app.route("/login", methods=["GET", "POST"])
    def login():
        password = os.environ.get("AYCF_APP_PASSWORD", "")
        if not password:
            return redirect(url_for("index"))
        if request.method == "POST":
            if not csrf_ok():
                flash("Your login form expired. Please try again.", "warning")
                return redirect(url_for("login"))
            supplied = request.form.get("password", "")
            if hmac.compare_digest(password, supplied):
                next_url = _safe_next(request.args.get("next")) or url_for("index")
                old_csrf = session.get("csrf_token")
                session.clear()
                session["aycf_authenticated"] = True
                session["csrf_token"] = old_csrf or secrets.token_urlsafe(32)
                session.permanent = True
                return redirect(next_url)
            flash("Incorrect password.", "danger")
        return render_template("login.html")

    @app.post("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    cache_root = _cache_dir()
    upstream_zip = os.environ.get("AYCF_UPSTREAM_ZIP", "https://github.com/markvincevarga/wizzair-aycf-availability/archive/refs/heads/main.zip")
    refresh_seconds = _env_int("AYCF_REFRESH_SECONDS", 21600, 300, 604800)
    upd = update_data_if_needed(cache_root=cache_root, upstream_zip_url=upstream_zip, refresh_interval_seconds=refresh_seconds, force=False)
    direct_dir = Path(cache_root) / "direct-data"
    graph = CurrentRouteGraph(str(direct_dir) if direct_dir.exists() and any(direct_dir.glob("*.csv")) else upd.data_dir)
    db = ScanCacheDB()

    def airport_code(value: str | None) -> str:
        return _STATION_ALIASES.get(normalize_name(value or ""), "")

    app.jinja_env.globals["airport_code"] = airport_code

    def canonical_city(value: str) -> str | None:
        wanted = normalize_name(value)
        if not wanted:
            return None
        cities = graph.cities()
        by_key = {normalize_name(city): city for city in cities}
        if wanted in by_key:
            return by_key[wanted]
        for group, members in AIRPORT_GROUPS.items():
            if wanted in {normalize_name(x) for x in members} and group in by_key:
                return by_key[group]
        return None

    def route_catalog():
        frame = graph.latest_frame()
        pairs = sorted(set(zip(frame["departure_from"], frame["departure_to"])))
        origins = sorted(set(frame["departure_from"]))
        destinations = sorted(set(frame["departure_to"]))
        generated = str(frame["data_generated"].iloc[0]).strip() if "data_generated" in frame.columns and len(frame) else ""
        return frame, pairs, origins, destinations, generated

    def current_scope_run():
        _, pairs, origins, destinations, generated = route_catalog()
        scope = load_scope()
        seconds_per_check = _env_float("AYCF_SCAN_SECONDS_PER_CHECK", 1.25, 0.2, 10.0)
        plan = scan_plan(pairs, scope, days=4, seconds_per_request=seconds_per_check)
        selected_pairs = plan["routes"]
        scope_id = scope_fingerprint(scope)
        run_id = None
        if generated and selected_pairs:
            run_id = hashlib.sha256((generated + "\n" + scope_id + "\n" + "\n".join(f"{a}>{b}" for a, b in selected_pairs)).encode()).hexdigest()[:20]
        run = db.get_pdf_run(run_id) if run_id else None
        hub_candidates = sorted(set(origins).intersection(destinations))
        return {"scope": scope, "scope_id": scope_id, "summary": scope_summary(scope), "pairs": selected_pairs, "primary_pairs": plan["primary_routes"], "hub_pairs": plan["hub_routes"], "checks": plan["checks"], "estimated_minutes": plan["estimated_minutes"], "origins": origin_options(origins), "destinations": destinations, "hub_candidates": hub_candidates, "generated": generated, "run_id": run_id, "run": run, "ready": bool(run and run.get("scanned_at"))}

    def launch_morning_scan():
        log_dir = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf"))) / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log = open(log_dir / "manual-morning.log", "ab", buffering=0)
        subprocess.Popen([sys.executable, str(ROOT / "termux" / "runtime.py"), "morning"], cwd=str(ROOT), env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

    def approved_connections(items, scope):
        approved = {normalize_name(x) for x in scope.get("connection_hubs") or []}
        out = []
        for item in items:
            path = item.get("path") or []
            if len(path) <= 2:
                out.append(item)
                continue
            intermediate = path[1:-1]
            if approved and all(normalize_name(hub) in approved for hub in intermediate):
                out.append(item)
        return out

    def decorate_itineraries(items, max_journey_minutes=0):
        out = []
        for item in items:
            legs = item.get("legs") or []
            if not legs:
                continue
            first, last = legs[0], legs[-1]
            try:
                dep = datetime.fromisoformat(first["departure"])
                arr = datetime.fromisoformat(last["arrival"])
                total_minutes = max(0, int((arr - dep).total_seconds() // 60))
            except Exception:
                total_minutes = 0
            if max_journey_minutes and total_minutes > max_journey_minutes:
                continue
            path = item.get("path") or [first.get("origin"), last.get("destination")]
            waits = item.get("connection_minutes_list")
            if not isinstance(waits, list):
                waits = []
                for previous, following in zip(legs, legs[1:]):
                    try:
                        wait = datetime.fromisoformat(following["departure"]) - datetime.fromisoformat(previous["arrival"])
                        waits.append(max(0, int(wait.total_seconds() // 60)))
                    except Exception:
                        waits.append(0)
            connections = []
            for idx, minutes in enumerate(waits):
                connections.append({"hub": path[idx + 1] if idx + 1 < len(path) - 1 else "", "minutes": int(minutes), "risky": 120 <= int(minutes) < 150})
            row = dict(item)
            row.update({"origin": path[0], "destination": path[-1], "hubs": path[1:-1], "hub": " + ".join(path[1:-1]), "stop_count": max(0, len(legs) - 1), "is_direct": len(legs) == 1, "total_minutes": total_minutes, "connections": connections, "connection_minutes_list": waits, "connection_minutes": min(waits) if waits else None, "risky_connection": any(c["risky"] for c in connections), "departure_time": first.get("departure", "")[11:16], "arrival_time": last.get("arrival", "")[11:16]})
            out.append(row)
        out.sort(key=lambda r: (r["stop_count"], r["risky_connection"], r.get("total_minutes", 0), r["legs"][0].get("departure", "")))
        return out

    @app.get("/")
    def index():
        scope_ctx = current_scope_run()
        vault = _vault_or_none()
        return render_template("index.html", cities=graph.cities(), connected=bool(vault and vault.exists()), default_start=date.today().isoformat(), default_return=(date.today() + timedelta(days=2)).isoformat(), cache_stats=db.stats(), scope_ctx=scope_ctx, selected_origin_keys={normalize_name(x) for x in scope_ctx["scope"]["origins"]}, selected_destination_keys={normalize_name(x) for x in scope_ctx["scope"]["destinations"]}, selected_hub_keys={normalize_name(x) for x in scope_ctx["scope"].get("connection_hubs", [])})

    @app.post("/settings/scan-scope")
    def update_scan_scope():
        if not csrf_ok():
            flash("Your settings form expired. Please try again.", "warning")
            return redirect(url_for("index"))
        _, _, pdf_origins, valid_destinations, _ = route_catalog()
        valid_origins = origin_options(pdf_origins)
        valid_hubs = sorted(set(pdf_origins).intersection(valid_destinations))
        origin_map = {normalize_name(x): x for x in valid_origins}
        destination_map = {normalize_name(x): x for x in valid_destinations}
        hub_map = {normalize_name(x): x for x in valid_hubs}
        origins = [origin_map[k] for k in (normalize_name(x) for x in request.form.getlist("scope_origins")) if k in origin_map]
        destinations = [destination_map[k] for k in (normalize_name(x) for x in request.form.getlist("scope_destinations")) if k in destination_map]
        hubs = [hub_map[k] for k in (normalize_name(x) for x in request.form.getlist("connection_hubs")) if k in hub_map]
        try:
            save_scope(origins, request.form.get("destination_mode", "all"), destinations, hubs)
        except ValueError as exc:
            flash(str(exc), "warning")
            return redirect(url_for("index"))
        flash("Morning scope saved. Forward and reverse UK/hub legs are included when the PDF offers them.", "success")
        if request.form.get("run_now") == "1":
            launch_morning_scan(); flash("Two-way morning cache scan started in the background.", "info")
        return redirect(url_for("index"))

    @app.post("/morning/run")
    def run_morning_now():
        if not csrf_ok():
            flash("Your form expired. Please try again.", "warning")
            return redirect(url_for("index"))
        launch_morning_scan(); flash("Morning cache scan started in the background. Refresh this page for status.", "info")
        return redirect(url_for("index"))

    @app.post("/scan")
    def scan():
        if not csrf_ok():
            flash("Your form expired. Please submit the scan again.", "warning")
            return redirect(url_for("index"))
        submitted_origins = request.form.getlist("origins") or request.form.getlist("origin")
        if not submitted_origins and request.form.get("origin"):
            submitted_origins = [request.form.get("origin")]
        raw_origins = [str(x).strip() for x in submitted_origins if str(x).strip()]
        canonical_origins = []
        for raw in raw_origins:
            city = canonical_city(raw)
            if city and city not in canonical_origins:
                canonical_origins.append(city)
        if not canonical_origins:
            flash("Select at least one starting airport.", "warning")
            return redirect(url_for("index"))
        destination_raw = (request.form.get("destination") or "").strip()
        destination = canonical_city(destination_raw) if destination_raw else None
        if destination_raw and not destination:
            flash("Choose a destination from the current AYCF route list.", "warning")
            return redirect(url_for("index"))
        canonical_origins = [o for o in canonical_origins if o != destination]
        if not canonical_origins:
            flash("At least one starting airport must differ from the destination.", "warning")
            return redirect(url_for("index"))
        try:
            start_day = date.fromisoformat((request.form.get("start_date") or "").strip())
        except ValueError:
            start_day = date.today()
        start_day = max(start_day, date.today())
        days = _form_int("days", 4, 1, 4)
        max_stops = _form_int("max_stops", 1, 0, 2)
        min_transfer = _form_int("min_transfer_minutes", 120, 120, 600)
        max_layover = _form_int("max_layover_minutes", 480, 120, 1080)
        max_journey = _form_int("max_journey_minutes", 720, 0, 2160)
        wants_return = request.form.get("return_trip") == "on" and bool(destination)
        try:
            return_start = date.fromisoformat((request.form.get("return_start_date") or "").strip()) if wants_return else start_day
        except ValueError:
            return_start = start_day
        return_start = max(return_start, start_day)
        scope_ctx = current_scope_run()
        if not scope_ctx["ready"]:
            flash("The new two-way scan scope has not completed yet. Run the morning cache first.", "warning")
            return redirect(url_for("index"))
        cache_run_id = scope_ctx["run_id"]
        max_results = _env_int("AYCF_MAX_RESULTS", 100, 1, 500)
        max_paths = _env_int("AYCF_MAX_PATHS_PER_DAY", 250, 10, 1000)
        outbound, returns = [], []
        cache_misses = 0
        try:
            seen = set()
            for origin in canonical_origins:
                found, misses = cached_scan_itineraries(graph, db, origin, destination, start_day, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, limit=max_results, max_paths_per_day=max_paths, pdf_run_id=cache_run_id, max_transfer_minutes=max_layover)
                cache_misses += misses
                for item in approved_connections(found, scope_ctx["scope"]):
                    sig = tuple((leg.get("flight_code"), leg.get("departure"), leg.get("arrival")) for leg in item.get("legs") or [])
                    if sig not in seen:
                        seen.add(sig); outbound.append(item)
            if wants_return:
                seen_return = set()
                for target_origin in canonical_origins:
                    found, misses = cached_scan_itineraries(graph, db, destination, target_origin, return_start, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, limit=max_results, max_paths_per_day=max_paths, pdf_run_id=cache_run_id, max_transfer_minutes=max_layover)
                    cache_misses += misses
                    for item in approved_connections(found, scope_ctx["scope"]):
                        sig = tuple((leg.get("flight_code"), leg.get("departure"), leg.get("arrival")) for leg in item.get("legs") or [])
                        if sig not in seen_return:
                            seen_return.add(sig); returns.append(item)
        except Exception as exc:
            app.logger.exception("Cached AYCF search failed")
            flash(f"Cache search failed safely: {exc}", "danger")
            return redirect(url_for("index"))
        outbound = decorate_itineraries(outbound, max_journey)
        returns = decorate_itineraries(returns, max_journey)
        display_origins = raw_origins or canonical_origins
        hubs = sorted({hub for row in outbound + returns for hub in row.get("hubs", [])})
        return render_template("results.html", outbound=outbound, returns=returns, origins=display_origins, origin=" + ".join(display_origins), destination=destination_raw or destination, start_date=start_day.isoformat(), return_start_date=return_start.isoformat() if wants_return else None, days=days, max_stops=max_stops, min_transfer_minutes=min_transfer, max_layover_minutes=max_layover, max_journey_minutes=max_journey, live_requests=0, return_requested=wants_return, result_source="morning-cache", cache_misses=cache_misses, cache_stats=db.stats(), result_hubs=hubs)

    @app.get("/flights")
    def all_flights():
        scope_ctx = current_scope_run()
        rows = []
        catalog_origins, catalog_destinations = set(), set()
        available_dates = []
        origin_q = destination_q = day_q = flight_q = ""
        if scope_ctx["ready"]:
            with db.connect() as conn:
                catalog_origins = {r["origin"] for r in conn.execute("SELECT DISTINCT origin FROM route_flights WHERE pdf_run_id=?", (scope_ctx["run_id"],)).fetchall()}
                catalog_destinations = {r["destination"] for r in conn.execute("SELECT DISTINCT destination FROM route_flights WHERE pdf_run_id=?", (scope_ctx["run_id"],)).fetchall()}
                available_dates = [r["travel_date"] for r in conn.execute("SELECT DISTINCT travel_date FROM route_flights WHERE pdf_run_id=? ORDER BY travel_date", (scope_ctx["run_id"],)).fetchall()]
            sql = "SELECT * FROM route_flights WHERE pdf_run_id=?"
            params = [scope_ctx["run_id"]]
            origin_q = (request.args.get("origin") or "").strip()
            destination_q = (request.args.get("destination") or "").strip()
            day_q = (request.args.get("date") or "").strip()
            flight_q = (request.args.get("flight") or "").strip().upper()
            if origin_q not in catalog_origins:
                origin_q = ""
            if destination_q not in catalog_destinations:
                destination_q = ""
            if day_q:
                try:
                    day_q = date.fromisoformat(day_q).isoformat()
                except ValueError:
                    day_q = ""
                if day_q not in available_dates:
                    day_q = ""
            if origin_q:
                sql += " AND origin=?"; params.append(origin_q)
            if destination_q:
                sql += " AND destination=?"; params.append(destination_q)
            if day_q:
                sql += " AND travel_date=?"; params.append(day_q)
            if flight_q:
                sql += " AND UPPER(flight_code) LIKE ?"; params.append(f"%{flight_q}%")
            sql += " ORDER BY travel_date, departure, origin, destination LIMIT 1000"
            with db.connect() as conn:
                raw_rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
            seen = set()
            for row in raw_rows:
                try:
                    dep = datetime.fromisoformat(row["departure"])
                    arr = datetime.fromisoformat(row["arrival"])
                except Exception:
                    continue
                if arr <= dep or dep.date().isoformat() != row.get("travel_date"):
                    continue
                sig = (row.get("origin"), row.get("destination"), row.get("flight_code"), row.get("departure"), row.get("arrival"))
                if sig in seen:
                    continue
                seen.add(sig)
                row["origin_code"] = airport_code(row.get("origin"))
                row["destination_code"] = airport_code(row.get("destination"))
                rows.append(row)
        filters = {"origin": origin_q, "destination": destination_q, "date": day_q, "flight": flight_q}
        return render_template("flights.html", flights=rows, scope_ctx=scope_ctx, origins=sorted(catalog_origins), destinations=sorted(catalog_destinations), available_dates=available_dates, filters=filters)

    @app.post("/admin/wizz/session")
    def import_wizz_session():
        if not _admin_ok(request): return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not request.is_json: return jsonify({"ok": False, "error": "JSON body required"}), 415
        vault = _vault_or_none()
        if not vault: return jsonify({"ok": False, "error": "AYCF_SESSION_ENCRYPTION_KEY is not configured"}), 503
        obj = request.get_json(silent=True)
        if not isinstance(obj, dict) or not isinstance(obj.get("cookies"), list): return jsonify({"ok": False, "error": "Expected Playwright storage_state JSON"}), 400
        try:
            client = WizzAYCFClient(obj); probe = client.bootstrap(); vault.save(obj); return jsonify({"ok": True, "probe": probe})
        except Exception as exc: return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/admin/wizz/disconnect")
    def disconnect_wizz():
        token = request.headers.get("X-AYCF-Admin-Token") or request.form.get("admin_token", ""); expected = os.environ.get("AYCF_ADMIN_TOKEN", "")
        if not expected or not hmac.compare_digest(expected, token): return jsonify({"ok": False, "error": "unauthorized"}), 401
        vault = _vault_or_none()
        if vault: vault.clear()
        flash("Wizz session removed.", "success"); return redirect(url_for("index"))

    @app.post("/refresh")
    def refresh():
        if not csrf_ok():
            flash("Your refresh form expired. Please try again.", "warning")
            return redirect(url_for("index"))
        update_data_if_needed(cache_root=cache_root, upstream_zip_url=upstream_zip, refresh_interval_seconds=refresh_seconds, force=True); graph.invalidate(); flash("Route data refreshed. The official morning scan remains the cache source of truth.", "success"); return redirect(url_for("index"))

    @app.get("/health")
    def health():
        result = {"ok": True}
        if session.get("aycf_authenticated"):
            vault = _vault_or_none(); scope_ctx = current_scope_run(); result.update({"wizz_session_configured": bool(vault), "wizz_session_connected": bool(vault and vault.exists()), "cache": db.stats(), "scope": {"id": scope_ctx["scope_id"], "ready": scope_ctx["ready"], "routes": len(scope_ctx["pairs"]), "priority_routes": len(scope_ctx["primary_pairs"]), "hub_routes": len(scope_ctx["hub_pairs"]), "estimated_minutes": scope_ctx["estimated_minutes"], "summary": scope_ctx["summary"]}})
        return result

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host=os.environ.get("AYCF_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8080")))
