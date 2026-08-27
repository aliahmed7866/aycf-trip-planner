import hashlib
import hmac
import os
import secrets
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for

from cache_db import ScanCacheDB, cached_scan_itineraries
from data_updater import update_data_if_needed
from scan_scope import AIRPORT_GROUPS, load_scope, normalize_name, origin_options, save_scope, scan_plan, scope_fingerprint, scope_summary
from scanner import CurrentRouteGraph, WizzAYCFClient, scan_itineraries
from session_vault import SessionVault

ROOT = Path(__file__).resolve().parent


def _cache_dir() -> str:
    return os.environ.get("AYCF_CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))


def _vault_or_none():
    try:
        return SessionVault()
    except Exception:
        return None


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
        if password and not session.get("aycf_authenticated"):
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

    def session_state():
        vault = _vault_or_none()
        return vault.load() if vault else None

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
        return {
            "scope": scope,
            "scope_id": scope_id,
            "summary": scope_summary(scope),
            "pairs": selected_pairs,
            "primary_pairs": plan["primary_routes"],
            "hub_pairs": plan["hub_routes"],
            "checks": plan["checks"],
            "estimated_minutes": plan["estimated_minutes"],
            "origins": origin_options(origins),
            "destinations": destinations,
            "hub_candidates": hub_candidates,
            "generated": generated,
            "run_id": run_id,
            "run": run,
            "ready": bool(run and run.get("scanned_at")),
        }

    def launch_morning_scan():
        log_dir = Path(os.environ.get("AYCF_STATE_DIR", str(Path.home() / ".local/share/aycf"))) / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log = open(log_dir / "manual-morning.log", "ab", buffering=0)
        subprocess.Popen([sys.executable, str(ROOT / "termux" / "runtime.py"), "morning"], cwd=str(ROOT), env=os.environ.copy(), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

    @app.get("/")
    def index():
        scope_ctx = current_scope_run()
        vault = _vault_or_none()
        return render_template(
            "index.html",
            cities=graph.cities(),
            connected=bool(vault and vault.exists()),
            default_start=date.today().isoformat(),
            default_return=(date.today() + timedelta(days=2)).isoformat(),
            cache_stats=db.stats(),
            scope_ctx=scope_ctx,
            selected_origin_keys={normalize_name(x) for x in scope_ctx["scope"]["origins"]},
            selected_destination_keys={normalize_name(x) for x in scope_ctx["scope"]["destinations"]},
            selected_hub_keys={normalize_name(x) for x in scope_ctx["scope"].get("connection_hubs", [])},
        )

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
        flash("Morning scan scope saved. Priority routes will scan first, followed by reachable selected hubs.", "success")
        if request.form.get("run_now") == "1":
            launch_morning_scan()
            flash("Morning cache scan started in the background.", "info")
        return redirect(url_for("index"))

    @app.post("/morning/run")
    def run_morning_now():
        if not csrf_ok():
            flash("Your form expired. Please try again.", "warning")
            return redirect(url_for("index"))
        launch_morning_scan()
        flash("Morning cache scan started in the background. Refresh this page for status.", "info")
        return redirect(url_for("index"))

    def approved_connections(items, scope):
        hubs = {normalize_name(x) for x in scope.get("connection_hubs") or []}
        if not hubs:
            return [item for item in items if len(item.get("path") or []) <= 2]
        return [
            item for item in items
            if len(item.get("path") or []) <= 2
            or (len(item.get("path") or []) == 3 and normalize_name(item["path"][1]) in hubs)
        ]

    @app.post("/scan")
    def scan():
        if not csrf_ok():
            flash("Your form expired. Please submit the scan again.", "warning")
            return redirect(url_for("index"))
        origin_raw = (request.form.get("origin") or "").strip()
        destination_raw = (request.form.get("destination") or "").strip()
        origin = canonical_city(origin_raw)
        destination = canonical_city(destination_raw) if destination_raw else None
        if not origin:
            flash("Choose an origin from the current AYCF route list.", "warning")
            return redirect(url_for("index"))
        if destination_raw and not destination:
            flash("Choose a destination from the current AYCF route list.", "warning")
            return redirect(url_for("index"))
        if destination and destination == origin:
            flash("Origin and destination must be different.", "warning")
            return redirect(url_for("index"))
        try:
            start_day = date.fromisoformat((request.form.get("start_date") or "").strip())
        except ValueError:
            start_day = date.today()
        start_day = max(start_day, date.today())
        days = _form_int("days", 4, 1, 4)
        max_stops = _form_int("max_stops", 1, 0, 1)
        min_transfer = _form_int("min_transfer_minutes", 150, 90, 600)
        wants_return = request.form.get("return_trip") == "on" and bool(destination)
        try:
            return_start = date.fromisoformat((request.form.get("return_start_date") or "").strip()) if wants_return else start_day
        except ValueError:
            return_start = start_day
        return_start = max(return_start, start_day)

        scope_ctx = current_scope_run()
        if not scope_ctx["ready"]:
            flash("The current scan scope has not completed yet. Run the morning cache first.", "warning")
            return redirect(url_for("index"))
        cache_run_id = scope_ctx["run_id"]
        try:
            outbound, outbound_misses = cached_scan_itineraries(graph, db, origin, destination, start_day, days, max_stops, min_transfer, _env_int("AYCF_MAX_RESULTS", 100, 1, 500), _env_int("AYCF_MAX_PATHS_PER_DAY", 250, 10, 1000), pdf_run_id=cache_run_id)
            outbound = approved_connections(outbound, scope_ctx["scope"])
            returns, return_misses = ([], 0)
            if wants_return:
                returns, return_misses = cached_scan_itineraries(graph, db, destination, origin, return_start, days, max_stops, min_transfer, _env_int("AYCF_MAX_RESULTS", 100, 1, 500), _env_int("AYCF_MAX_PATHS_PER_DAY", 250, 10, 1000), pdf_run_id=cache_run_id)
                returns = approved_connections(returns, scope_ctx["scope"])
        except Exception as exc:
            app.logger.exception("Cached AYCF search failed")
            flash(f"Cache search failed safely: {exc}", "danger")
            return redirect(url_for("index"))

        cache_misses = outbound_misses + return_misses
        live_requests = 0
        source = "morning-cache"
        if cache_misses and os.environ.get("AYCF_ALLOW_LIVE_FALLBACK", "false").lower() == "true":
            state = session_state()
            if state:
                try:
                    client = WizzAYCFClient(state, cache_ttl=_env_int("AYCF_LIVE_CACHE_SECONDS", 300, 30, 3600), min_delay=_env_float("AYCF_MIN_REQUEST_DELAY", 1.0, 0.2, 10.0))
                    client.bootstrap()
                    outbound, out_meta = scan_itineraries(graph, client, origin, destination, start_day, days, max_stops, min_transfer, _env_int("AYCF_MAX_RESULTS", 100, 1, 500), _env_int("AYCF_MAX_PATHS_PER_DAY", 250, 10, 1000))
                    outbound = approved_connections(outbound, scope_ctx["scope"])
                    if wants_return:
                        returns, _ = scan_itineraries(graph, client, destination, origin, return_start, days, max_stops, min_transfer, _env_int("AYCF_MAX_RESULTS", 100, 1, 500), _env_int("AYCF_MAX_PATHS_PER_DAY", 250, 10, 1000))
                        returns = approved_connections(returns, scope_ctx["scope"])
                    live_requests = out_meta.get("live_requests", client.live_requests)
                    source = "live-fallback"
                except Exception as exc:
                    app.logger.exception("Live AYCF fallback failed")
                    flash(f"Live fallback failed: {exc}", "warning")

        return render_template("results.html", outbound=outbound, returns=returns, origin=origin_raw or origin, destination=destination_raw or destination, start_date=start_day.isoformat(), return_start_date=return_start.isoformat() if wants_return else None, days=days, min_transfer_minutes=min_transfer, live_requests=live_requests, return_requested=wants_return, result_source=source, cache_misses=cache_misses, cache_stats=db.stats())

    @app.get("/flights")
    def all_flights():
        scope_ctx = current_scope_run()
        rows = []
        if scope_ctx["ready"]:
            sql = "SELECT * FROM route_flights WHERE pdf_run_id=?"
            params = [scope_ctx["run_id"]]
            origin_q = (request.args.get("origin") or "").strip()
            destination_q = (request.args.get("destination") or "").strip()
            day_q = (request.args.get("date") or "").strip()
            flight_q = (request.args.get("flight") or "").strip()
            if origin_q:
                sql += " AND origin=?"; params.append(origin_q)
            if destination_q:
                sql += " AND destination=?"; params.append(destination_q)
            if day_q:
                sql += " AND travel_date=?"; params.append(day_q)
            if flight_q:
                sql += " AND flight_code LIKE ?"; params.append(f"%{flight_q}%")
            sql += " ORDER BY departure, origin, destination LIMIT 1000"
            with db.connect() as conn:
                rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        origins = sorted({r["origin"] for r in rows})
        destinations = sorted({r["destination"] for r in rows})
        return render_template("flights.html", flights=rows, scope_ctx=scope_ctx, origins=origins, destinations=destinations, filters=request.args)

    @app.post("/admin/wizz/session")
    def import_wizz_session():
        if not _admin_ok(request):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        if not request.is_json:
            return jsonify({"ok": False, "error": "JSON body required"}), 415
        vault = _vault_or_none()
        if not vault:
            return jsonify({"ok": False, "error": "AYCF_SESSION_ENCRYPTION_KEY is not configured"}), 503
        obj = request.get_json(silent=True)
        if not isinstance(obj, dict) or not isinstance(obj.get("cookies"), list):
            return jsonify({"ok": False, "error": "Expected Playwright storage_state JSON"}), 400
        try:
            client = WizzAYCFClient(obj)
            probe = client.bootstrap()
            vault.save(obj)
            return jsonify({"ok": True, "probe": probe})
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/admin/wizz/disconnect")
    def disconnect_wizz():
        token = request.headers.get("X-AYCF-Admin-Token") or request.form.get("admin_token", "")
        expected = os.environ.get("AYCF_ADMIN_TOKEN", "")
        if not expected or not hmac.compare_digest(expected, token):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        vault = _vault_or_none()
        if vault:
            vault.clear()
        flash("Wizz session removed.", "success")
        return redirect(url_for("index"))

    @app.post("/refresh")
    def refresh():
        update_data_if_needed(cache_root=cache_root, upstream_zip_url=upstream_zip, refresh_interval_seconds=refresh_seconds, force=True)
        graph.invalidate()
        flash("Route data refreshed. The official morning scan remains the cache source of truth.", "success")
        return redirect(url_for("index"))

    @app.get("/health")
    def health():
        result = {"ok": True}
        if session.get("aycf_authenticated"):
            vault = _vault_or_none()
            scope_ctx = current_scope_run()
            result.update({"wizz_session_configured": bool(vault), "wizz_session_connected": bool(vault and vault.exists()), "cache": db.stats(), "scope": {"id": scope_ctx["scope_id"], "ready": scope_ctx["ready"], "routes": len(scope_ctx["pairs"]), "priority_routes": len(scope_ctx["primary_pairs"]), "hub_routes": len(scope_ctx["hub_pairs"]), "estimated_minutes": scope_ctx["estimated_minutes"], "summary": scope_ctx["summary"]}})
        return result

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host=os.environ.get("AYCF_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8080")))
