import hmac
import os
import secrets
from datetime import date, timedelta
from urllib.parse import urlsplit

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for

from data_updater import update_data_if_needed
from scanner import CurrentRouteGraph, WizzAYCFClient, scan_itineraries
from session_vault import SessionVault


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
    upstream_zip = os.environ.get(
        "AYCF_UPSTREAM_ZIP",
        "https://github.com/markvincevarga/wizzair-aycf-availability/archive/refs/heads/main.zip",
    )
    refresh_seconds = _env_int("AYCF_REFRESH_SECONDS", 21600, 300, 604800)

    upd = update_data_if_needed(
        cache_root=cache_root,
        upstream_zip_url=upstream_zip,
        refresh_interval_seconds=refresh_seconds,
        force=False,
    )
    graph = CurrentRouteGraph(upd.data_dir)

    def session_state():
        vault = _vault_or_none()
        return vault.load() if vault else None

    def canonical_city(value: str) -> str | None:
        wanted = (value or "").strip().casefold()
        if not wanted:
            return None
        for city in graph.cities():
            if city.casefold() == wanted:
                return city
        return None

    @app.get("/")
    def index():
        cities = graph.cities()
        vault = _vault_or_none()
        connected = bool(vault and vault.exists())
        today = date.today()
        return render_template(
            "index.html",
            cities=cities,
            connected=connected,
            default_start=today.isoformat(),
            default_return=(today + timedelta(days=2)).isoformat(),
        )

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

        start_raw = (request.form.get("start_date") or "").strip()
        try:
            start_day = date.fromisoformat(start_raw) if start_raw else date.today()
        except ValueError:
            start_day = date.today()
        today = date.today()
        if start_day < today:
            start_day = today

        try:
            days = max(1, min(4, int(request.form.get("days") or 4)))
        except ValueError:
            days = 4
        try:
            max_stops = max(0, min(1, int(request.form.get("max_stops") or 1)))
        except ValueError:
            max_stops = 1
        try:
            min_transfer = max(90, min(600, int(request.form.get("min_transfer_minutes") or 150)))
        except ValueError:
            min_transfer = 150

        wants_return = request.form.get("return_trip") == "on" and bool(destination)
        return_start_raw = (request.form.get("return_start_date") or "").strip()
        try:
            return_start = date.fromisoformat(return_start_raw) if return_start_raw else start_day
        except ValueError:
            return_start = start_day
        if return_start < start_day:
            return_start = start_day

        state = session_state()
        if not state:
            flash("Connect your Wizz account before scanning.", "danger")
            return redirect(url_for("index"))

        max_results = _env_int("AYCF_MAX_RESULTS", 100, 1, 500)
        max_paths = _env_int("AYCF_MAX_PATHS_PER_DAY", 200, 10, 1000)
        try:
            client = WizzAYCFClient(
                state,
                cache_ttl=_env_int("AYCF_LIVE_CACHE_SECONDS", 300, 30, 3600),
                min_delay=_env_float("AYCF_MIN_REQUEST_DELAY", 1.0, 0.2, 10.0),
            )
            client.bootstrap()
            outbound = scan_itineraries(
                graph,
                client,
                origin,
                destination,
                start_day,
                days=days,
                max_stops=max_stops,
                min_transfer_minutes=min_transfer,
                limit=max_results,
                max_paths_per_day=max_paths,
            )
            returns = []
            if wants_return:
                returns = scan_itineraries(
                    graph,
                    client,
                    destination,
                    origin,
                    return_start,
                    days=days,
                    max_stops=max_stops,
                    min_transfer_minutes=min_transfer,
                    limit=max_results,
                    max_paths_per_day=max_paths,
                )
        except Exception as exc:
            app.logger.exception("AYCF scan failed")
            flash(str(exc), "danger")
            return redirect(url_for("index"))

        return render_template(
            "results.html",
            outbound=outbound,
            returns=returns,
            origin=origin,
            destination=destination,
            start_date=start_day.isoformat(),
            return_start_date=return_start.isoformat() if wants_return else None,
            days=days,
            min_transfer_minutes=min_transfer,
            live_requests=client.live_requests,
            return_requested=wants_return,
        )

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
        if len(obj.get("cookies", [])) > 500:
            return jsonify({"ok": False, "error": "Storage state contains too many cookies"}), 400
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
        if request.accept_mimetypes.accept_html:
            flash("Wizz session removed.", "success")
            return redirect(url_for("index"))
        return jsonify({"ok": True})

    @app.post("/refresh")
    def refresh():
        update_data_if_needed(
            cache_root=cache_root,
            upstream_zip_url=upstream_zip,
            refresh_interval_seconds=refresh_seconds,
            force=True,
        )
        graph.invalidate()
        flash("Current AYCF PDF data refreshed.", "success")
        return redirect(url_for("index"))

    @app.get("/health")
    def health():
        result = {"ok": True}
        if session.get("aycf_authenticated"):
            vault = _vault_or_none()
            result.update(
                {
                    "wizz_session_configured": bool(vault),
                    "wizz_session_connected": bool(vault and vault.exists()),
                }
            )
        return result

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
