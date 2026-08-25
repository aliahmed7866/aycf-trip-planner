import hmac
import os
from datetime import date

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

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


def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32)

    cache_root = _cache_dir()
    upstream_zip = os.environ.get(
        "AYCF_UPSTREAM_ZIP",
        "https://github.com/markvincevarga/wizzair-aycf-availability/archive/refs/heads/main.zip",
    )
    refresh_seconds = int(os.environ.get("AYCF_REFRESH_SECONDS", "21600"))

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

    @app.get("/")
    def index():
        cities = graph.cities()
        vault = _vault_or_none()
        connected = bool(vault and vault.exists())
        return render_template(
            "index.html",
            cities=cities,
            connected=connected,
            default_start=date.today().isoformat(),
        )

    @app.post("/scan")
    def scan():
        origin = (request.form.get("origin") or "").strip()
        destination = (request.form.get("destination") or "").strip() or None
        start_raw = (request.form.get("start_date") or "").strip()
        try:
            start_day = date.fromisoformat(start_raw) if start_raw else date.today()
        except ValueError:
            start_day = date.today()
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
        wants_return = request.form.get("return_trip") == "on"

        if not origin:
            flash("Choose an origin.", "warning")
            return redirect(url_for("index"))

        state = session_state()
        if not state:
            flash("Connect your Wizz account before scanning.", "danger")
            return redirect(url_for("index"))

        try:
            client = WizzAYCFClient(
                state,
                cache_ttl=int(os.environ.get("AYCF_LIVE_CACHE_SECONDS", "300")),
                min_delay=float(os.environ.get("AYCF_MIN_REQUEST_DELAY", "1.0")),
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
                limit=int(os.environ.get("AYCF_MAX_RESULTS", "100")),
            )
            returns = []
            if wants_return and destination:
                returns = scan_itineraries(
                    graph,
                    client,
                    destination,
                    origin,
                    start_day,
                    days=days,
                    max_stops=max_stops,
                    min_transfer_minutes=min_transfer,
                    limit=int(os.environ.get("AYCF_MAX_RESULTS", "100")),
                )
        except Exception as exc:
            flash(str(exc), "danger")
            return redirect(url_for("index"))

        return render_template(
            "results.html",
            outbound=outbound,
            returns=returns,
            origin=origin,
            destination=destination,
            start_date=start_day.isoformat(),
            days=days,
            min_transfer_minutes=min_transfer,
        )

    @app.post("/admin/wizz/session")
    def import_wizz_session():
        if not _admin_ok(request):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
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
        flash("Current AYCF PDF data refreshed.", "success")
        return redirect(url_for("index"))

    @app.get("/health")
    def health():
        vault = _vault_or_none()
        return {
            "ok": True,
            "wizz_session_configured": bool(vault),
            "wizz_session_connected": bool(vault and vault.exists()),
            "data_dir": upd.data_dir,
        }

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
