import os

from flask import Response
from termux import runtime

# Apply the same Termux reliability/runtime configuration as the normal web entrypoint.
runtime._patch_scanner(runtime._load_runtime())
runtime._patch_transport()
runtime._repair_cached_flight_dates()
runtime._ensure_pdf_catalogue()
os.environ["AYCF_WEB_PROCESS"] = "true"

from app import create_app as create_base_app
from cache_db import ScanCacheDB
from stability_blueprint import bp as stability_bp
from termux.health_ui import bp as system_health_bp
from termux.multi_search import bp as multi_search_bp
from watch_blueprint import create_watch_blueprint


_REQUIRED_ENDPOINTS = {
    "index",
    "all_flights",
    "watches.watchlist",
    "watches.add",
    "watches.toggle",
    "watches.remove",
    "watches.check_now",
    "watches.test_notification",
    "stability.page",
    "system_health.page",
    "system_health.status_json",
    "system_health.run_scan",
    "system_health.repair_auth",
    "system_health.check_now",
    "multi_search.scan",
}


def _validate_full_console(app) -> None:
    endpoints = {rule.endpoint for rule in app.url_map.iter_rules()}
    missing = sorted(_REQUIRED_ENDPOINTS - endpoints)
    if missing:
        raise RuntimeError("AYCF full console is missing endpoints: " + ", ".join(missing))


def create_app():
    app = create_base_app()
    app.register_blueprint(system_health_bp)
    app.register_blueprint(multi_search_bp)
    app.register_blueprint(create_watch_blueprint(ScanCacheDB()))
    app.register_blueprint(stability_bp)

    app.jinja_env.globals["system_health_enabled"] = True
    app.jinja_env.globals["multi_search_enabled"] = True
    app.jinja_env.globals["watches_enabled"] = True
    app.jinja_env.globals["stability_enabled"] = True
    app.jinja_env.globals["full_console_enabled"] = True

    @app.get("/favicon.ico")
    def favicon():
        return Response(status=204)

    _validate_full_console(app)
    app.logger.info("AYCF full console ready: planner, flights, watches, stability, system health and multi-search registered")
    return app


app = create_app()


if __name__ == "__main__":
    app.run(host=os.environ.get("AYCF_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8080")))
