import os

from flask import request

from app import _cache_dir, create_app as create_base_app
from data_updater import update_data_if_needed
from planner import AYCFPlanner
from watch_blueprint import create_watch_blueprint
from watch_service import check_watches, watch_db_path


def _watch_planner(cache_root: str) -> AYCFPlanner:
    upstream_zip = os.environ.get(
        "AYCF_UPSTREAM_ZIP",
        "https://github.com/markvincevarga/wizzair-aycf-availability/archive/refs/heads/main.zip",
    )
    refresh_seconds = int(os.environ.get("AYCF_REFRESH_SECONDS", str(24 * 3600)))
    upd = update_data_if_needed(
        cache_root=cache_root,
        upstream_zip_url=upstream_zip,
        refresh_interval_seconds=refresh_seconds,
        force=False,
    )
    return AYCFPlanner(data_dir=upd.data_dir)


def create_app():
    app = create_base_app()
    cache_root = _cache_dir()
    app.register_blueprint(create_watch_blueprint(cache_root))

    @app.after_request
    def check_watches_after_refresh(response):
        if request.method == "POST" and request.path == "/refresh" and response.status_code < 400:
            try:
                summary = check_watches(
                    watch_db_path(cache_root),
                    _watch_planner(cache_root),
                    notify=True,
                )
                app.logger.info("AYCF watch check after refresh: %s", summary)
            except Exception:
                app.logger.exception("AYCF watch check failed after refresh")
        return response

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
