import os

from termux import runtime

# Apply the same Termux reliability/runtime configuration as the normal web entrypoint.
runtime._patch_scanner(runtime._load_runtime())
runtime._patch_transport()
runtime._repair_cached_flight_dates()
runtime._ensure_pdf_catalogue()
os.environ["AYCF_WEB_PROCESS"] = "true"

from app import create_app as create_base_app
from cache_db import ScanCacheDB
from termux.health_ui import bp as system_health_bp
from termux.multi_search import bp as multi_search_bp
from watch_blueprint import create_watch_blueprint


def create_app():
    app = create_base_app()
    app.register_blueprint(system_health_bp)
    app.register_blueprint(multi_search_bp)
    app.register_blueprint(create_watch_blueprint(ScanCacheDB()))
    app.jinja_env.globals["system_health_enabled"] = True
    app.jinja_env.globals["multi_search_enabled"] = True
    app.jinja_env.globals["watches_enabled"] = True
    return app


app = create_app()


if __name__ == "__main__":
    app.run(
        host=os.environ.get("AYCF_BIND_HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8080")),
    )
