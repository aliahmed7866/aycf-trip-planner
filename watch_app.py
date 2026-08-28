import os

from app import create_app as create_base_app
from cache_db import ScanCacheDB
from watch_blueprint import create_watch_blueprint


def create_app():
    app = create_base_app()
    app.register_blueprint(create_watch_blueprint(ScanCacheDB()))
    return app


app = create_app()

if __name__ == "__main__":
    app.run(host=os.environ.get("AYCF_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8080")))
