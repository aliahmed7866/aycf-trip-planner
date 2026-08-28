import os

from app import _cache_dir, create_app as create_base_app
from watch_blueprint import create_watch_blueprint


def create_app():
    app = create_base_app()
    app.register_blueprint(create_watch_blueprint(_cache_dir()))
    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
