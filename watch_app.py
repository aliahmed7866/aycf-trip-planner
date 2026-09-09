"""Compatibility entrypoint for the complete Termux web console."""
import os
from termux.runtime import create_web_app

create_app = create_web_app
app = create_app()

if __name__ == "__main__":
    app.run(host=os.environ.get("AYCF_BIND_HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8080")))
