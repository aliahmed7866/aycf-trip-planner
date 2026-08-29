from __future__ import annotations

import os

from app import create_app


app = create_app()


def _termux_health():
    """Cheap liveness/readiness probe for runit and the local admin hub.

    The normal planner data operations can scan hundreds of thousands of CSV
    rows. A health endpoint must never trigger that work: if create_app()
    completed and Flask is serving this route, the web process is ready.
    """
    return {
        "ok": True,
        "service": "aycf",
        "ready": True,
        "auto_login_enabled": os.environ.get("AYCF_AUTO_LOGIN", "").lower() == "true",
        "env_aycf_auto_login": os.environ.get("AYCF_AUTO_LOGIN"),
    }


# create_app() registers the legacy health view. Replace only that view for the
# Termux service so existing application routes and endpoint names stay intact.
app.view_functions["health"] = _termux_health


if __name__ == "__main__":
    app.run(
        host=os.environ.get("AYCF_BIND_HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8080")),
        debug=False,
    )
