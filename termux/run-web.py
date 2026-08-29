from __future__ import annotations

import os
import sys
from pathlib import Path

from werkzeug.exceptions import HTTPException

# When this file is executed as `python termux/run-web.py`, Python places the
# `termux/` directory at sys.path[0]. Add the repository root explicitly so the
# application modules can always be imported under runit/Termux.
APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

# AYCF's watch-enabled entrypoint wraps the planner app and registers /watches
# plus its add/toggle/delete/check routes. Import the already-created app so the
# application is initialized only once during service startup.
from watch_app import app


def _termux_health():
    """Cheap liveness/readiness probe for runit and the local admin hub."""
    return {
        "ok": True,
        "service": "aycf",
        "ready": True,
        "auto_login_enabled": os.environ.get("AYCF_AUTO_LOGIN", "").lower() == "true",
        "env_aycf_auto_login": os.environ.get("AYCF_AUTO_LOGIN"),
    }


# Keep normal HTTP errors as HTTP errors. The base planner has a broad
# Exception handler for unexpected failures; without this specific handler a
# normal 404 is presented as a misleading "Server error" page.
@app.errorhandler(HTTPException)
def _http_error(error: HTTPException):
    return error


# The base app registers the legacy health view. Replace only that view for the
# Termux service so health probes never scan the large historical dataset.
app.view_functions["health"] = _termux_health


if __name__ == "__main__":
    app.run(
        host=os.environ.get("AYCF_BIND_HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8080")),
        debug=False,
    )
