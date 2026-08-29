#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
CONFIG_DIR="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}"
STATE_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}"
ENV_FILE="$CONFIG_DIR/env"
[ -f "$ENV_FILE" ] || { echo "Missing $ENV_FILE; run termux/setup.sh first." >&2; exit 1; }
source "$ENV_FILE"
export AYCF_APP_DIR="$APP_DIR"

# Keep the admin browser session stable across restarts. If the user already
# provides FLASK_SECRET_KEY in env, respect it; otherwise generate one once and
# persist it outside the Git checkout.
mkdir -p "$STATE_DIR"
if [ -z "${FLASK_SECRET_KEY:-}" ]; then
  SECRET_FILE="$STATE_DIR/admin-flask-secret"
  if [ ! -s "$SECRET_FILE" ]; then
    umask 077
    python - <<'PY' > "$SECRET_FILE.tmp"
import secrets
print(secrets.token_urlsafe(48))
PY
    mv "$SECRET_FILE.tmp" "$SECRET_FILE"
    chmod 600 "$SECRET_FILE"
  fi
  export FLASK_SECRET_KEY="$(cat "$SECRET_FILE")"
fi

# The admin card should follow the actual integrated AYCF process. Repair any
# stale registry entry left by earlier deployment variants without touching
# unrelated apps or secrets.
REGISTRY_PATH="${AYCF_ADMIN_REGISTRY:-$CONFIG_DIR/apps.json}"
python - "$REGISTRY_PATH" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(0)
try:
    payload = json.loads(path.read_text(encoding='utf-8'))
except Exception:
    raise SystemExit(0)
apps = payload.get('apps') if isinstance(payload, dict) else None
if not isinstance(apps, list):
    raise SystemExit(0)
changed = False
for app in apps:
    if isinstance(app, dict) and app.get('id') == 'aycf':
        if app.get('process_match') != 'watch_app.py':
            app['process_match'] = 'watch_app.py'
            changed = True
if changed:
    path.write_text(json.dumps(payload, indent=2) + '\n', encoding='utf-8')
PY
chmod 600 "$REGISTRY_PATH" 2>/dev/null || true

cd "$APP_DIR"
exec python termux/admin_hub.py
