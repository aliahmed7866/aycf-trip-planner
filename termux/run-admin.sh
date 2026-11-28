#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
APP_DIR="${AYCF_APP_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
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

cd "$APP_DIR"
# Recover only apps previously switched on in the panel.
if compgen -G "${AYCF_STATE_DIR:-$HOME/.local/share/aycf}/hub-updates/*.on" >/dev/null; then
  python "$APP_DIR/termux/recover_apps.py" >>"${AYCF_STATE_DIR:-$HOME/.local/share/aycf}/logs/app-recovery.log" 2>&1 </dev/null &
fi
exec python termux/admin_hub.py
