#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
STATE_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}"
CONFIG_DIR="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}"
LOG_DIR="$STATE_DIR/logs"
STATUS_FILE="$STATE_DIR/deploy-status.txt"
LOCK_DIR="$STATE_DIR/deploy.lock"
DEPLOY_REF="${AYCF_DEPLOY_REF:-deploy/termux}"
mkdir -p "$LOG_DIR" "$CONFIG_DIR" "$HOME/.termux/boot"

log() { printf '[AYCF deploy] %s\n' "$*"; }
status() { printf '%s\n' "$*" > "$STATUS_FILE"; }

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  log "Another deployment is already running; skipping."
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

cd "$APP_DIR"

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  log "Working tree has local changes; deployment deferred."
  status "deferred dirty $(date -u +%FT%TZ)"
  exit 0
fi

if pgrep -f 'termux/runtime.py morning' >/dev/null 2>&1 || pgrep -f 'termux/automated_morning.py' >/dev/null 2>&1; then
  log "Morning scan is active; deployment deferred."
  status "deferred scan-active $(date -u +%FT%TZ)"
  exit 0
fi

log "Fetching validated deployment branch $DEPLOY_REF..."
git fetch --quiet origin "$DEPLOY_REF"
TARGET="$(git rev-parse FETCH_HEAD)"
CURRENT="$(git rev-parse HEAD)"
if [ "$TARGET" = "$CURRENT" ]; then
  status "current $CURRENT $(date -u +%FT%TZ)"
  exit 0
fi

if ! git merge-base --is-ancestor "$CURRENT" "$TARGET"; then
  log "Validated deploy commit is not a fast-forward from local HEAD; refusing automatic update."
  status "blocked non-fast-forward target=$TARGET current=$CURRENT $(date -u +%FT%TZ)"
  exit 1
fi

log "Deploying $CURRENT -> $TARGET"
git merge --ff-only "$TARGET"
chmod 700 termux/*.sh

# Termux provides native builds of pandas/cryptography via pkg. Installing the
# generic requirements file here can make pip try to compile cryptography with
# maturin/rust for Android, which is unsupported. Keep phone deployments on the
# Termux-specific pure-Python dependency set instead.
python -m pip install -r requirements-termux.txt --disable-pip-version-check -q

# Add admin-hub configuration without replacing existing secrets or app state.
ENV_FILE="$CONFIG_DIR/env"
if [ -f "$ENV_FILE" ]; then
  grep -q '^export AYCF_ADMIN_BIND_HOST=' "$ENV_FILE" || printf "\nexport AYCF_ADMIN_BIND_HOST='127.0.0.1'\n" >> "$ENV_FILE"
  grep -q '^export AYCF_ADMIN_PORT=' "$ENV_FILE" || printf "export AYCF_ADMIN_PORT='8079'\n" >> "$ENV_FILE"
fi
REGISTRY_FILE="$CONFIG_DIR/apps.json"
if [ ! -f "$REGISTRY_FILE" ] && [ -f "$APP_DIR/termux/apps.json.example" ]; then
  cp "$APP_DIR/termux/apps.json.example" "$REGISTRY_FILE"
  chmod 600 "$REGISTRY_FILE"
fi
if [ -f "$APP_DIR/termux/run-admin.sh" ]; then
  cat > "$HOME/.termux/boot/05-aycf-admin" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
exec '$APP_DIR/termux/run-admin.sh'
EOF
  chmod 700 "$HOME/.termux/boot/05-aycf-admin"
fi

# Keep the Android schedules synchronized with the deployed scripts.
./termux/schedule-morning.sh >/dev/null 2>&1 || true
./termux/schedule-deploy.sh >/dev/null 2>&1 || true

# Restart only the local web/admin processes. Never interrupt an active morning worker.
pkill -f 'termux/runtime.py web' >/dev/null 2>&1 || true
pkill -f 'termux/admin_hub.py' >/dev/null 2>&1 || true
sleep 1
nohup ./termux/run-web.sh >> "$LOG_DIR/web.log" 2>&1 < /dev/null &
if [ -x ./termux/run-admin.sh ]; then
  nohup ./termux/run-admin.sh >> "$LOG_DIR/admin.log" 2>&1 < /dev/null &
fi

# Give Flask a moment to bind, then verify the local health endpoints.
sleep 4
if python - <<'PY'
from urllib.request import urlopen

def healthy(url):
    try:
        with urlopen(url, timeout=5) as r:
            return 200 <= r.status < 300
    except Exception:
        return False

ok = healthy('http://127.0.0.1:8080/health')
admin_required = __import__('pathlib').Path('termux/admin_hub.py').exists()
admin_ok = healthy('http://127.0.0.1:8079/health') if admin_required else True
raise SystemExit(0 if ok and admin_ok else 1)
PY
then
  log "Deployment healthy at $TARGET"
  status "healthy $TARGET $(date -u +%FT%TZ)"
else
  log "Deployment completed but a local health check failed. Check $LOG_DIR/web.log and $LOG_DIR/admin.log"
  status "unhealthy $TARGET $(date -u +%FT%TZ)"
  exit 1
fi
