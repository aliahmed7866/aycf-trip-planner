#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

ENV_FILE="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}/env"
[ ! -f "$ENV_FILE" ] || source "$ENV_FILE"

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
STATE_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}"
CONFIG_DIR="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}"
LOG_DIR="$STATE_DIR/logs"
STATUS_FILE="$STATE_DIR/deploy-status.txt"
LOCK_FILE="$STATE_DIR/deploy.flock"
DEPLOY_REF="${AYCF_DEPLOY_REF:-deploy/termux}"
mkdir -p "$LOG_DIR" "$CONFIG_DIR" "$HOME/.termux/boot"

log() { printf '[AYCF deploy] %s\n' "$*"; }
status() { printf '%s\n' "$*" > "$STATUS_FILE"; }

reconcile_admin() {
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
    chmod 700 "$APP_DIR/termux/run-admin.sh"
    cat > "$HOME/.termux/boot/05-aycf-admin" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
exec '$APP_DIR/termux/run-admin.sh'
EOF
    chmod 700 "$HOME/.termux/boot/05-aycf-admin"

    if ! pgrep -f 'termux/admin_hub.py' >/dev/null 2>&1; then
      log "Starting admin hub..."
      nohup bash "$APP_DIR/termux/run-admin.sh" >> "$LOG_DIR/admin.log" 2>&1 < /dev/null 9>&- &
      sleep 2
    fi
  fi
}

services_healthy() {
  python - "${1:-all}" <<'PYHEALTH'
import os
import sys
from pathlib import Path
from urllib.request import urlopen

service = sys.argv[1]
targets = []
if service in {"all", "web"}:
    targets.append(("AYCF", os.environ.get("PORT", "8080")))
if service in {"all", "admin"} and Path(os.environ.get("AYCF_APP_DIR", str(Path.home() / "aycf-trip-planner")), "termux/admin_hub.py").exists():
    targets.append(("Admin", os.environ.get("AYCF_ADMIN_PORT", "8079")))
ok = True
for name, port in targets:
    try:
        with urlopen(f"http://127.0.0.1:{int(port)}/health", timeout=5) as response:
            healthy = 200 <= response.status < 300
    except Exception:
        healthy = False
    if not healthy:
        print(f"[AYCF deploy] {name} health check failed", flush=True)
        ok = False
raise SystemExit(0 if ok else 1)
PYHEALTH
}

# The lock belongs to this open file description, shared with the short Python
# helper. It is released by the OS even if Android kills the updater. Close fd 9
# in persistent web/admin children so they cannot retain the deployment lock.
exec 9>"$LOCK_FILE"
if ! python - <<'PYLOCK'
import fcntl
try:
    fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    raise SystemExit(1)
PYLOCK
then
  log "Another deployment is already running; skipping."
  exit 0
fi
export AYCF_APP_DIR="$APP_DIR"

cd "$APP_DIR"

if [ -n "$(git -c core.fileMode=false status --porcelain --untracked-files=no)" ]; then
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
  # Even on an already-current checkout, reconcile services/config. This is
  # required when a deployment introduced a new service because the previous
  # updater process continues executing its pre-update script from memory.
  reconcile_admin
  if ! services_healthy web && ! pgrep -f 'watch_app.py|termux/runtime.py web' >/dev/null 2>&1; then
    log "Planner is stopped; restarting the current checkout."
    nohup bash "$APP_DIR/termux/run-web.sh" >> "$LOG_DIR/web.log" 2>&1 < /dev/null 9>&- &
    sleep 4
  fi
  if services_healthy; then
    status "current $CURRENT $(date -u +%FT%TZ)"
    exit 0
  fi
  log "Checkout is current but service health checks failed. Check $LOG_DIR/web.log and $LOG_DIR/admin.log"
  status "unhealthy $CURRENT $(date -u +%FT%TZ)"
  exit 1
fi

if ! git merge-base --is-ancestor "$CURRENT" "$TARGET"; then
  log "Validated deploy commit is not a fast-forward from local HEAD; refusing automatic update."
  status "blocked non-fast-forward target=$TARGET current=$CURRENT $(date -u +%FT%TZ)"
  exit 1
fi

log "Deploying $CURRENT -> $TARGET"
git -c core.fileMode=false merge --ff-only "$TARGET"
chmod 700 termux/*.sh

# Termux provides native builds of pandas/cryptography via pkg. Installing the
# generic requirements file here can make pip try to compile cryptography with
# maturin/rust for Android, which is unsupported. Keep phone deployments on the
# Termux-specific pure-Python dependency set instead.
python -m pip install -r requirements-termux.txt --disable-pip-version-check -q

reconcile_admin

# Keep the Android schedules synchronized with the deployed scripts.
bash ./termux/schedule-morning.sh >/dev/null 2>&1 || true
bash ./termux/schedule-deploy.sh >/dev/null 2>&1 || true

# Restart only the local web/admin processes. Never interrupt an active morning worker.
pkill -f 'termux/runtime.py web' >/dev/null 2>&1 || true
pkill -f 'termux/admin_hub.py' >/dev/null 2>&1 || true
sleep 1
nohup bash ./termux/run-web.sh >> "$LOG_DIR/web.log" 2>&1 < /dev/null 9>&- &
if [ -x ./termux/run-admin.sh ]; then
  nohup bash ./termux/run-admin.sh >> "$LOG_DIR/admin.log" 2>&1 < /dev/null 9>&- &
fi

# Give Flask a moment to bind, then verify the local health endpoints.
sleep 4
if services_healthy
then
  log "Deployment healthy at $TARGET"
  status "healthy $TARGET $(date -u +%FT%TZ)"
else
  log "Deployment completed but a local health check failed. Check $LOG_DIR/web.log and $LOG_DIR/admin.log"
  status "unhealthy $TARGET $(date -u +%FT%TZ)"
  exit 1
fi
