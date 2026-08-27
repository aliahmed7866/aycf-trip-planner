#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
STATE_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}"
LOG_DIR="$STATE_DIR/logs"
STATUS_FILE="$STATE_DIR/deploy-status.txt"
LOCK_DIR="$STATE_DIR/deploy.lock"
DEPLOY_REF="${AYCF_DEPLOY_REF:-deploy/termux}"
mkdir -p "$LOG_DIR"

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

python -m pip install -r requirements.txt --disable-pip-version-check -q

# Keep the Android schedules synchronized with the deployed scripts.
./termux/schedule-morning.sh >/dev/null 2>&1 || true
./termux/schedule-deploy.sh >/dev/null 2>&1 || true

# Restart only the local web process. Never interrupt an active morning worker.
pkill -f 'termux/runtime.py web' >/dev/null 2>&1 || true
sleep 1
nohup ./termux/run-web.sh >> "$LOG_DIR/web.log" 2>&1 < /dev/null &

# Give Flask a moment to bind, then verify the local health endpoint.
sleep 4
if python - <<'PY'
from urllib.request import urlopen
try:
    with urlopen('http://127.0.0.1:8080/health', timeout=5) as r:
        ok = 200 <= r.status < 300
except Exception:
    ok = False
raise SystemExit(0 if ok else 1)
PY
then
  log "Deployment healthy at $TARGET"
  status "healthy $TARGET $(date -u +%FT%TZ)"
else
  log "Deployment completed but web health check failed. Check $LOG_DIR/web.log"
  status "unhealthy $TARGET $(date -u +%FT%TZ)"
  exit 1
fi
