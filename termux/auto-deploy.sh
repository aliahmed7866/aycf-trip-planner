#!/data/data/com.termux/files/usr/bin/bash
set -Eeuo pipefail

manual=false
[ "${1:-}" != "--restart" ] || manual=true
requested_port="${PORT:-}"
requested_ref="${AYCF_DEPLOY_REF:-}"
ENV_FILE="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}/env"
[ ! -f "$ENV_FILE" ] || source "$ENV_FILE"
if "$manual"; then
  [ -z "$requested_port" ] || export PORT="$requested_port"
  [ -z "$requested_ref" ] || export AYCF_DEPLOY_REF="$requested_ref"
fi

APP_DIR="${AYCF_APP_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
STATE_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}"
CONFIG_DIR="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}"
LOG_DIR="$STATE_DIR/logs"
STATUS_FILE="$STATE_DIR/deploy-status.txt"
LOCK_FILE="$STATE_DIR/deploy.flock"
DEPLOY_REF="${AYCF_DEPLOY_REF:-deploy/termux}"
mkdir -p "$LOG_DIR" "$CONFIG_DIR" "$HOME/.termux/boot"

log() { printf '[AYCF deploy] %s\n' "$*"; }
write_result() {
  local path="$1"
  shift
  printf '%s\n' "$*" > "$path.tmp"
  mv -f "$path.tmp" "$path"
}
status() {
  log "$*"
  write_result "$STATUS_FILE" "$@"
  [ -z "${AYCF_DEPLOY_RESULT:-}" ] || write_result "$AYCF_DEPLOY_RESULT" "$@"
}

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
  fi
}

services_healthy() {
  python "$APP_DIR/termux/service_health.py" "${1:-all}"
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
  [ -z "${AYCF_DEPLOY_RESULT:-}" ] || write_result "$AYCF_DEPLOY_RESULT" "deferred busy"
  exit 0
fi
export AYCF_APP_DIR="$APP_DIR" AYCF_STATE_DIR="$STATE_DIR"

cd "$APP_DIR"

# Record the exact failed stage, including errors that occur after Git advances.
stage=preflight
trap 'rc=$?; status "failed $stage (exit $rc)"; exit "$rc"' ERR
if [ "$(git branch --show-current)" != "$DEPLOY_REF" ]; then
  status "blocked wrong-branch expected=$DEPLOY_REF"
  exit 1
fi
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

stage=fetch
log "Fetching validated deployment branch $DEPLOY_REF..."
git fetch --quiet origin "$DEPLOY_REF"
TARGET="$(git rev-parse FETCH_HEAD)"
CURRENT="$(git rev-parse HEAD)"
PENDING="$STATE_DIR/deploy-pending"
start_web() {
  [ ! -e "$STATE_DIR/planner.down" ] || return 0
  printf "\n[AYCF startup] %s\n" "$(date -u +%FT%TZ)" >> "$LOG_DIR/web.log"
  nohup bash "$APP_DIR/termux/run-web.sh" >> "$LOG_DIR/web.log" 2>&1 < /dev/null 9>&- &
}
start_admin() {
  [ -f "$APP_DIR/termux/run-admin.sh" ] || return 0
  printf "\n[AYCF startup] %s\n" "$(date -u +%FT%TZ)" >> "$LOG_DIR/admin.log"
  nohup bash "$APP_DIR/termux/run-admin.sh" >> "$LOG_DIR/admin.log" 2>&1 < /dev/null 9>&- &
}
wait_healthy() {
  log "Waiting up to ${AYCF_STARTUP_TIMEOUT:-120}s for the local apps to start..."
  python "$APP_DIR/termux/service_health.py" --wait | tee "$STATE_DIR/startup-status.txt"
}

if [ "$TARGET" = "$CURRENT" ] && [ ! -e "$PENDING" ] && ! "$manual"; then
  stage=configuration
  reconcile_admin
  if ! services_healthy admin && ! python termux/checkout_processes.py "$APP_DIR" --running admin; then
    start_admin
  fi
  if ! services_healthy web && ! python termux/checkout_processes.py "$APP_DIR" --running web; then
    start_web
  fi
  if wait_healthy; then
    status "current $CURRENT $(date -u +%FT%TZ)"
    exit 0
  fi
  status "unhealthy $CURRENT $(tail -1 "$STATE_DIR/startup-status.txt")"
  exit 1
fi

if ! git merge-base --is-ancestor "$CURRENT" "$TARGET"; then
  status "blocked non-fast-forward target=$TARGET current=$CURRENT"
  exit 1
fi

# Write before the merge so a killed updater or failed install can resume.
stage=checkout
write_result "$PENDING" "$TARGET"
if [ "$TARGET" != "$CURRENT" ]; then
  git -c core.fileMode=false merge --ff-only "$TARGET"
fi
chmod 700 termux/*.sh
stage=dependencies
requirements="$(sha256sum requirements-termux.txt)"
if [ "$requirements" != "$(cat "$STATE_DIR/deploy-requirements" 2>/dev/null || true)" ]; then
  python -m pip install -r requirements-termux.txt --disable-pip-version-check -q
  write_result "$STATE_DIR/deploy-requirements" "$requirements"
fi

# Check native Termux modules before stopping a working server.
stage=runtime-dependencies
python - <<'PYDEPS'
import importlib
import sys
failed = []
for name in ("pandas", "cryptography.fernet", "flask", "dateutil", "requests", "websocket"):
    try:
        importlib.import_module(name)
    except Exception as exc:
        failed.append(name)
        print(f"Cannot load {name}: {type(exc).__name__}: {exc}", flush=True)
if failed:
    print("For pandas or cryptography on Termux, run pkg install python-pandas python-cryptography.", flush=True)
    print("Then retry this update. Full output is in the update log.", flush=True)
    raise SystemExit(1)
PYDEPS
stage=configuration
reconcile_admin
bash ./termux/schedule-morning.sh >/dev/null 2>&1 || true
bash ./termux/schedule-deploy.sh >/dev/null 2>&1 || true
stage=restart
python "$APP_DIR/termux/checkout_processes.py" "$APP_DIR" web admin
start_web
start_admin
if wait_healthy; then
  rm -f "$PENDING"
  status "healthy $TARGET $(date -u +%FT%TZ)"
  log "Deployment healthy at $TARGET"
else
  status "unhealthy $TARGET $(tail -1 "$STATE_DIR/startup-status.txt")"
  exit 1
fi
