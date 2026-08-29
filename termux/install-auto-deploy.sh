#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
SERVICE_DIR="$PREFIX/var/service/aycf-deploy"
STATE_DIR="${AYCF_DEPLOY_STATE_DIR:-$HOME/.local/state/aycf}"
LOG_FILE="$STATE_DIR/deploy.log"

cd "$APP_DIR"

if ! command -v sv >/dev/null 2>&1; then
  pkg install -y termux-services
fi

mkdir -p "$STATE_DIR"
touch "$LOG_FILE"
chmod 600 "$LOG_FILE"

chmod +x "$APP_DIR/termux/auto-deploy.sh"
mkdir -p "$SERVICE_DIR"
cat > "$SERVICE_DIR/run" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
exec >>"$LOG_FILE" 2>&1
cd "$APP_DIR"
exec "$APP_DIR/termux/auto-deploy.sh"
EOF
chmod +x "$SERVICE_DIR/run"

sv-enable aycf-deploy >/dev/null 2>&1 || true

# Always restart so an already-running watcher loads the latest deployment
# script and service environment instead of continuing with a stale process.
sv restart aycf-deploy >/dev/null 2>&1 || {
  sv down aycf-deploy >/dev/null 2>&1 || true
  sleep 1
  sv up aycf-deploy >/dev/null 2>&1 || true
}

sleep 1
echo "[AYCF] Auto-deploy watcher installed/restarted."
echo "[AYCF] Deploy log: $LOG_FILE"
sv status aycf-deploy || true
