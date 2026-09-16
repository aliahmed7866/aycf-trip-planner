#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
APP_DIR="${AYCF_APP_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
ENV_FILE="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}/env"
LOG_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}/logs"
mkdir -p "$LOG_DIR"
[ -f "$ENV_FILE" ] && source "$ENV_FILE"

termux-wake-lock || true
cd "$APP_DIR"
{
  echo "[$(date -u +%FT%TZ)] supervisor wake"
  supervisor_status=0
  python termux/supervisor.py || supervisor_status=$?
  # Local due/availability checks are cheap; fare work is independently capped.
  python termux/feeder_refresh.py || true
  exit "$supervisor_status"
} >> "$LOG_DIR/supervisor.log" 2>&1
