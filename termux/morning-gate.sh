#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
ENV_FILE="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}/env"
LOG_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}/logs"
mkdir -p "$LOG_DIR"
[ -f "$ENV_FILE" ] && source "$ENV_FILE"

termux-wake-lock || true
cd "$APP_DIR"
{
  echo "[$(date -u +%FT%TZ)] supervisor wake"
  python termux/supervisor.py
} >> "$LOG_DIR/supervisor.log" 2>&1
