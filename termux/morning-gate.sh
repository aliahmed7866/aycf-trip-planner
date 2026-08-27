#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
ENV_FILE="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}/env"
LOG_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}/logs"
mkdir -p "$LOG_DIR"
source "$ENV_FILE"

# JobScheduler timing is approximate. Wake every ~15 minutes, but do network
# work only during a generous morning publication window. morning_scan.py is
# idempotent for PDF + user scope, so repeated wakes exit without Wizz polling
# after that publication/scope has completed.
HOUR="$(date -u +%H)"
case "$HOUR" in
  06|07|08|09|10) ;;
  *) exit 0 ;;
esac

termux-wake-lock || true
cd "$APP_DIR"
{
  echo "[$(date -u +%FT%TZ)] morning gate"
  python termux/runtime.py morning
} >> "$LOG_DIR/morning.log" 2>&1
