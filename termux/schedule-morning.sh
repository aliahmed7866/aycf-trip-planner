#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
SCRIPT="$APP_DIR/termux/morning-gate.sh"
chmod 700 "$SCRIPT"
termux-job-scheduler \
  --script "$SCRIPT" \
  --job-id 2608 \
  --period-ms 900000 \
  --network any \
  --battery-not-low true \
  --storage-not-low true \
  --persisted true

echo "Scheduled AYCF morning gate every ~15 minutes (network work only 06:00-08:59 UTC)."
termux-job-scheduler --pending
