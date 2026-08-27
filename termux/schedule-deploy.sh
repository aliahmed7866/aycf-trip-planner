#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
SCRIPT="$APP_DIR/termux/auto-deploy.sh"
chmod 700 "$SCRIPT"
termux-job-scheduler \
  --script "$SCRIPT" \
  --job-id 2610 \
  --period-ms 900000 \
  --network any \
  --battery-not-low true \
  --storage-not-low true \
  --persisted true

echo "Scheduled CI-gated AYCF deploy check every ~15 minutes."
termux-job-scheduler --pending
