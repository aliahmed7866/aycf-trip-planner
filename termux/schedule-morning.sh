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

echo "Scheduled AYCF supervisor every ~15 minutes. Most wakes are local-only; auth health is rate-limited and scans only run in the configured UTC publication window."
termux-job-scheduler --pending
