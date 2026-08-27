#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
ENV_FILE="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}/env"
[ -f "$ENV_FILE" ] || { echo "Missing $ENV_FILE; run termux/setup.sh first." >&2; exit 1; }
source "$ENV_FILE"
cd "$APP_DIR"
exec python app.py
