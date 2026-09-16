#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
APP_DIR="${AYCF_APP_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
ENV_FILE="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}/env"
[ -f "$ENV_FILE" ] || { echo "Missing $ENV_FILE; run termux/setup.sh first." >&2; exit 1; }
hub_port_override="${PORT:-}"
source "$ENV_FILE"
[ -z "$hub_port_override" ] || export PORT="$hub_port_override"
cd "$APP_DIR"
# Keep the historical process-match text in argv for the admin hub while
# running the integrated watch-enabled entrypoint.
exec python watch_app.py termux/runtime.py web
