#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
umask 077
REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
SHOP_DIR="$REPO_DIR/mahrukh"
MAHRUKH_DATA="${MAHRUKH_INSTANCE:-$SHOP_DIR/instance}"
mkdir -p "$MAHRUKH_DATA"
MAHRUKH_DATA="$(cd -- "$MAHRUKH_DATA" && pwd)"
export MAHRUKH_INSTANCE="$MAHRUKH_DATA"
if ! command -v python >/dev/null 2>&1; then
  echo 'Install Python first: pkg install python python-pip'
  exit 1
fi
if [ ! -d "$SHOP_DIR/.venv" ]; then
  python -m venv "$SHOP_DIR/.venv"
fi
"$SHOP_DIR/.venv/bin/python" -m pip install -r "$SHOP_DIR/requirements.txt"
if [ ! -f "$MAHRUKH_DATA/config.json" ]; then
  if [ -t 0 ]; then
    "$SHOP_DIR/.venv/bin/python" "$SHOP_DIR/setup.py"
  else
    echo "First-time setup requires a terminal. Run: bash '$REPO_DIR/termux/install-mahrukh.sh'"
    exit 1
  fi
fi
# Read config without evaluating shell content. Register only this app and preserve every other entry.
"$SHOP_DIR/.venv/bin/python" "$SHOP_DIR/register.py"
if [ -z "${PREFIX:-}" ] || ! command -v sv >/dev/null 2>&1; then
  echo 'Configuration complete. Foreground start:'
  echo "cd '$SHOP_DIR' && .venv/bin/python run.py"
  echo 'Optional background service: pkg install termux-services; reopen Termux, then rerun this installer.'
  exit 0
fi
SERVICE_DIR="$PREFIX/var/service/mahrukh"
mkdir -p "$SERVICE_DIR"
# Keep an existing service down during changes; do not restart other applications.
if [ -f "$SERVICE_DIR/run" ]; then
  sv down "$SERVICE_DIR" || { echo 'Could not stop the existing Mahrukh service.'; exit 1; }
fi
"$SHOP_DIR/.venv/bin/python" "$SHOP_DIR/register.py" --check-port
"$SHOP_DIR/.venv/bin/python" - "$SERVICE_DIR/run" "$SHOP_DIR" "$MAHRUKH_DATA" <<'PY'
from pathlib import Path
import shlex
import sys
run, folder, data = sys.argv[1:]
Path(run).write_text('#!/data/data/com.termux/files/usr/bin/bash\nexec 2>&1\numask 077\ncd ' + shlex.quote(folder) + '\nexport MAHRUKH_INSTANCE=' + shlex.quote(data) + '\nexec .venv/bin/python run.py\n')
Path(run).chmod(0o700)
PY
sv-enable mahrukh
sv up "$SERVICE_DIR"
"$SHOP_DIR/.venv/bin/python" "$SHOP_DIR/register.py" --health
