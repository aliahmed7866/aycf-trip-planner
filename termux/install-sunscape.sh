#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
APP_DIR="${SUNSCAPE_APP_DIR:-$HOME/sunscape}"
BRANCH="${SUNSCAPE_BRANCH:-main}"
if [ ! -e "$APP_DIR" ]; then
  git clone --branch "$BRANCH" https://github.com/aliahmed7866/Sunscape.git "$APP_DIR"
elif [ ! -d "$APP_DIR/.git" ]; then
  echo 'Sunscape directory is not a Git checkout; existing files were preserved.' >&2
  exit 1
fi
[ -f "$APP_DIR/termux/install-service.sh" ] || { echo 'Sunscape installer is missing. Update the checkout first.' >&2; exit 1; }
export SUNSCAPE_APP_DIR="$APP_DIR"
exec bash "$APP_DIR/termux/install-service.sh"
