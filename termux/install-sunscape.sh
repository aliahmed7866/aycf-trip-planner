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
# Repair uses the current installer, including supervision and port migration fixes.
if [ -n "$(git -C "$APP_DIR" status --porcelain)" ]; then
  echo 'Sunscape has local changes. Preserve them before repairing setup.' >&2
  exit 1
fi
if [ "$(git -C "$APP_DIR" branch --show-current)" != "$BRANCH" ]; then
  echo "Sunscape is not on $BRANCH; the checkout was preserved." >&2
  exit 1
fi
git -C "$APP_DIR" fetch origin "$BRANCH"
git -C "$APP_DIR" merge --ff-only "origin/$BRANCH"
[ -f "$APP_DIR/termux/install-service.sh" ] || { echo 'Sunscape installer is missing. Update the checkout first.' >&2; exit 1; }
# Migrate the former default; the installer checks availability and saves the new endpoint.
if [ "${SUNSCAPE_PORT:-}" = 8081 ]; then unset SUNSCAPE_PORT; fi
export SUNSCAPE_APP_DIR="$APP_DIR"
exec bash "$APP_DIR/termux/install-service.sh"
