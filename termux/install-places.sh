#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${PLACES_APP_DIR:-$HOME/Places}"
REPO_URL="${PLACES_REPO_URL:-https://github.com/aliahmed7866/Places.git}"

pkg install -y git python >/dev/null

if [ ! -d "$APP_DIR/.git" ]; then
  git clone "$REPO_URL" "$APP_DIR"
else
  git -C "$APP_DIR" fetch origin
  git -C "$APP_DIR" checkout main
  git -C "$APP_DIR" pull --ff-only origin main
fi

bash "$APP_DIR/termux/install.sh"

# Migration is deliberately safe to repeat. The migration tool reads the old
# AYCF journal read-only and ignores records already present in Places.
if [ -x "$HOME/.local/bin/places" ]; then
  "$HOME/.local/bin/places" migrate || true
  "$HOME/.local/bin/places" start
fi
