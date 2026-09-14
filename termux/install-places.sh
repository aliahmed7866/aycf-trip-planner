#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${PLACES_APP_DIR:-$HOME/Places}"
REPO_URL="${PLACES_REPO_URL:-https://github.com/aliahmed7866/Places.git}"

pkg install -y git python >/dev/null

if [ ! -d "$APP_DIR/.git" ]; then
  git clone "$REPO_URL" "$APP_DIR"
else
  git -C "$APP_DIR" fetch origin
  [ "$(git -C "$APP_DIR" branch --show-current)" = main ] || { echo "Places checkout is not on main." >&2; exit 1; }
  git -C "$APP_DIR" pull --ff-only origin main
fi

PLACES_APP_DIR="$APP_DIR" bash "$APP_DIR/termux/install.sh"

# Migration is deliberately safe to repeat. The migration tool reads the old
# AYCF journal read-only and ignores records already present in Places.
if [ -x "$HOME/.local/bin/places" ]; then
  source_db="${AYCF_JOURNAL_DB_PATH:-${AYCF_STATE_DIR:-$HOME/.local/share/aycf}/travel-journal.sqlite3}"
  if [ -n "${AYCF_DB_PATH:-}" ] && [ -z "${AYCF_JOURNAL_DB_PATH:-}" ]; then
    source_db="$(dirname "$AYCF_DB_PATH")/travel-journal.sqlite3"
  fi
  if [ -f "$source_db" ]; then
    "$HOME/.local/bin/places" migrate --source "$source_db"
  fi
  "$HOME/.local/bin/places" start
fi
