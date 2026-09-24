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
# Repair can use an installed recovery helper without updating source files.
# Never stash, reset or overwrite local edits as a side effect of service repair.
if [ -n "$(git -C "$APP_DIR" status --porcelain --untracked-files=no)" ]; then
  if [ ! -f "$APP_DIR/termux/configure.py" ]; then
    echo 'Local changes preserved. This checkout lacks the new Sunscape recovery helper.' >&2
    echo 'Review these changed files before updating Sunscape:' >&2
    git -C "$APP_DIR" status --short --untracked-files=no >&2
    exit 1
  fi
  echo '[Sunscape] Local changes preserved; repairing with the installed setup scripts. Source update skipped.'
else
  if [ "$(git -C "$APP_DIR" branch --show-current)" != "$BRANCH" ]; then
    echo "Sunscape is not on $BRANCH; the checkout was preserved." >&2
    exit 1
  fi
  git -C "$APP_DIR" fetch origin "$BRANCH"
  git -C "$APP_DIR" merge --ff-only "origin/$BRANCH"
fi
[ -f "$APP_DIR/termux/install-service.sh" ] || { echo 'Sunscape installer is missing. Update the checkout first.' >&2; exit 1; }
# Migrate the former default; the installer checks availability and saves the new endpoint.
if [ "${SUNSCAPE_PORT:-}" = 8081 ]; then unset SUNSCAPE_PORT; fi
export SUNSCAPE_APP_DIR="$APP_DIR"
exec bash "$APP_DIR/termux/install-service.sh"
