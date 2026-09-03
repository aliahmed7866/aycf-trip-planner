#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

EXPENSE_DIR="${EXPENSE_APP_DIR:-$HOME/Expense_manager}"
REPOSITORY="${EXPENSE_REPOSITORY:-https://github.com/aliahmed7866/Expense_manager.git}"
BRANCH="${EXPENSE_BRANCH:-main}"

if [ ! -e "$EXPENSE_DIR" ]; then
  git clone --branch "$BRANCH" --depth 1 "$REPOSITORY" "$EXPENSE_DIR"
elif [ ! -d "$EXPENSE_DIR/.git" ]; then
  echo "[Pocketwise] $EXPENSE_DIR exists but is not a git repository." >&2
  exit 1
else
  if [ -n "$(git -C "$EXPENSE_DIR" status --porcelain --untracked-files=no 2>/dev/null)" ]; then
    echo "[Pocketwise] Local tracked changes found; refusing to overwrite them." >&2
    exit 1
  fi
  git -C "$EXPENSE_DIR" fetch --quiet origin "$BRANCH"
  git -C "$EXPENSE_DIR" checkout --quiet "$BRANCH"
  git -C "$EXPENSE_DIR" merge --ff-only --quiet "origin/$BRANCH"
fi

bash "$EXPENSE_DIR/termux/install-service.sh"
bash "$EXPENSE_DIR/termux/install-auto-deploy.sh"
echo "[Pocketwise] Setup complete and automatic updates enabled."
