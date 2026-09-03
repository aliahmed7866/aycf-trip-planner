#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

EXPENSE_DIR="${EXPENSE_APP_DIR:-$HOME/Expense_manager}"
REPOSITORY="https://github.com/aliahmed7866/Expense_manager.git"

if [ ! -e "$EXPENSE_DIR" ]; then
  git clone --depth 1 "$REPOSITORY" "$EXPENSE_DIR"
elif [ ! -d "$EXPENSE_DIR/.git" ]; then
  echo "[Expense Manager] $EXPENSE_DIR exists but is not a git repository."
  exit 1
else
  cd "$EXPENSE_DIR"
  if [ -z "$(git status --porcelain --untracked-files=no 2>/dev/null)" ]; then
    git fetch --quiet origin main
    git checkout --quiet main
    git merge --ff-only --quiet origin/main
  else
    echo "[Expense Manager] Local tracked changes found; installing without overwriting them."
  fi
fi

cd "$EXPENSE_DIR"
bash termux/install-service.sh
bash termux/install-auto-deploy.sh
echo "[Expense Manager] Setup complete and automatic updates enabled."
