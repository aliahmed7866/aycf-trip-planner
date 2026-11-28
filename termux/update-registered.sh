#!/data/data/com.termux/files/usr/bin/bash
# Invoked in the registered checkout under the Hub's operation lock.
set -euo pipefail
app="$1"
branch="$2"
case "$app" in notes|muso|akhlaq) ;; *) exit 2 ;; esac
[ "$(git branch --show-current)" = "$branch" ]
[ -z "$(git status --porcelain --untracked-files=no)" ]
git fetch origin "$branch"
git merge --ff-only FETCH_HEAD
case "$app" in
  notes) bash termux/install-service.sh ;;
  muso) .venv/bin/python -m pip install -r requirements.txt ;;
  akhlaq) node --check server.mjs ;;
esac
