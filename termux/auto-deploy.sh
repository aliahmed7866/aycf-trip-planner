#!/data/data/com.termux/files/usr/bin/bash
set -u

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
BRANCH="${AYCF_BRANCH:-main}"
SERVICE="${AYCF_SERVICE:-aycf}"
PORT="${AYCF_PORT:-8080}"
INTERVAL="${AYCF_DEPLOY_INTERVAL:-60}"
STATE_DIR="${AYCF_DEPLOY_STATE_DIR:-$HOME/.local/state/aycf}"
LOG_PREFIX="[AYCF deploy]"

mkdir -p "$STATE_DIR"

log() { printf '%s %s\n' "$LOG_PREFIX" "$*"; }

health_ok() {
  curl -fsS --max-time 10 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1
}

deploy_once() {
  cd "$APP_DIR" || { log "App directory missing: $APP_DIR"; return 1; }

  if [ -n "$(git status --porcelain 2>/dev/null)" ]; then
    log "Working tree is dirty; skipping automatic deploy."
    return 1
  fi

  remote_sha="$(git ls-remote origin "refs/heads/$BRANCH" 2>/dev/null | awk 'NR==1{print $1}')"
  [ -n "$remote_sha" ] || { log "Could not read origin/$BRANCH"; return 1; }

  local_sha="$(git rev-parse HEAD 2>/dev/null || true)"
  [ "$remote_sha" != "$local_sha" ] || return 0

  log "New origin/$BRANCH commit detected: ${remote_sha:0:12}"
  git fetch --quiet origin "$BRANCH" || { log "Fetch failed"; return 1; }
  git checkout --quiet "$BRANCH" || { log "Checkout failed"; return 1; }
  git merge --ff-only --quiet "origin/$BRANCH" || { log "Fast-forward failed; no restart performed"; return 1; }

  if [ -x "$APP_DIR/.venv/bin/pip" ] && [ -f requirements.txt ]; then
    "$APP_DIR/.venv/bin/pip" install -q -r requirements.txt || { log "Dependency install failed"; return 1; }
  fi

  if ! sv restart "$SERVICE" >/dev/null 2>&1; then
    log "Service restart failed: $SERVICE"
    return 1
  fi

  for _ in 1 2 3 4 5 6; do
    sleep 2
    if health_ok; then
      printf '%s\n' "$remote_sha" > "$STATE_DIR/last_successful_sha"
      log "Deployed ${remote_sha:0:12}; health check passed."
      return 0
    fi
  done

  log "Deploy completed but health check failed on port $PORT"
  return 1
}

log "Watching origin/$BRANCH every ${INTERVAL}s"
while true; do
  deploy_once || true
  sleep "$INTERVAL"
done
