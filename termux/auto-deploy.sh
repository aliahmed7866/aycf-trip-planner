#!/data/data/com.termux/files/usr/bin/bash
set -u

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
BRANCH="${AYCF_BRANCH:-main}"
PORT="${AYCF_PORT:-8080}"
ADMIN_PORT="${AYCF_ADMIN_PORT:-8079}"
INTERVAL="${AYCF_DEPLOY_INTERVAL:-60}"
STATE_DIR="${AYCF_DEPLOY_STATE_DIR:-$HOME/.local/state/aycf}"
LOG_PREFIX="[AYCF deploy]"

mkdir -p "$STATE_DIR"
log() { printf '%s %s\n' "$LOG_PREFIX" "$*"; }

health_ok() {
  curl -fsS --max-time 10 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && \
  curl -fsS --max-time 10 "http://127.0.0.1:$ADMIN_PORT/health" >/dev/null 2>&1
}

deploy_once() {
  cd "$APP_DIR" || { log "App directory missing: $APP_DIR"; return 1; }

  # Local runtime/untracked files are allowed; tracked edits are not.
  if [ -n "$(git status --porcelain --untracked-files=no 2>/dev/null)" ]; then
    log "Tracked working tree changes detected; skipping automatic deploy."
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

  # Rebuild service definitions on every deploy so environment/service changes deploy too.
  if ! bash "$APP_DIR/termux/install-service.sh" >/dev/null 2>&1; then
    log "Stack service installation/reload failed"
    return 1
  fi

  sv restart aycf >/dev/null 2>&1 || { log "Service restart failed: aycf"; return 1; }
  sv restart aycf-admin >/dev/null 2>&1 || { log "Service restart failed: aycf-admin"; return 1; }

  for _ in 1 2 3 4 5 6 7 8; do
    sleep 2
    if health_ok; then
      printf '%s\n' "$remote_sha" > "$STATE_DIR/last_successful_sha"
      log "Deployed ${remote_sha:0:12}; AYCF + admin health checks passed."
      return 0
    fi
  done

  log "Deploy completed but stack health check failed (web $PORT / admin $ADMIN_PORT)"
  return 1
}

log "Watching origin/$BRANCH every ${INTERVAL}s"
while true; do
  deploy_once || true
  sleep "$INTERVAL"
done
