#!/data/data/com.termux/files/usr/bin/bash
set -u

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
BRANCH="${AYCF_BRANCH:-main}"
PORT="${AYCF_PORT:-8080}"
ADMIN_PORT="${AYCF_ADMIN_PORT:-8079}"
INTERVAL="${AYCF_DEPLOY_INTERVAL:-60}"
STATE_DIR="${AYCF_DEPLOY_STATE_DIR:-$HOME/.local/state/aycf}"
LOG_PREFIX="[AYCF deploy]"
STATUS_FILE="$STATE_DIR/deploy-status.txt"
SUCCESS_FILE="$STATE_DIR/last_successful_sha"

mkdir -p "$STATE_DIR"
log() { printf '%s %s %s\n' "$LOG_PREFIX" "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
status() { printf '%s\n' "$*" > "$STATUS_FILE"; }

health_ok() {
  curl -fsS --max-time 10 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && \
  curl -fsS --max-time 10 "http://127.0.0.1:$ADMIN_PORT/health" >/dev/null 2>&1
}

deploy_once() {
  cd "$APP_DIR" || { status "error: app directory missing"; log "App directory missing: $APP_DIR"; return 1; }

  if [ -n "$(git status --porcelain --untracked-files=no 2>/dev/null)" ]; then
    status "blocked: tracked working tree changes"
    log "Tracked working tree changes detected; skipping automatic deploy."
    return 1
  fi

  remote_sha="$(git ls-remote origin "refs/heads/$BRANCH" 2>/dev/null | awk 'NR==1{print $1}')"
  [ -n "$remote_sha" ] || { status "error: could not read origin/$BRANCH"; log "Could not read origin/$BRANCH"; return 1; }

  local_sha="$(git rev-parse HEAD 2>/dev/null || true)"
  deployed_sha="$(cat "$SUCCESS_FILE" 2>/dev/null || true)"

  # A manual git pull must not trick the watcher into thinking the running
  # services were restarted. Only last_successful_sha proves deployment.
  if [ "$remote_sha" = "$local_sha" ] && [ "$remote_sha" = "$deployed_sha" ] && health_ok; then
    status "up-to-date: ${remote_sha:0:12}"
    return 0
  fi

  status "deploying: ${remote_sha:0:12}"
  if [ "$remote_sha" != "$local_sha" ]; then
    log "New origin/$BRANCH commit detected: ${remote_sha:0:12}"
    git fetch --quiet origin "$BRANCH" || { status "error: fetch failed"; log "Fetch failed"; return 1; }
    git checkout --quiet "$BRANCH" || { status "error: checkout failed"; log "Checkout failed"; return 1; }
    git merge --ff-only --quiet "origin/$BRANCH" || { status "error: fast-forward failed"; log "Fast-forward failed; no restart performed"; return 1; }
    local_sha="$(git rev-parse HEAD 2>/dev/null || true)"
  else
    log "Code is already at ${remote_sha:0:12}, but services have not recorded this deployment; restarting stack."
  fi

  if [ "$local_sha" != "$remote_sha" ]; then
    status "error: local SHA does not match origin"
    log "Local SHA ${local_sha:0:12} does not match remote ${remote_sha:0:12}."
    return 1
  fi

  if ! bash "$APP_DIR/termux/install-service.sh"; then
    status "error: stack install/restart/health validation failed"
    log "Stack install/restart/health validation failed"
    return 1
  fi

  if health_ok; then
    printf '%s\n' "$remote_sha" > "$SUCCESS_FILE"
    status "deployed: ${remote_sha:0:12}"
    log "Deployed ${remote_sha:0:12}; AYCF + admin health checks passed."
    return 0
  fi

  status "error: post-install health failed"
  log "Installer returned successfully but stack health check failed (web $PORT / admin $ADMIN_PORT)"
  return 1
}

log "Watching origin/$BRANCH every ${INTERVAL}s"
while true; do
  deploy_once || true
  sleep "$INTERVAL"
done
