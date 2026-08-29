#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
PORT="${AYCF_PORT:-8080}"
ADMIN_PORT="${AYCF_ADMIN_PORT:-8079}"
CONFIG_DIR="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}"
ENV_FILE="${AYCF_ENV_FILE:-$CONFIG_DIR/env}"
VENV_DIR="$APP_DIR/.venv"
AYCF_SERVICE_DIR="$PREFIX/var/service/aycf"
ADMIN_SERVICE_DIR="$PREFIX/var/service/aycf-admin"
DEPLOY_SERVICE_DIR="$PREFIX/var/service/aycf-deploy"

cd "$APP_DIR"

if ! command -v sv >/dev/null 2>&1; then
  pkg install -y termux-services
fi

VENV_CREATED=0
if [ ! -d "$VENV_DIR" ]; then
  python -m venv "$VENV_DIR"
  VENV_CREATED=1
fi

# Dependencies are only installed when requirements actually change. This keeps
# normal code-only auto-deploys fast and avoids unnecessary network/package work.
REQ_HASH="$($VENV_DIR/bin/python - <<'PY'
from pathlib import Path
import hashlib
print(hashlib.sha256(Path('requirements.txt').read_bytes()).hexdigest())
PY
)"
REQ_STAMP="$VENV_DIR/.aycf-requirements-sha256"
INSTALLED_HASH="$(cat "$REQ_STAMP" 2>/dev/null || true)"
if [ "$VENV_CREATED" = "1" ] || [ "$REQ_HASH" != "$INSTALLED_HASH" ]; then
  "$VENV_DIR/bin/python" -m pip install --upgrade pip
  "$VENV_DIR/bin/pip" install -r requirements.txt
  printf '%s\n' "$REQ_HASH" > "$REQ_STAMP"
else
  echo "[AYCF] Dependencies unchanged; skipping pip install."
fi

mkdir -p "$CONFIG_DIR"
chmod 700 "$CONFIG_DIR"

if [ ! -f "$ENV_FILE" ]; then
  FLASK_SECRET="$($VENV_DIR/bin/python - <<'PY'
import secrets
print(secrets.token_urlsafe(48))
PY
)"
  APP_PASSWORD="$($VENV_DIR/bin/python - <<'PY'
import secrets
print(secrets.token_urlsafe(18))
PY
)"
  cat > "$ENV_FILE" <<EOF
export FLASK_SECRET_KEY='$FLASK_SECRET'
export AYCF_APP_PASSWORD='$APP_PASSWORD'
export AYCF_BIND_HOST='127.0.0.1'
export PORT='$PORT'
export AYCF_ADMIN_BIND_HOST='127.0.0.1'
export AYCF_ADMIN_PORT='$ADMIN_PORT'
EOF
  chmod 600 "$ENV_FILE"
  echo "[AYCF] Created private config: $ENV_FILE"
fi

grep -q '^export AYCF_ADMIN_BIND_HOST=' "$ENV_FILE" || printf "\nexport AYCF_ADMIN_BIND_HOST='127.0.0.1'\n" >> "$ENV_FILE"
grep -q '^export AYCF_ADMIN_PORT=' "$ENV_FILE" || printf "export AYCF_ADMIN_PORT='%s'\n" "$ADMIN_PORT" >> "$ENV_FILE"

if [ -f "$APP_DIR/termux/apps.json.example" ]; then
  cp "$APP_DIR/termux/apps.json.example" "$CONFIG_DIR/apps.json"
  chmod 600 "$CONFIG_DIR/apps.json"
fi

mkdir -p "$AYCF_SERVICE_DIR" "$ADMIN_SERVICE_DIR"

cat > "$AYCF_SERVICE_DIR/run" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
exec 2>&1
cd "$APP_DIR"
ENV_FILE="$ENV_FILE"
[ -f "\$ENV_FILE" ] && . "\$ENV_FILE"
export PORT="\${PORT:-$PORT}"
exec "$VENV_DIR/bin/python" termux/run-web.py
EOF
chmod +x "$AYCF_SERVICE_DIR/run"

cat > "$ADMIN_SERVICE_DIR/run" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
exec 2>&1
cd "$APP_DIR"
ENV_FILE="$ENV_FILE"
[ -f "\$ENV_FILE" ] && . "\$ENV_FILE"
export AYCF_ADMIN_PORT="\${AYCF_ADMIN_PORT:-$ADMIN_PORT}"
exec "$VENV_DIR/bin/python" termux/admin_hub.py
EOF
chmod +x "$ADMIN_SERVICE_DIR/run"

sv-enable aycf >/dev/null 2>&1 || true
sv-enable aycf-admin >/dev/null 2>&1 || true

for service in aycf aycf-admin; do
  sv restart "$service" >/dev/null 2>&1 || {
    sv down "$service" >/dev/null 2>&1 || true
    sleep 1
    sv up "$service" >/dev/null 2>&1 || true
  }
done

for _ in 1 2 3 4 5 6 7 8 9 10; do
  if curl -fsS --max-time 5 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 && \
     curl -fsS --max-time 5 "http://127.0.0.1:$ADMIN_PORT/health" >/dev/null 2>&1; then
    echo "[AYCF] Web:   http://127.0.0.1:$PORT"
    echo "[AYCF] Admin: http://127.0.0.1:$ADMIN_PORT"
    # If this installer was invoked by an older watcher after pulling newer
    # deployment code, restart that watcher after this deployment has had time
    # to record success. Future merges therefore self-upgrade the deployer too.
    if [ -d "$DEPLOY_SERVICE_DIR" ]; then
      (sleep 8; sv restart aycf-deploy >/dev/null 2>&1 || true) >/dev/null 2>&1 &
    fi
    exit 0
  fi
  sleep 2
done

echo "[AYCF] Stack installed/restarted, but one or more health checks failed."
sv status aycf || true
sv status aycf-admin || true
exit 1
