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

cd "$APP_DIR"

if ! command -v sv >/dev/null 2>&1; then
  pkg install -y termux-services
fi

if [ ! -d "$VENV_DIR" ]; then
  python -m venv "$VENV_DIR"
fi

"$VENV_DIR/bin/python" -m pip install --upgrade pip
"$VENV_DIR/bin/pip" install -r requirements.txt

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

if [ ! -f "$CONFIG_DIR/apps.json" ] && [ -f "$APP_DIR/termux/apps.json.example" ]; then
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
exec "$VENV_DIR/bin/python" app.py
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

# Always activate freshly written service definitions. `sv up` alone leaves an
# already-running process on its previous environment/command indefinitely.
for service in aycf aycf-admin; do
  sv restart "$service" >/dev/null 2>&1 || {
    # A just-created runit service can briefly be undiscovered. Force it down/up
    # as a bounded fallback rather than leaving a stale process running.
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
    exit 0
  fi
  sleep 2
done

echo "[AYCF] Stack installed/restarted, but one or more health checks failed."
sv status aycf || true
sv status aycf-admin || true
exit 1
