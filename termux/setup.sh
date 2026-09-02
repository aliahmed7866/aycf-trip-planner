#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

REPO_URL="${AYCF_REPO_URL:-https://github.com/aliahmed7866/aycf-trip-planner.git}"
BRANCH="${AYCF_BRANCH:-deploy/termux}"
APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
STATE_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}"
CONFIG_DIR="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}"

pkg update -y
pkg install -y git python python-cryptography poppler termux-api tur-repo android-tools fakeroot
pkg install -y python-pandas

if [ ! -d "$APP_DIR/.git" ]; then
  git clone --branch "$BRANCH" "$REPO_URL" "$APP_DIR"
else
  git -C "$APP_DIR" fetch origin "$BRANCH"
  git -C "$APP_DIR" checkout "$BRANCH"
  git -C "$APP_DIR" pull --ff-only origin "$BRANCH"
fi
chmod 700 "$APP_DIR"/termux/*.sh

python -m pip install --upgrade pip wheel
python -m pip install flask==3.0.3 python-dateutil==2.9.0.post0 requests==2.32.3 websocket-client==1.8.0

mkdir -p "$STATE_DIR/cache" "$STATE_DIR/logs" "$CONFIG_DIR" "$HOME/.termux/boot"
chmod 700 "$STATE_DIR" "$CONFIG_DIR"

ENV_FILE="$CONFIG_DIR/env"
if [ ! -f "$ENV_FILE" ]; then
  FLASK_SECRET="$(python - <<'PY'
import secrets
print(secrets.token_urlsafe(48))
PY
)"
  APP_PASSWORD="$(python - <<'PY'
import secrets
print(secrets.token_urlsafe(18))
PY
)"
  ADMIN_TOKEN="$(python - <<'PY'
import secrets
print(secrets.token_urlsafe(48))
PY
)"
  FERNET_KEY="$(python - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
)"
  cat > "$ENV_FILE" <<EOF
export FLASK_SECRET_KEY='$FLASK_SECRET'
export AYCF_APP_PASSWORD='$APP_PASSWORD'
export AYCF_ADMIN_TOKEN='$ADMIN_TOKEN'
export AYCF_SESSION_ENCRYPTION_KEY='$FERNET_KEY'
export SESSION_COOKIE_SECURE='false'
export AYCF_BIND_HOST='127.0.0.1'
export PORT='8080'
export AYCF_ADMIN_BIND_HOST='127.0.0.1'
export AYCF_ADMIN_PORT='8079'
export WIZZ_SESSION_FILE='$STATE_DIR/wizz_session.enc'
export AYCF_CACHE_DIR='$STATE_DIR/cache'
export AYCF_TERMUX_DB_PATH='$STATE_DIR/aycf.sqlite3'
export AYCF_PDF_URL='https://multipass.wizzair.com/aycf-availability.pdf'
export AYCF_TERMUX_ALLOW_LIVE_FALLBACK='false'
export AYCF_MIN_REQUEST_DELAY='1.0'
export AYCF_DEPLOY_REF='deploy/termux'
EOF
  chmod 600 "$ENV_FILE"
else
  grep -q '^export AYCF_ADMIN_BIND_HOST=' "$ENV_FILE" || printf "\nexport AYCF_ADMIN_BIND_HOST='127.0.0.1'\n" >> "$ENV_FILE"
  grep -q '^export AYCF_ADMIN_PORT=' "$ENV_FILE" || printf "export AYCF_ADMIN_PORT='8079'\n" >> "$ENV_FILE"
fi

REGISTRY_FILE="$CONFIG_DIR/apps.json"
if [ ! -f "$REGISTRY_FILE" ]; then
  cp "$APP_DIR/termux/apps.json.example" "$REGISTRY_FILE"
  chmod 600 "$REGISTRY_FILE"
fi

cat > "$HOME/.termux/boot/05-aycf-admin" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
exec '$APP_DIR/termux/run-admin.sh'
EOF
chmod 700 "$HOME/.termux/boot/05-aycf-admin"

cat > "$HOME/.termux/boot/10-aycf-web" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
exec '$APP_DIR/termux/run-web.sh'
EOF
chmod 700 "$HOME/.termux/boot/10-aycf-web"

# Keep both persistent jobs installed. They are idempotent and can be rerun.
"$APP_DIR/termux/schedule-morning.sh" >/dev/null 2>&1 || true
"$APP_DIR/termux/schedule-deploy.sh" >/dev/null 2>&1 || true

cat <<EOF

AYCF Termux setup complete.
App directory: $APP_DIR
State directory: $STATE_DIR

Your fallback password is stored in:
  $ENV_FILE

Direct 127.0.0.1 access is passwordless. The password is enforced if you bind beyond localhost.

Admin hub:
  Registry: $REGISTRY_FILE
  Start: $APP_DIR/termux/run-admin.sh
  Open: http://127.0.0.1:8079
  It uses the same AYCF app password and keeps management bound to localhost.

Automatic deployment:
  Pull requests are validated before they merge into deploy/termux.
  This phone checks deploy/termux about every 15 minutes and only fast-forwards a clean checkout.
  Deployment status: $STATE_DIR/deploy-status.txt
  Deployment/web logs: $STATE_DIR/logs/

Next:
  1. Install/open Termux:API and Termux:Boot from the same source/signature as Termux.
  2. Disable battery optimisation for Termux, Termux:API and Termux:Boot where Android allows it.
  3. Connect Wizz once: $APP_DIR/termux/connect-wizz-chrome.sh
  4. Run: $APP_DIR/termux/run-admin.sh
  5. Open http://127.0.0.1:8079 to manage apps, or http://127.0.0.1:8080 for AYCF directly.
EOF
