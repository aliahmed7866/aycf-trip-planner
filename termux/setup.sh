#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

REPO_URL="${AYCF_REPO_URL:-https://github.com/aliahmed7866/aycf-trip-planner.git}"
BRANCH="${AYCF_BRANCH:-feature/live-aycf-scanner}"
APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
STATE_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}"
CONFIG_DIR="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}"

pkg update -y
pkg install -y git python python-cryptography poppler termux-api tur-repo
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
python -m pip install flask==3.0.3 python-dateutil==2.9.0.post0 requests==2.32.3

mkdir -p "$STATE_DIR/cache" "$CONFIG_DIR" "$HOME/.termux/boot"
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
export WIZZ_SESSION_FILE='$STATE_DIR/wizz_session.enc'
export AYCF_CACHE_DIR='$STATE_DIR/cache'
export AYCF_DB_PATH='$STATE_DIR/aycf.sqlite3'
export AYCF_PDF_URL='https://multipass.wizzair.com/aycf-availability.pdf'
export AYCF_ALLOW_LIVE_FALLBACK='true'
export AYCF_MIN_REQUEST_DELAY='1.0'
EOF
  chmod 600 "$ENV_FILE"
fi

cat > "$HOME/.termux/boot/10-aycf-web" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
exec '$APP_DIR/termux/run-web.sh'
EOF
chmod 700 "$HOME/.termux/boot/10-aycf-web"

cat <<EOF

AYCF Termux setup complete.
App directory: $APP_DIR
State directory: $STATE_DIR

Your local scanner password is stored in:
  $ENV_FILE

Show it with:
  grep AYCF_APP_PASSWORD '$ENV_FILE'

Next:
  1. Install Termux:API and Termux:Boot from the SAME source/signature as Termux, then open each once.
  2. Android Settings > Apps > Termux: disable battery optimisation / allow unrestricted background battery where available.
  3. Run: $APP_DIR/termux/schedule-morning.sh
  4. Import your Wizz session (see TERMUX.md).
  5. Run: $APP_DIR/termux/run-web.sh
  6. Open http://127.0.0.1:8080 in Chrome.
EOF
