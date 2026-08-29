#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
STATE_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}"
CONFIG_DIR="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}"
ENV_FILE="$CONFIG_DIR/env"
LOG_DIR="$STATE_DIR/logs"
SERVICE_ROOT="${PREFIX:-/data/data/com.termux/files/usr}/var/service"

cd "$APP_DIR"
mkdir -p "$STATE_DIR" "$LOG_DIR" "$CONFIG_DIR"

branch="$(git branch --show-current)"
if [ "$branch" != "deploy/termux" ]; then
  echo "This migration must be run from deploy/termux; current branch is: $branch" >&2
  exit 2
fi

# Disable only the temporary lightweight-main AYCF runit services. Do not touch Sunscape.
if command -v sv >/dev/null 2>&1; then
  for svc in aycf aycf-admin aycf-deploy; do
    if [ -d "$SERVICE_ROOT/$svc" ] || [ -L "$SERVICE_ROOT/$svc" ]; then
      sv down "$svc" >/dev/null 2>&1 || true
      touch "$SERVICE_ROOT/$svc/down" 2>/dev/null || true
    fi
  done
fi

# Stop only AYCF processes from either implementation before starting the canonical full console.
pkill -f 'termux/run-web.py' >/dev/null 2>&1 || true
pkill -f 'watch_app.py termux/runtime.py web' >/dev/null 2>&1 || true
pkill -f 'termux/admin_hub.py' >/dev/null 2>&1 || true
sleep 1

# Preserve existing secrets; only restore the deployment ref and required admin defaults.
if [ ! -f "$ENV_FILE" ]; then
  echo "Missing $ENV_FILE. The full deployment needs the existing AYCF environment from the phone." >&2
  exit 3
fi

if grep -q '^export AYCF_DEPLOY_REF=' "$ENV_FILE"; then
  sed -i "s|^export AYCF_DEPLOY_REF=.*|export AYCF_DEPLOY_REF='deploy/termux'|" "$ENV_FILE"
else
  printf "\nexport AYCF_DEPLOY_REF='deploy/termux'\n" >> "$ENV_FILE"
fi
grep -q '^export AYCF_ADMIN_BIND_HOST=' "$ENV_FILE" || printf "export AYCF_ADMIN_BIND_HOST='127.0.0.1'\n" >> "$ENV_FILE"
grep -q '^export AYCF_ADMIN_PORT=' "$ENV_FILE" || printf "export AYCF_ADMIN_PORT='8079'\n" >> "$ENV_FILE"
chmod 600 "$ENV_FILE"

chmod 700 termux/*.sh

# The full scanner intentionally uses Termux's native pandas/cryptography packages.
if ! python - <<'PY'
import pandas, cryptography, flask, requests
print('Termux runtime dependencies OK')
PY
then
  echo "Missing native Termux dependencies. Run: pkg install -y tur-repo python-pandas python-cryptography android-tools" >&2
  exit 4
fi
python -m pip install -r requirements-termux.txt --disable-pip-version-check -q

# Restore the original scheduled deployment/morning automation.
./termux/schedule-morning.sh >/dev/null 2>&1 || true
./termux/schedule-deploy.sh >/dev/null 2>&1 || true

# Restore Termux:Boot launchers without touching Sunscape.
mkdir -p "$HOME/.termux/boot"
cat > "$HOME/.termux/boot/05-aycf-admin" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
exec '$APP_DIR/termux/run-admin.sh'
EOF
cat > "$HOME/.termux/boot/10-aycf-web" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
exec '$APP_DIR/termux/run-web.sh'
EOF
chmod 700 "$HOME/.termux/boot/05-aycf-admin" "$HOME/.termux/boot/10-aycf-web"

# Reconcile the app registry so the admin hub keeps AYCF and Sunscape together.
if [ ! -f "$CONFIG_DIR/apps.json" ] && [ -f termux/apps.json.example ]; then
  cp termux/apps.json.example "$CONFIG_DIR/apps.json"
  chmod 600 "$CONFIG_DIR/apps.json"
fi

nohup ./termux/run-web.sh >> "$LOG_DIR/web.log" 2>&1 < /dev/null &
nohup ./termux/run-admin.sh >> "$LOG_DIR/admin.log" 2>&1 < /dev/null &
sleep 4

python - <<'PY'
from urllib.request import urlopen

def check(name, url):
    try:
        with urlopen(url, timeout=8) as r:
            ok = 200 <= r.status < 300
            print(f"{name}: HTTP {r.status}")
            return ok
    except Exception as exc:
        print(f"{name}: FAILED ({exc})")
        return False

ok = check('AYCF full console', 'http://127.0.0.1:8080/health')
ok = check('AYCF admin hub', 'http://127.0.0.1:8079/health') and ok
ok = check('Sunscape', 'http://127.0.0.1:8081/health') and ok
raise SystemExit(0 if ok else 1)
PY

echo "Full deploy/termux application restored successfully."
echo "Planner:  http://127.0.0.1:8080"
echo "Admin:    http://127.0.0.1:8079"
echo "Sunscape: http://127.0.0.1:8081"
