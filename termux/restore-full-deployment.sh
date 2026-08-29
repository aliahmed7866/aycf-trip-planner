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

# ---- Preflight: do not stop the currently working web/admin services until all
# required full-console runtime pieces have validated successfully. ----
if [ ! -f "$ENV_FILE" ]; then
  echo "Missing $ENV_FILE. The full deployment needs the existing AYCF environment from the phone." >&2
  exit 3
fi

required_files=(
  requirements-termux.txt
  watch_app.py
  termux/run-web.sh
  termux/run-admin.sh
  termux/auto-deploy.sh
  termux/schedule-deploy.sh
  termux/schedule-morning.sh
  termux/apps.json.example
)
for path in "${required_files[@]}"; do
  [ -f "$path" ] || { echo "Missing required full-console file: $path" >&2; exit 3; }
done

for command_name in python git nohup pkill termux-job-scheduler termux-wake-lock; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "Missing required Termux command: $command_name" >&2
    echo "Ensure Termux:API is installed and run: pkg install -y termux-api" >&2
    exit 4
  }
done

chmod 700 termux/*.sh

# Native Android builds must come from Termux packages rather than pip.
if ! python - <<'PY'
import pandas
import cryptography
print('Native Termux pandas/cryptography OK')
PY
then
  echo "Missing native Termux dependencies." >&2
  echo "Run: pkg install -y tur-repo python-pandas python-cryptography android-tools" >&2
  exit 4
fi

# Install/verify only the pure-Python/runtime packages used by the full console.
# This happens while the current service is still alive, so a package failure
# cannot unnecessarily take AYCF offline.
python -m pip install -r requirements-termux.txt --disable-pip-version-check -q
python - <<'PY'
import cryptography
import dateutil
import flask
import pandas
import requests
import websocket
print('Full Termux Python runtime OK')
PY

python -m py_compile \
  app.py watch_app.py scanner.py itinerary_search.py cache_db.py \
  termux/runtime.py termux/admin_hub.py termux/multi_search.py termux/health_ui.py

echo "Preflight complete; beginning controlled handoff to deploy/termux."

# First disable only the temporary main-branch deploy watcher so it cannot race
# with the branch/environment migration. Leave the currently serving AYCF and
# admin processes alive until configuration/schedules are ready.
if command -v sv >/dev/null 2>&1 && { [ -d "$SERVICE_ROOT/aycf-deploy" ] || [ -L "$SERVICE_ROOT/aycf-deploy" ]; }; then
  sv down aycf-deploy >/dev/null 2>&1 || true
  touch "$SERVICE_ROOT/aycf-deploy/down" 2>/dev/null || true
fi

# Preserve existing secrets; restore only deployment/admin defaults required by
# the canonical full deployment.
if grep -q '^export AYCF_DEPLOY_REF=' "$ENV_FILE"; then
  sed -i "s|^export AYCF_DEPLOY_REF=.*|export AYCF_DEPLOY_REF='deploy/termux'|" "$ENV_FILE"
else
  printf "\nexport AYCF_DEPLOY_REF='deploy/termux'\n" >> "$ENV_FILE"
fi
grep -q '^export AYCF_ADMIN_BIND_HOST=' "$ENV_FILE" || printf "export AYCF_ADMIN_BIND_HOST='127.0.0.1'\n" >> "$ENV_FILE"
grep -q '^export AYCF_ADMIN_PORT=' "$ENV_FILE" || printf "export AYCF_ADMIN_PORT='8079'\n" >> "$ENV_FILE"
chmod 600 "$ENV_FILE"

# Restore the original scheduled deployment/morning automation. These are not
# optional: a failure here aborts before the web/admin handoff.
./termux/schedule-morning.sh >/dev/null
./termux/schedule-deploy.sh >/dev/null

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

# The lightweight main installer may have persisted an AYCF registry entry that
# points at the temporary runit service. Replace only AYCF/Sunscape with the
# canonical full-deployment definitions and preserve any unrelated custom apps.
python - "$CONFIG_DIR/apps.json" "termux/apps.json.example" <<'PY'
import json
import sys
from pathlib import Path

registry = Path(sys.argv[1])
example = Path(sys.argv[2])
canonical_payload = json.loads(example.read_text(encoding='utf-8'))
canonical = {
    item['id']: item
    for item in canonical_payload.get('apps', [])
    if isinstance(item, dict) and item.get('id') in {'aycf', 'sunscape'}
}

try:
    current_payload = json.loads(registry.read_text(encoding='utf-8')) if registry.exists() else {'apps': []}
except Exception:
    current_payload = {'apps': []}

current = current_payload.get('apps') if isinstance(current_payload, dict) else []
if not isinstance(current, list):
    current = []

out = []
seen = set()
for item in current:
    if not isinstance(item, dict):
        continue
    app_id = item.get('id')
    if app_id in canonical:
        if app_id not in seen:
            out.append(canonical[app_id])
            seen.add(app_id)
    else:
        out.append(item)

for app_id in ('aycf', 'sunscape'):
    if app_id in canonical and app_id not in seen:
        out.append(canonical[app_id])

registry.write_text(json.dumps({'apps': out}, indent=2) + '\n', encoding='utf-8')
PY
chmod 600 "$CONFIG_DIR/apps.json"

# ---- Handoff: now stop only the temporary AYCF services/processes. ----
if command -v sv >/dev/null 2>&1; then
  for svc in aycf aycf-admin; do
    if [ -d "$SERVICE_ROOT/$svc" ] || [ -L "$SERVICE_ROOT/$svc" ]; then
      sv down "$svc" >/dev/null 2>&1 || true
      touch "$SERVICE_ROOT/$svc/down" 2>/dev/null || true
    fi
  done
fi

pkill -f 'termux/run-web.py' >/dev/null 2>&1 || true
pkill -f 'watch_app.py termux/runtime.py web' >/dev/null 2>&1 || true
pkill -f 'termux/admin_hub.py' >/dev/null 2>&1 || true
sleep 1

# Refuse to start if another unexpected process still owns either AYCF port.
python - <<'PY'
import socket

for port in (8080, 8079):
    sock = socket.socket()
    try:
        sock.bind(('127.0.0.1', port))
    except OSError as exc:
        raise SystemExit(f'Port {port} is still in use after AYCF handoff: {exc}')
    finally:
        sock.close()
print('AYCF ports are free')
PY

nohup ./termux/run-web.sh >> "$LOG_DIR/web.log" 2>&1 < /dev/null &
nohup ./termux/run-admin.sh >> "$LOG_DIR/admin.log" 2>&1 < /dev/null &

# The integrated watch_app fails fast itself if Planner/Flights/Watches/Admin or
# multi-search endpoints are missing. Give startup enough time for cache/runtime
# initialization, then require AYCF + Admin + Sunscape to be healthy together.
if ! python - <<'PY'
import time
from urllib.request import urlopen

targets = [
    ('AYCF full console', 'http://127.0.0.1:8080/health'),
    ('AYCF admin hub', 'http://127.0.0.1:8079/health'),
    ('Sunscape', 'http://127.0.0.1:8081/health'),
]

def healthy(url):
    try:
        with urlopen(url, timeout=4) as response:
            return 200 <= response.status < 300, response.status
    except Exception as exc:
        return False, type(exc).__name__

last = {}
for _ in range(15):
    all_ok = True
    for name, url in targets:
        ok, detail = healthy(url)
        last[name] = detail
        all_ok = all_ok and ok
    if all_ok:
        for name, _ in targets:
            print(f'{name}: HTTP {last[name]}')
        raise SystemExit(0)
    time.sleep(2)

for name, _ in targets:
    print(f'{name}: FAILED ({last.get(name, "no response")})')
raise SystemExit(1)
PY
then
  echo "Full deployment failed health validation." >&2
  echo "=== AYCF web log ===" >&2
  tail -50 "$LOG_DIR/web.log" >&2 2>/dev/null || true
  echo "=== AYCF admin log ===" >&2
  tail -50 "$LOG_DIR/admin.log" >&2 2>/dev/null || true
  exit 5
fi

echo "Full deploy/termux application restored successfully."
echo "Planner:  http://127.0.0.1:8080"
echo "Admin:    http://127.0.0.1:8079"
echo "Sunscape: http://127.0.0.1:8081"
