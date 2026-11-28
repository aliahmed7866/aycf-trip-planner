#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
STATE_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}"
export AYCF_STATE_DIR="$STATE_DIR"
LOG_DIR="$STATE_DIR/logs"
SERVICE_ROOT="${PREFIX:-/data/data/com.termux/files/usr}/var/service"

# Validate the ports that the launchers will read from the saved phone settings.
settings="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}/env"
[ ! -f "$settings" ] || source "$settings"
cd "$APP_DIR"
mkdir -p "$LOG_DIR"

branch="$(git branch --show-current)"
if [ "$branch" != "deploy/termux" ]; then
  echo "Must run from deploy/termux; current branch: $branch" >&2
  exit 2
fi

port_free() {
  python - "$1" <<'PY'
import socket, sys
port = int(sys.argv[1])
with socket.socket() as sock:
    sock.settimeout(0.5)
    # A refused connection means there is no active listener. Unlike bind(),
    # this is not confused by a recently closed socket in TIME_WAIT.
    raise SystemExit(1 if sock.connect_ex(("127.0.0.1", port)) == 0 else 0)
PY
}

# Keep the temporary main-branch runit services down permanently.
if command -v sv >/dev/null 2>&1; then
  for svc in aycf-deploy aycf aycf-admin; do
    if [ -d "$SERVICE_ROOT/$svc" ] || [ -L "$SERVICE_ROOT/$svc" ]; then
      sv down "$svc" >/dev/null 2>&1 || true
      touch "$SERVICE_ROOT/$svc/down" 2>/dev/null || true
    fi
  done
fi

python "$APP_DIR/termux/checkout_processes.py" "$APP_DIR" web admin

for _ in $(seq 1 10); do
  if port_free "${PORT:-8080}" && port_free "${AYCF_ADMIN_PORT:-8079}"; then
    break
  fi
  sleep 1
done

if ! port_free "${PORT:-8080}" || ! port_free "${AYCF_ADMIN_PORT:-8079}"; then
  python "$APP_DIR/termux/checkout_processes.py" "$APP_DIR" web admin
  sleep 2
fi

for port in "${PORT:-8080}" "${AYCF_ADMIN_PORT:-8079}"; do
  if ! port_free "$port"; then
    echo "Port $port is still occupied after AYCF-only process cleanup." >&2
    exit 3
  fi
done

echo "AYCF ports are free; starting full deploy/termux console."
# Invoke through bash so Git checkout/reset file-mode changes cannot break startup.
nohup bash ./termux/run-web.sh >> "$LOG_DIR/web.log" 2>&1 < /dev/null &
nohup bash ./termux/run-admin.sh >> "$LOG_DIR/admin.log" 2>&1 < /dev/null &

if ! python - <<'PY'
import os
from pathlib import Path
import time
from urllib.request import urlopen

targets = [
    ('AYCF admin hub', 'http://127.0.0.1:'+os.environ.get('AYCF_ADMIN_PORT', '8079')+'/health'),
]
if not (Path(os.environ['AYCF_STATE_DIR']) / 'planner.down').exists():
    targets.append(('AYCF full console', 'http://127.0.0.1:'+os.environ.get('PORT', '8080')+'/health'))

def probe(url):
    try:
        with urlopen(url, timeout=4) as r:
            return 200 <= r.status < 300, r.status
    except Exception as exc:
        return False, type(exc).__name__

last = {}
for _ in range(20):
    all_ok = True
    for name, url in targets:
        ok, detail = probe(url)
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
  echo "Full-console startup failed." >&2
  echo "=== web.log ===" >&2
  tail -60 "$LOG_DIR/web.log" >&2 2>/dev/null || true
  echo "=== admin.log ===" >&2
  tail -60 "$LOG_DIR/admin.log" >&2 2>/dev/null || true
  exit 4
fi

echo "Full deploy/termux handoff completed successfully."
