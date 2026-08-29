#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
STATE_DIR="${AYCF_STATE_DIR:-$HOME/.local/share/aycf}"
LOG_DIR="$STATE_DIR/logs"
SERVICE_ROOT="${PREFIX:-/data/data/com.termux/files/usr}/var/service"

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
s = socket.socket()
try:
    s.bind(('127.0.0.1', port))
except OSError:
    raise SystemExit(1)
finally:
    s.close()
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

# Drain only AYCF web/admin processes. First TERM, then KILL only if they remain.
patterns=(
  'termux/run-web.py'
  'watch_app.py termux/runtime.py web'
  'termux/admin_hub.py'
  'termux/run-admin.sh'
)
for pattern in "${patterns[@]}"; do
  pkill -TERM -f "$pattern" >/dev/null 2>&1 || true
done

for _ in $(seq 1 10); do
  if port_free 8080 && port_free 8079; then
    break
  fi
  sleep 1
done

if ! port_free 8080 || ! port_free 8079; then
  for pattern in "${patterns[@]}"; do
    pkill -KILL -f "$pattern" >/dev/null 2>&1 || true
  done
  sleep 2
fi

for port in 8080 8079; do
  if ! port_free "$port"; then
    echo "Port $port is still occupied after AYCF-only process cleanup." >&2
    echo "Matching AYCF processes:" >&2
    pgrep -af 'aycf|watch_app|admin_hub|run-web|run-admin' >&2 || true
    exit 3
  fi
done

echo "AYCF ports are free; starting full deploy/termux console."
nohup ./termux/run-web.sh >> "$LOG_DIR/web.log" 2>&1 < /dev/null &
nohup ./termux/run-admin.sh >> "$LOG_DIR/admin.log" 2>&1 < /dev/null &

if ! python - <<'PY'
import time
from urllib.request import urlopen

targets = [
    ('AYCF full console', 'http://127.0.0.1:8080/health'),
    ('AYCF admin hub', 'http://127.0.0.1:8079/health'),
    ('Sunscape', 'http://127.0.0.1:8081/health'),
]

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
