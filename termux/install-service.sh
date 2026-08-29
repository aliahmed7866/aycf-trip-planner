#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
PORT="${AYCF_PORT:-8080}"
SERVICE_DIR="$PREFIX/var/service/aycf"
VENV_DIR="$APP_DIR/.venv"

cd "$APP_DIR"

if ! command -v sv >/dev/null 2>&1; then
  pkg install -y termux-services
fi

if [ ! -d "$VENV_DIR" ]; then
  python -m venv "$VENV_DIR"
fi

"$VENV_DIR/bin/python" -m pip install --upgrade pip
"$VENV_DIR/bin/pip" install -r requirements.txt

mkdir -p "$SERVICE_DIR"
cat > "$SERVICE_DIR/run" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
exec 2>&1
cd "$APP_DIR"
export PORT=$PORT
exec "$VENV_DIR/bin/python" app.py
EOF
chmod +x "$SERVICE_DIR/run"

sv-enable aycf >/dev/null 2>&1 || true
sv up aycf >/dev/null 2>&1 || true

sleep 2
if curl -fsS --max-time 10 "http://127.0.0.1:$PORT/health" >/dev/null; then
  echo "[AYCF] Running on http://127.0.0.1:$PORT"
else
  echo "[AYCF] Service installed, but health check is not responding yet."
  sv status aycf || true
  exit 1
fi
