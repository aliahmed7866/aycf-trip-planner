#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
PRIVATE_PAGE="https://multipass.wizzair.com/en/w6/subscriptions/spa/private-page/wallets"

if ! command -v adb >/dev/null 2>&1; then
  echo "[AYCF] adb unavailable; cannot auto-refresh Wizz session."
  exit 20
fi

adb start-server >/dev/null 2>&1 || true

connected_device() {
  adb devices 2>/dev/null | awk 'NR>1 && $2=="device" {print $1; exit}'
}

try_connect() {
  local addr="$1"
  [ -n "$addr" ] || return 1
  adb connect "$addr" >/dev/null 2>&1 && return 0
  ANDROID_NO_USE_FWMARK_CLIENT=1 adb connect "$addr" >/dev/null 2>&1 && return 0
  if command -v fakeroot >/dev/null 2>&1; then
    fakeroot env ANDROID_NO_USE_FWMARK_CLIENT=1 adb connect "$addr" >/dev/null 2>&1 && return 0
  fi
  return 1
}

DEVICE="$(connected_device || true)"
if [ -z "$DEVICE" ]; then
  # Wireless Debugging pairing persists. Android advertises the current normal
  # ADB connection port via mDNS, even though that port changes over time.
  MDNS="$(adb mdns services 2>/dev/null | awk '/_adb-tls-connect\._tcp/ {print $NF; exit}' || true)"
  if [ -n "$MDNS" ]; then
    PORT="${MDNS##*:}"
    # Same-device localhost avoids Android routing/fwmark problems on some ROMs.
    try_connect "127.0.0.1:$PORT" || try_connect "$MDNS" || true
  fi
  DEVICE="$(connected_device || true)"
fi

if [ -z "$DEVICE" ]; then
  echo "[AYCF] Automatic Wizz refresh unavailable: Wireless debugging is off or the phone is not reachable through the existing ADB pairing."
  exit 21
fi

adb forward --remove tcp:9222 >/dev/null 2>&1 || true
if ! adb forward tcp:9222 localabstract:chrome_devtools_remote >/dev/null 2>&1; then
  echo "[AYCF] Could not expose Chrome DevTools through ADB."
  exit 22
fi

# Keep this non-interactive. The Python helper can reopen the Wizz private page
# through CDP if Chrome has no Multipass tab, but it never types credentials.
set +e
python "$APP_DIR/termux/refresh_wizz_from_chrome.py"
RC=$?
set -e
adb forward --remove tcp:9222 >/dev/null 2>&1 || true
exit "$RC"
