#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"

if ! command -v adb >/dev/null 2>&1; then
  echo "[AYCF] adb unavailable; cannot auto-refresh Wizz session."
  exit 20
fi

adb start-server >/dev/null 2>&1 || true
connected_device() { adb devices 2>/dev/null | awk 'NR>1 && $2=="device" {print $1; exit}'; }
try_connect() {
  local addr="$1"; [ -n "$addr" ] || return 1
  adb connect "$addr" >/dev/null 2>&1 && return 0
  ANDROID_NO_USE_FWMARK_CLIENT=1 adb connect "$addr" >/dev/null 2>&1 && return 0
  if command -v fakeroot >/dev/null 2>&1; then fakeroot env ANDROID_NO_USE_FWMARK_CLIENT=1 adb connect "$addr" >/dev/null 2>&1 && return 0; fi
  return 1
}

DEVICE="$(connected_device || true)"
if [ -z "$DEVICE" ]; then
  MDNS="$(adb mdns services 2>/dev/null | awk '/_adb-tls-connect\._tcp/ {print $NF; exit}' || true)"
  if [ -n "$MDNS" ]; then
    PORT="${MDNS##*:}"
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

cleanup() { adb forward --remove tcp:9222 >/dev/null 2>&1 || true; }
trap cleanup EXIT

set +e
python "$APP_DIR/termux/refresh_wizz_from_chrome.py"
RC=$?
set -e

# refresh helper uses 4 specifically for "Chrome is reachable but login is required".
# If encrypted credentials were opted into, attempt an ordinary login once, then
# validate/capture cookies again. Security challenges remain manual by design.
if [ "$RC" -eq 4 ]; then
  set +e
  python "$APP_DIR/termux/auto_login_wizz.py"
  LOGIN_RC=$?
  set -e
  if [ "$LOGIN_RC" -eq 0 ]; then
    set +e
    python "$APP_DIR/termux/refresh_wizz_from_chrome.py"
    RC=$?
    set -e
  else
    RC="$LOGIN_RC"
  fi
fi
exit "$RC"
