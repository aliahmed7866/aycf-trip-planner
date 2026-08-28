#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
PRIVATE_PAGE="https://multipass.wizzair.com/w6/subscriptions/spa/private-page/wallets"
DEVTOOLS_PORT="${AYCF_CHROME_DEVTOOLS_PORT:-9222}"

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

cleanup() { adb forward --remove "tcp:$DEVTOOLS_PORT" >/dev/null 2>&1 || true; }
trap cleanup EXIT

forward_devtools() {
  adb forward --remove "tcp:$DEVTOOLS_PORT" >/dev/null 2>&1 || true
  adb forward "tcp:$DEVTOOLS_PORT" localabstract:chrome_devtools_remote >/dev/null 2>&1
}

devtools_ok() {
  command -v curl >/dev/null 2>&1 || return 1
  curl --silent --show-error --fail --max-time 4 "http://127.0.0.1:$DEVTOOLS_PORT/json/version" 2>/dev/null | grep -q 'webSocketDebuggerUrl'
}

restart_chrome_for_wizz() {
  echo "[AYCF] Chrome DevTools is unresponsive; restarting Chrome and reopening Wizz automatically."
  adb shell am force-stop com.android.chrome >/dev/null 2>&1 || true
  adb shell am start -a android.intent.action.VIEW -d "$PRIVATE_PAGE" com.android.chrome >/dev/null 2>&1 || \
    adb shell monkey -p com.android.chrome 1 >/dev/null 2>&1 || true
  for _ in 1 2 3 4 5 6 7 8; do
    sleep 1
    if adb shell cat /proc/net/unix 2>/dev/null | grep -q '@chrome_devtools_remote'; then
      forward_devtools || true
      if devtools_ok; then
        echo "[AYCF] Chrome DevTools recovered automatically."
        return 0
      fi
    fi
  done
  return 1
}

if ! forward_devtools; then
  echo "[AYCF] Could not expose Chrome DevTools through ADB."
  exit 22
fi

# Android Chrome can leave the DevTools abstract socket present but wedged. Heal
# that once here instead of requiring manual adb/Chrome recovery on every expiry.
if ! devtools_ok; then
  if ! restart_chrome_for_wizz; then
    echo "[AYCF] Chrome DevTools remained unavailable after automatic restart."
    exit 23
  fi
fi

run_refresh() {
  set +e
  python "$APP_DIR/termux/refresh_wizz_from_chrome.py"
  local rc=$?
  set -e
  return "$rc"
}

set +e
run_refresh
RC=$?
set -e

# A transient CDP failure can still occur after Chrome starts. Recover once and
# retry before giving up; this keeps the normal scan path one-command/unattended.
if [ "$RC" -eq 3 ]; then
  if restart_chrome_for_wizz; then
    set +e
    run_refresh
    RC=$?
    set -e
  fi
fi

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
    run_refresh
    RC=$?
    set -e
  else
    RC="$LOGIN_RC"
  fi
fi
exit "$RC"
