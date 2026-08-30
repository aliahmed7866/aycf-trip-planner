#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
PRIVATE_PAGE="https://multipass.wizzair.com/w6/subscriptions/spa/private-page/wallets"
DEVTOOLS_PORT="${AYCF_CHROME_DEVTOOLS_PORT:-9222}"

# Repair older Chrome captures that saved the endpoint-discovery GET request
# instead of the POST JSON availability template. This is safe and idempotent.
python "$APP_DIR/termux/repair_wizz_runtime.py" || true

# First choice: renew entirely inside Termux using the encrypted credentials and
# Wizz's actual HTTP login form/redirect flow. This avoids Android Chrome/CDP.
set +e
python "$APP_DIR/termux/refresh_wizz_direct.py"
DIRECT_RC=$?
set -e
if [ "$DIRECT_RC" -eq 0 ]; then
  exit 0
fi

# A direct-network failure is not improved by restarting Chrome. Fail cleanly
# and let the next scheduled/manual scan retry naturally.
if [ "$DIRECT_RC" -eq 20 ]; then
  echo "[AYCF] Direct Wizz renewal hit a network error; browser fallback skipped."
  exit "$DIRECT_RC"
fi

# Browser fallback is now exceptional: missing/changed login flow, expired or
# rejected credentials, first capture, or an interactive security challenge.
echo "[AYCF] Direct Wizz renewal was not sufficient (exit $DIRECT_RC); trying browser fallback."

if ! command -v adb >/dev/null 2>&1; then
  echo "[AYCF] Browser fallback unavailable because adb is not installed/reachable."
  exit "$DIRECT_RC"
fi

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

discover_and_connect() {
  local mdns_list addr port
  mdns_list="$(adb mdns services 2>/dev/null | awk '/_adb-tls-connect\._tcp/ {print $NF}' || true)"
  [ -n "$mdns_list" ] || return 1
  for addr in $mdns_list; do
    port="${addr##*:}"
    if [[ "$port" =~ ^[0-9]+$ ]]; then
      # Same-device Samsung/Android commonly accepts localhost even when the
      # mDNS record advertises the Wi-Fi address.
      try_connect "127.0.0.1:$port" || try_connect "localhost:$port" || try_connect "$addr" || true
      [ -n "$(connected_device || true)" ] && return 0
    else
      try_connect "$addr" || true
      [ -n "$(connected_device || true)" ] && return 0
    fi
  done
  return 1
}

start_adb_normal() {
  adb start-server >/dev/null 2>&1 || true
}

start_adb_fwmark() {
  # ANDROID_NO_USE_FWMARK_CLIENT only helps same-device networking when the ADB
  # server itself inherited it. Restarting merely the client is insufficient if
  # a normal adb server is already running.
  adb kill-server >/dev/null 2>&1 || true
  ANDROID_NO_USE_FWMARK_CLIENT=1 adb start-server >/dev/null 2>&1 || true
}

start_adb_fakeroot_fwmark() {
  command -v fakeroot >/dev/null 2>&1 || return 1
  adb kill-server >/dev/null 2>&1 || true
  fakeroot env ANDROID_NO_USE_FWMARK_CLIENT=1 adb start-server >/dev/null 2>&1 || true
}

# Wireless debugging being enabled does not guarantee that Termux's current ADB
# server can reach adbd. The Android debugging port can rotate, and on Samsung a
# server started without the fwmark workaround can fail even though pairing is
# still valid. Try progressively stronger same-device recovery modes before
# declaring Wireless debugging unavailable.
start_adb_normal
DEVICE="$(connected_device || true)"
if [ -z "$DEVICE" ]; then
  discover_and_connect || true
  DEVICE="$(connected_device || true)"
fi
if [ -z "$DEVICE" ]; then
  echo "[AYCF] ADB device not visible; restarting ADB with Android same-device networking workaround."
  start_adb_fwmark
  discover_and_connect || true
  DEVICE="$(connected_device || true)"
fi
if [ -z "$DEVICE" ] && command -v fakeroot >/dev/null 2>&1; then
  echo "[AYCF] ADB still unavailable; retrying same-device discovery through fakeroot."
  start_adb_fakeroot_fwmark || true
  discover_and_connect || true
  DEVICE="$(connected_device || true)"
fi
if [ -z "$DEVICE" ]; then
  echo "[AYCF] Browser fallback unavailable: Wireless debugging is enabled but no paired ADB endpoint could be reached. Open Android Wireless debugging once and verify this phone remains paired with Termux."
  exit 21
fi

echo "[AYCF] Browser fallback connected to Android via ADB ($DEVICE)."

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

if [ "$RC" -eq 3 ]; then
  if restart_chrome_for_wizz; then
    set +e
    run_refresh
    RC=$?
    set -e
  fi
fi

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
