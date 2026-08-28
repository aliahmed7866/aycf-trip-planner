#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

APP_DIR="${AYCF_APP_DIR:-$HOME/aycf-trip-planner}"
ENV_FILE="${AYCF_CONFIG_DIR:-$HOME/.config/aycf}/env"
PRIVATE_PAGE="https://multipass.wizzair.com/w6/subscriptions/spa/private-page/wallets"

if [ ! -f "$ENV_FILE" ]; then
  echo "Missing $ENV_FILE. Run termux/setup.sh first."
  exit 1
fi
# shellcheck disable=SC1090
source "$ENV_FILE"

if ! command -v adb >/dev/null 2>&1; then
  echo "adb is not installed. Run: pkg install android-tools"
  exit 1
fi

pair_try() {
  local label="$1"
  shift
  echo
  echo "[AYCF] Pair attempt: $label"
  adb kill-server >/dev/null 2>&1 || true
  if "$@"; then
    echo "[AYCF] Pairing succeeded: $label"
    return 0
  fi
  return 1
}

connect_try() {
  local label="$1"
  shift
  echo "[AYCF] Connect attempt: $label"
  if "$@"; then
    return 0
  fi
  return 1
}

cat <<'EOF'
Android-only Wizz connection
----------------------------
This uses Android Wireless Debugging to let Termux talk to Chrome on THIS phone.
Your Wizz password stays in Chrome. The importer reads only the browser/session
material needed for AYCF and stores the resulting session encrypted locally.

Before continuing:
  1. Enable Developer options on Android.
  2. Turn BOTH USB debugging and Wireless debugging ON.
  3. If a VPN/private DNS/firewall app is active, temporarily disable it.
  4. Developer options > Wireless debugging > Pair device with pairing code.
  5. KEEP THE PAIRING POPUP OPEN while entering the details below. The pairing
     port/code are temporary and can change as soon as the popup is closed.

Split-screen with Settings and Termux is strongly recommended.
EOF

read -r -p "Pairing address shown by Android (IP:PORT): " PAIR_ADDR
read -r -p "6-digit pairing code: " PAIR_CODE

PAIR_PORT="${PAIR_ADDR##*:}"
if ! [[ "$PAIR_PORT" =~ ^[0-9]+$ ]] || [ "$PAIR_PORT" -lt 1 ] || [ "$PAIR_PORT" -gt 65535 ]; then
  echo "Invalid pairing address: $PAIR_ADDR"
  exit 1
fi
if ! [[ "$PAIR_CODE" =~ ^[0-9]{6}$ ]]; then
  echo "Pairing code must be exactly 6 digits."
  exit 1
fi

PAIRED=false

# Standard Android platform-tools path.
if pair_try "normal adb" adb pair "$PAIR_ADDR" "$PAIR_CODE"; then
  PAIRED=true
fi

# Android's fwmark networking can block an app from talking back to adbd on the
# same device. This environment variable is the established Termux workaround.
if [ "$PAIRED" = false ] && pair_try "fwmark workaround" env ANDROID_NO_USE_FWMARK_CLIENT=1 adb pair "$PAIR_ADDR" "$PAIR_CODE"; then
  PAIRED=true
fi

# On Samsung and some other Android builds, fakeroot + localhost is more reliable
# for the same-device pairing TLS socket than the Wi-Fi address shown by Android.
if [ "$PAIRED" = false ] && command -v fakeroot >/dev/null 2>&1; then
  if pair_try "fakeroot + localhost" env ANDROID_NO_USE_FWMARK_CLIENT=1 fakeroot adb pair "localhost:$PAIR_PORT" "$PAIR_CODE"; then
    PAIRED=true
  elif pair_try "fakeroot + displayed address" env ANDROID_NO_USE_FWMARK_CLIENT=1 fakeroot adb pair "$PAIR_ADDR" "$PAIR_CODE"; then
    PAIRED=true
  fi
fi

if [ "$PAIRED" = false ]; then
  cat <<'EOF'

ADB pairing did not complete.

This exact 'protocol fault (couldn't read status message)' is a known failure
mode of Android platform-tools/Termux on some devices. Do not keep reusing the
same code: close and reopen 'Pair device with pairing code' before another try.

Recommended recovery:
  • Settings > Developer options > Wireless debugging: OFF, then ON.
  • Re-open Pair device with pairing code and keep the popup visible.
  • Make sure USB debugging is also ON.
  • Temporarily disable VPN/private-DNS/firewall apps.
  • Run this script again.

Your existing AYCF database and encrypted Wizz session were not modified.
EOF
  exit 1
fi

cat <<'EOF'

Pairing is complete.
Now return to the MAIN Wireless debugging screen. Android shows a DIFFERENT
'IP address & Port' for normal debugging. Do not use the pairing-popup port.
EOF
read -r -p "Wireless debugging connection address (IP:PORT): " CONNECT_ADDR
CONNECT_PORT="${CONNECT_ADDR##*:}"

CONNECTED=false
if connect_try "normal adb" adb connect "$CONNECT_ADDR"; then
  CONNECTED=true
fi
if [ "$CONNECTED" = false ] && connect_try "fwmark workaround" env ANDROID_NO_USE_FWMARK_CLIENT=1 adb connect "$CONNECT_ADDR"; then
  CONNECTED=true
fi
if [ "$CONNECTED" = false ] && command -v fakeroot >/dev/null 2>&1 && [[ "$CONNECT_PORT" =~ ^[0-9]+$ ]]; then
  if connect_try "fakeroot + localhost" env ANDROID_NO_USE_FWMARK_CLIENT=1 fakeroot adb connect "localhost:$CONNECT_PORT"; then
    CONNECTED=true
  elif connect_try "fakeroot + displayed address" env ANDROID_NO_USE_FWMARK_CLIENT=1 fakeroot adb connect "$CONNECT_ADDR"; then
    CONNECTED=true
  fi
fi

if [ "$CONNECTED" = false ]; then
  echo "ADB connection failed. Keep Wireless debugging enabled and verify the MAIN-screen connection port."
  exit 1
fi

adb forward --remove tcp:9222 >/dev/null 2>&1 || true
if ! adb forward tcp:9222 localabstract:chrome_devtools_remote >/dev/null; then
  if command -v fakeroot >/dev/null 2>&1; then
    env ANDROID_NO_USE_FWMARK_CLIENT=1 fakeroot adb forward --remove tcp:9222 >/dev/null 2>&1 || true
    env ANDROID_NO_USE_FWMARK_CLIENT=1 fakeroot adb forward tcp:9222 localabstract:chrome_devtools_remote >/dev/null
  else
    echo "Could not create Chrome DevTools ADB forward."
    exit 1
  fi
fi

cat <<EOF

Opening Wizz Multipass in Chrome:
  $PRIVATE_PAGE

Log in normally in Chrome. Complete any MFA/CAPTCHA. Once you are on the
logged-in Multipass wallet/search page, come back to Termux and press Enter.
EOF
termux-open-url "$PRIVATE_PAGE" >/dev/null 2>&1 || true
read -r -p "Press Enter after Wizz login is complete... " _

python "$APP_DIR/termux/import_wizz_from_chrome.py"

adb forward --remove tcp:9222 >/dev/null 2>&1 || true
if command -v fakeroot >/dev/null 2>&1; then
  env ANDROID_NO_USE_FWMARK_CLIENT=1 fakeroot adb forward --remove tcp:9222 >/dev/null 2>&1 || true
fi
cat <<'EOF'

Done. You may now turn Wireless debugging OFF again in Developer options.
The encrypted Wizz session remains in your AYCF local vault.
EOF
