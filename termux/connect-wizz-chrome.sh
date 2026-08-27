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

cat <<'EOF'
Android-only Wizz connection
----------------------------
This uses Android Wireless Debugging to let Termux talk to Chrome on THIS phone.
Your Wizz password stays in Chrome. The importer reads only wizzair.com cookies,
validates them, and encrypts them directly in the local AYCF vault.

Before continuing:
  1. Enable Developer options on Android.
  2. Developer options > Wireless debugging > ON.
  3. Tap Wireless debugging > Pair device with pairing code.
  4. Keep that pairing screen open (split-screen with Termux is easiest).
EOF

read -r -p "Pairing address shown by Android (IP:PORT): " PAIR_ADDR
read -r -p "6-digit pairing code: " PAIR_CODE

if ! adb pair "$PAIR_ADDR" "$PAIR_CODE"; then
  echo
  echo "Normal adb pair failed. Retrying with Android network-mark workaround..."
  if ! ANDROID_NO_USE_FWMARK_CLIENT=1 adb pair "$PAIR_ADDR" "$PAIR_CODE"; then
    echo "Pairing failed. Re-open 'Pair device with pairing code' and try again."
    exit 1
  fi
fi

cat <<'EOF'

Now return to the main Wireless debugging screen. Android shows a DIFFERENT
'IP address & Port' for normal debugging (not the pairing popup port).
EOF
read -r -p "Wireless debugging connection address (IP:PORT): " CONNECT_ADDR

if ! adb connect "$CONNECT_ADDR"; then
  if ! ANDROID_NO_USE_FWMARK_CLIENT=1 adb connect "$CONNECT_ADDR"; then
    echo "ADB connection failed. Check that Wireless debugging is still enabled."
    exit 1
  fi
fi

adb forward --remove tcp:9222 >/dev/null 2>&1 || true
adb forward tcp:9222 localabstract:chrome_devtools_remote >/dev/null

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
cat <<'EOF'

Done. You may now turn Wireless debugging OFF again in Developer options.
The encrypted Wizz session remains in your AYCF local vault.
EOF
