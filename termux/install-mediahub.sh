#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail
MEDIAHUB_APP_DIR="${MEDIAHUB_APP_DIR:-$HOME/Django-youtube-video-and-mp3-downloader}"
if [ ! -d "$MEDIAHUB_APP_DIR/.git" ]; then
  git clone https://github.com/aliahmed7866/Django-youtube-video-and-mp3-downloader.git "$MEDIAHUB_APP_DIR"
fi
if [ ! -f "$MEDIAHUB_APP_DIR/termux/install-service.sh" ]; then
  echo 'Media Hub rebuild is not present. Merge its rebuild PR and update this checkout first.' >&2
  exit 1
fi
exec bash "$MEDIAHUB_APP_DIR/termux/install-service.sh"
