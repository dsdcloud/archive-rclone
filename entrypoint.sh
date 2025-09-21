#!/bin/sh
set -e
mkdir -p /config /downloads
if [ ! -f "$RCLONE_CONFIG_PATH" ]; then
  echo "No rclone config found at $RCLONE_CONFIG_PATH. Upload via bot or mount one."
fi
exec python bot.py
