#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SYSTEMD_DIR="/etc/systemd/system"

HISTORY_UNIT_SRC="$SCRIPT_DIR/flight-history.service"
CACHE_UNIT_SRC="$SCRIPT_DIR/aircraft-image-cache.service"

HISTORY_UNIT_DST="$SYSTEMD_DIR/piaware-modern-flight-history.service"
CACHE_UNIT_DST="$SYSTEMD_DIR/piaware-modern-aircraft-image-cache.service"

if [[ "${EUID}" -ne 0 ]]; then
    echo "Run this script with sudo."
    exit 1
fi

sed "s|@PROJECT_ROOT@|$PROJECT_ROOT|g" "$HISTORY_UNIT_SRC" > "$HISTORY_UNIT_DST"
sed "s|@PROJECT_ROOT@|$PROJECT_ROOT|g" "$CACHE_UNIT_SRC" > "$CACHE_UNIT_DST"
chmod 0644 "$HISTORY_UNIT_DST" "$CACHE_UNIT_DST"

systemctl daemon-reload
systemctl enable --now piaware-modern-flight-history.service
systemctl enable --now piaware-modern-aircraft-image-cache.service
systemctl restart piaware-modern-flight-history.service
systemctl restart piaware-modern-aircraft-image-cache.service

echo "Installed and started:"
echo "  piaware-modern-flight-history.service"
echo "  piaware-modern-aircraft-image-cache.service"
echo "Using project root:"
echo "  $PROJECT_ROOT"
