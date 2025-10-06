#!/usr/bin/env bash
set -euo pipefail

APP_NAME="moonraker-owl"
CONFIG_FILE="$HOME/printer_data/config/${APP_NAME}.cfg"
LOG_FILE="$HOME/printer_data/logs/${APP_NAME}.log"
SERVICE_PATH="/etc/systemd/system/${APP_NAME}.service"
VENV_PATH="$HOME/${APP_NAME}-env"

cat <<EOF
moonraker-owl uninstall helper (scaffolding)

Manual steps required:
  sudo systemctl stop ${APP_NAME}.service || true
  sudo systemctl disable ${APP_NAME}.service || true
  sudo rm -f ${SERVICE_PATH}
  sudo systemctl daemon-reload
  sudo systemctl reset-failed || true
  rm -rf ${VENV_PATH}
  rm -f ${CONFIG_FILE}
  rm -f ${LOG_FILE}
  rm -rf ~/moonraker-owl

Note: This script currently prints instructions only.
EOF
