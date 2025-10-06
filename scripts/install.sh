#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
APP_NAME="moonraker-owl"
DEFAULT_VENV="$HOME/${APP_NAME}-env"
CONFIG_DIR="$HOME/printer_data/config"
CONFIG_FILE="${CONFIG_DIR}/${APP_NAME}.cfg"

print_header() {
  echo "========================================"
  echo " moonraker-owl installer (scaffolding)"
  echo "========================================"
}

ensure_dirs() {
  mkdir -p "${CONFIG_DIR}"
  mkdir -p "$HOME/.owl"
}

create_default_config() {
  if [[ -f "${CONFIG_FILE}" ]]; then
    echo "Config exists at ${CONFIG_FILE}; leaving untouched."
    return
  fi

  cat <<'EOF' > "${CONFIG_FILE}"
[cloud]
base_url = https://api.owl.dev
broker_host = mqtt.owl.dev
broker_port = 8883

[moonraker]
url = http://127.0.0.1:7125
transport = websocket

[telemetry]
rate_hz = 1.0
include_fields = status,progress,telemetry

[commands]
ack_timeout_seconds = 30

[logging]
level = INFO
path = ~/printer_data/logs/moonraker-owl.log
log_network = false
EOF
  echo "Wrote default config to ${CONFIG_FILE}."
}

print_next_steps() {
  cat <<EOF

TODO: Complete the installer logic.
- Create a dedicated virtual environment (default: ${DEFAULT_VENV}).
- Install moonraker-owl into the environment: pip install ${PROJECT_DIR}
- Register the systemd unit in ${PROJECT_DIR}/systemd/moonraker-owl.service
- Enable & start the service: systemctl enable --now moonraker-owl.service

This scaffolding script currently sets up config directories only.
EOF
}

main() {
  print_header
  ensure_dirs
  create_default_config
  print_next_steps
}

main "$@"
