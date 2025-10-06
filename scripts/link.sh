#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_FILE="$HOME/printer_data/config/moonraker-owl.cfg"

python3 -m moonraker_owl.cli --config "${CONFIG_FILE}" link "$@"
