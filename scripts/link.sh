#!/usr/bin/env bash
# =============================================================================
# moonraker-owl Device Linking Helper
# =============================================================================
# This script provides a convenient way to link your printer to Owl Cloud.
#
# Usage:
#   ./link.sh              # Interactive linking
#   ./link.sh --force      # Re-link even if already linked
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"
CONFIG_FILE="${HOME}/printer_data/config/moonraker-owl.cfg"

# Check if venv exists
if [[ ! -d "${VENV_DIR}" ]]; then
    echo "Error: Virtual environment not found at ${VENV_DIR}"
    echo "Please run ./scripts/install.sh first."
    exit 1
fi

# Check if config exists
if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "Error: Configuration file not found at ${CONFIG_FILE}"
    echo "Please run ./scripts/install.sh first."
    exit 1
fi

# Run the link command
exec "${VENV_DIR}/bin/moonraker-owl" --config "${CONFIG_FILE}" link "$@"
