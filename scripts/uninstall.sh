#!/usr/bin/env bash
# =============================================================================
# moonraker-owl Uninstallation Script
# =============================================================================
# This script removes moonraker-owl from a Klipper/Moonraker system.
#
# Usage:
#   ./uninstall.sh              # Interactive uninstallation
#   ./uninstall.sh --keep-config  # Keep configuration files
#   ./uninstall.sh --all        # Remove everything including config
#
# =============================================================================

set -euo pipefail

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
APP_NAME="moonraker-owl"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"
CONFIG_DIR="${HOME}/printer_data/config"
CONFIG_FILE="${CONFIG_DIR}/${APP_NAME}.cfg"
LOG_DIR="${HOME}/printer_data/logs"
LOG_FILE="${LOG_DIR}/${APP_NAME}.log"
SERVICE_NAME="${APP_NAME}"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
CREDENTIALS_DIR="${HOME}/.owl"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Options
KEEP_CONFIG=false
REMOVE_ALL=false

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

print_header() {
    echo -e "${BLUE}"
    echo "========================================"
    echo "  moonraker-owl Uninstaller"
    echo "========================================"
    echo -e "${NC}"
}

print_step() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[i]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

stop_service() {
    print_info "Stopping service..."

    if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
        sudo systemctl stop "${SERVICE_NAME}"
        print_step "Service stopped"
    else
        print_info "Service was not running"
    fi
}

disable_service() {
    print_info "Disabling service..."

    if systemctl is-enabled --quiet "${SERVICE_NAME}" 2>/dev/null; then
        sudo systemctl disable "${SERVICE_NAME}"
        print_step "Service disabled"
    else
        print_info "Service was not enabled"
    fi
}

remove_service_file() {
    print_info "Removing service file..."

    if [[ -f "${SERVICE_FILE}" ]]; then
        sudo rm -f "${SERVICE_FILE}"
        sudo systemctl daemon-reload
        sudo systemctl reset-failed 2>/dev/null || true
        print_step "Service file removed"
    else
        print_info "Service file not found"
    fi
}

remove_virtualenv() {
    print_info "Removing virtual environment..."

    if [[ -d "${VENV_DIR}" ]]; then
        rm -rf "${VENV_DIR}"
        print_step "Virtual environment removed"
    else
        print_info "Virtual environment not found"
    fi
}

remove_config() {
    if [[ "${KEEP_CONFIG}" == "true" ]]; then
        print_info "Keeping configuration files (--keep-config specified)"
        return
    fi

    print_info "Removing configuration files..."

    if [[ -f "${CONFIG_FILE}" ]]; then
        rm -f "${CONFIG_FILE}"
        print_step "Configuration file removed: ${CONFIG_FILE}"
    fi

    if [[ -f "${LOG_FILE}" ]]; then
        rm -f "${LOG_FILE}"
        print_step "Log file removed: ${LOG_FILE}"
    fi

    # Remove rotated logs
    rm -f "${LOG_DIR}/${APP_NAME}.log."* 2>/dev/null || true
}

remove_credentials() {
    if [[ "${KEEP_CONFIG}" == "true" ]]; then
        print_info "Keeping credentials (--keep-config specified)"
        return
    fi

    print_info "Removing credentials..."

    if [[ -d "${CREDENTIALS_DIR}" ]]; then
        rm -rf "${CREDENTIALS_DIR}"
        print_step "Credentials directory removed: ${CREDENTIALS_DIR}"
    else
        print_info "Credentials directory not found"
    fi
}

prompt_confirmation() {
    if [[ "${REMOVE_ALL}" == "true" ]]; then
        return 0
    fi

    echo ""
    echo "This will remove:"
    echo "  - moonraker-owl systemd service"
    echo "  - Virtual environment at ${VENV_DIR}"
    
    if [[ "${KEEP_CONFIG}" != "true" ]]; then
        echo "  - Configuration file at ${CONFIG_FILE}"
        echo "  - Log files at ${LOG_DIR}/${APP_NAME}.log*"
        echo "  - Credentials at ${CREDENTIALS_DIR}"
    fi
    
    echo ""
    echo -e "${YELLOW}Note: The project source code in ${PROJECT_DIR} will NOT be removed.${NC}"
    echo ""
    
    read -p "Continue? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Uninstallation cancelled"
        exit 0
    fi
}

print_complete() {
    echo ""
    echo -e "${GREEN}========================================"
    echo "  Uninstallation Complete!"
    echo -e "========================================${NC}"
    echo ""
    
    if [[ "${KEEP_CONFIG}" == "true" ]]; then
        echo "Configuration preserved at: ${CONFIG_FILE}"
        echo "Credentials preserved at: ${CREDENTIALS_DIR}"
        echo ""
        echo "To reinstall, run: ./scripts/install.sh"
    else
        echo "All moonraker-owl components have been removed."
        echo ""
        echo "To reinstall from scratch, run: ./scripts/install.sh"
    fi
    
    echo ""
    echo "To completely remove the source code:"
    echo "  rm -rf ${PROJECT_DIR}"
    echo ""
}

show_help() {
    echo "moonraker-owl Uninstallation Script"
    echo ""
    echo "Usage: ./uninstall.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --keep-config  Keep configuration and credentials"
    echo "  --all          Remove everything without prompting"
    echo "  --help, -h     Show this help message"
    echo ""
    echo "This script removes moonraker-owl from your system, including:"
    echo "  - Systemd service"
    echo "  - Virtual environment"
    echo "  - Configuration files (unless --keep-config)"
    echo "  - Device credentials (unless --keep-config)"
    echo ""
    echo "The source code directory is NOT removed automatically."
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --keep-config)
                KEEP_CONFIG=true
                ;;
            --all)
                REMOVE_ALL=true
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
        shift
    done

    print_header
    prompt_confirmation

    # Run uninstallation steps
    stop_service
    disable_service
    remove_service_file
    remove_virtualenv
    remove_config
    remove_credentials
    print_complete
}

main "$@"
