#!/usr/bin/env bash
# =============================================================================
# moonraker-owl Installation Script
# =============================================================================
# This script installs moonraker-owl on a Klipper/Moonraker system.
#
# Usage:
#   ./install.sh              # Standard installation
#   ./install.sh --help       # Show help
#
# Requirements:
#   - Python 3.10+
#   - Moonraker running on this system
#   - Internet connection (for pip packages)
#
# The script will:
#   1. Create a Python virtual environment in the project directory
#   2. Install moonraker-owl and dependencies
#   3. Create default configuration file
#   4. Register and start the systemd service
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
SERVICE_NAME="${APP_NAME}"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
CREDENTIALS_DIR="${HOME}/.owl"

# CN pip mirror (Tsinghua University)
PIP_MIRROR="https://pypi.tuna.tsinghua.edu.cn/simple"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

print_header() {
    echo -e "${BLUE}"
    echo "========================================"
    echo "  moonraker-owl Installer"
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

check_python() {
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed. Please install Python 3.10 or later."
        exit 1
    fi

    local python_version
    python_version=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    local major minor
    major=$(echo "$python_version" | cut -d. -f1)
    minor=$(echo "$python_version" | cut -d. -f2)

    if [[ "$major" -lt 3 ]] || [[ "$major" -eq 3 && "$minor" -lt 10 ]]; then
        print_error "Python 3.10+ is required. Found: Python $python_version"
        exit 1
    fi

    print_step "Python $python_version detected"
}

check_prerequisites() {
    print_info "Checking prerequisites..."

    check_python

    # Check for venv module
    if ! python3 -c "import venv" &> /dev/null; then
        print_error "Python venv module not found. Install with: sudo apt install python3-venv"
        exit 1
    fi
    print_step "Python venv module available"

    # Check if Moonraker is running (optional, just a warning)
    if ! systemctl is-active --quiet moonraker 2>/dev/null; then
        print_warn "Moonraker service not detected. Make sure Moonraker is running before starting moonraker-owl."
    else
        print_step "Moonraker service detected"
    fi
}

create_directories() {
    print_info "Creating directories..."

    mkdir -p "${CONFIG_DIR}"
    mkdir -p "${LOG_DIR}"
    mkdir -p "${CREDENTIALS_DIR}"
    chmod 700 "${CREDENTIALS_DIR}"

    print_step "Directories created"
}

create_virtualenv() {
    print_info "Creating virtual environment..."

    if [[ -d "${VENV_DIR}" ]]; then
        print_warn "Virtual environment already exists. Recreating..."
        rm -rf "${VENV_DIR}"
    fi

    python3 -m venv "${VENV_DIR}"
    print_step "Virtual environment created at ${VENV_DIR}"
}

install_package() {
    print_info "Installing moonraker-owl..."

    # Upgrade pip first
    "${VENV_DIR}/bin/pip" install --upgrade pip -i "${PIP_MIRROR}" --quiet

    # Install the package
    "${VENV_DIR}/bin/pip" install "${PROJECT_DIR}" -i "${PIP_MIRROR}"

    print_step "moonraker-owl installed"
}

create_config() {
    print_info "Setting up configuration..."

    if [[ -f "${CONFIG_FILE}" ]]; then
        print_warn "Configuration file already exists at ${CONFIG_FILE}"
        print_warn "Keeping existing configuration."
    else
        # Copy example config (must exist in distribution)
        if [[ ! -f "${PROJECT_DIR}/owl.cfg.example" ]]; then
            print_error "owl.cfg.example not found in ${PROJECT_DIR}"
            print_error "The installation package appears to be incomplete."
            exit 1
        fi
        cp "${PROJECT_DIR}/owl.cfg.example" "${CONFIG_FILE}"
        print_step "Configuration file created at ${CONFIG_FILE}"
    fi
}

install_service() {
    print_info "Installing systemd service..."

    # Get current user
    local current_user
    current_user=$(whoami)

    # Generate service file
    sudo tee "${SERVICE_FILE}" > /dev/null << EOF
[Unit]
Description=moonraker-owl - Owl Cloud Agent for Moonraker
Documentation=https://github.com/project-owl/agent
After=network-online.target moonraker.service
Wants=network-online.target

[Service]
Type=simple
User=${current_user}
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_DIR}/bin/moonraker-owl --config ${CONFIG_FILE} start
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=${LOG_DIR} ${CREDENTIALS_DIR}
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable "${SERVICE_NAME}"

    print_step "Systemd service installed and enabled"
}

print_next_steps() {
    echo ""
    echo -e "${GREEN}========================================"
    echo "  Installation Complete!"
    echo -e "========================================${NC}"
    echo ""
    echo -e "${YELLOW}⚠  IMPORTANT: Device Linking Required${NC}"
    echo ""
    echo "Before starting the service, you must link this printer to Owl Cloud:"
    echo ""
    echo -e "  ${BLUE}1.${NC} Open Owl Web and navigate to Printers → Add Printer"
    echo -e "  ${BLUE}2.${NC} Copy the 6-character link code"
    echo -e "  ${BLUE}3.${NC} Run the link command:"
    echo ""
    echo -e "     ${GREEN}${VENV_DIR}/bin/moonraker-owl --config ${CONFIG_FILE} link${NC}"
    echo ""
    echo -e "  ${BLUE}4.${NC} Enter the link code when prompted"
    echo -e "  ${BLUE}5.${NC} Start the service:"
    echo ""
    echo -e "     ${GREEN}sudo systemctl start ${SERVICE_NAME}${NC}"
    echo ""
    echo "Other useful commands:"
    echo ""
    echo -e "  Check status:   ${BLUE}sudo systemctl status ${SERVICE_NAME}${NC}"
    echo -e "  View logs:      ${BLUE}journalctl -u ${SERVICE_NAME} -f${NC}"
    echo -e "  Show config:    ${BLUE}${VENV_DIR}/bin/moonraker-owl --config ${CONFIG_FILE} show-config${NC}"
    echo ""
    echo "Configuration file: ${CONFIG_FILE}"
    echo "Log file: ${LOG_DIR}/${APP_NAME}.log"
    echo ""
}

show_help() {
    echo "moonraker-owl Installation Script"
    echo ""
    echo "Usage: ./install.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --help, -h     Show this help message"
    echo ""
    echo "This script installs moonraker-owl on a Klipper/Moonraker system."
    echo "It creates a virtual environment, installs dependencies, and sets up"
    echo "the systemd service."
    echo ""
    echo "After installation, run 'moonraker-owl link' to connect to Owl Cloud."
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
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

    # Run installation steps
    check_prerequisites
    create_directories
    create_virtualenv
    install_package
    create_config
    install_service
    print_next_steps
}

main "$@"
