"""Constants used across the moonraker-owl package."""

from __future__ import annotations

from pathlib import Path

APP_NAME = "moonraker-owl"
DEFAULT_CONFIG_FILENAME = f"{APP_NAME}.cfg"
DEFAULT_CONFIG_PATH = Path.home() / "printer_data" / "config" / DEFAULT_CONFIG_FILENAME
DEFAULT_CREDENTIALS_PATH = Path.home() / ".owl" / "device.json"
DEFAULT_VENV_PATH = Path.home() / f"{APP_NAME}-env"

DEFAULT_LOG_PATH = Path.home() / "printer_data" / "logs" / f"{APP_NAME}.log"

DEFAULT_MOONRAKER_HOST = "192.168.50.231"
DEFAULT_MOONRAKER_PORT = 7125

DEFAULT_BROKER_HOST = "localhost:61198"
DEFAULT_LINK_BASE_URL = "http://localhost:5024"
