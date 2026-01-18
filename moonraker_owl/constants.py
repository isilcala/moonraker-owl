"""Constants used across the moonraker-owl package."""

from __future__ import annotations

from pathlib import Path

APP_NAME = "moonraker-owl"
DEFAULT_CONFIG_FILENAME = f"{APP_NAME}.cfg"
DEFAULT_CONFIG_PATH = Path.home() / "printer_data" / "config" / DEFAULT_CONFIG_FILENAME
DEFAULT_CREDENTIALS_PATH = Path.home() / ".owl" / "device.json"
DEFAULT_VENV_PATH = Path.home() / f"{APP_NAME}-env"

DEFAULT_LOG_PATH = Path.home() / "printer_data" / "logs" / f"{APP_NAME}.log"

DEFAULT_MOONRAKER_HOST = "127.0.0.1"
DEFAULT_MOONRAKER_PORT = 7125

# Staging environment defaults
DEFAULT_BROKER_HOST = "mqtt.owl.elencala.com"
DEFAULT_LINK_BASE_URL = "https://owl.elencala.com"

# Legacy: DEVICE_TOKEN_MQTT_PROPERTY_NAME = "x-device-token" - No longer used with JWT auth
