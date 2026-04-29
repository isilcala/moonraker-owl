"""Constants used across the moonraker-owl package."""

from __future__ import annotations

from pathlib import Path

APP_NAME = "moonraker-owl"
DEFAULT_CONFIG_FILENAME = f"{APP_NAME}.toml"
DEFAULT_CONFIG_PATH = Path.home() / "printer_data" / "config" / DEFAULT_CONFIG_FILENAME
DEFAULT_DATA_DIR = Path.home() / f".{APP_NAME}"
DEFAULT_CREDENTIALS_PATH = DEFAULT_DATA_DIR / "credentials.json"
DEFAULT_CLOUD_CONFIG_PATH = DEFAULT_DATA_DIR / "cloud-config.json"
DEFAULT_IDEMPOTENCY_PATH = DEFAULT_DATA_DIR / "idempotency.json"
DEFAULT_VENV_PATH = Path.home() / f"{APP_NAME}-env"

DEFAULT_LOG_PATH = Path.home() / "printer_data" / "logs" / f"{APP_NAME}.log"

DEFAULT_MOONRAKER_HOST = "127.0.0.1"
DEFAULT_MOONRAKER_PORT = 7125

# Cloud endpoints intentionally have no shipped defaults: a fresh install
# without an explicit cloud.base_url / cloud.broker_host MUST fail-fast
# rather than silently connect to whatever the developer last targeted
# (audit A-10, OQ-3). Operators must set these in
# ~/printer_data/config/moonraker-owl.toml.
DEFAULT_BROKER_HOST = ""
DEFAULT_LINK_BASE_URL = ""

