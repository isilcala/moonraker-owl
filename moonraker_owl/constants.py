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


class MQTTTopics:
    """Central registry for the MQTT topic taxonomy.

    All device-scoped topics share the ``owl/printers/{deviceId}`` base. Keeping
    the templates in one place avoids drift between publishers, subscribers, and
    the command/ack paths (P1-12). Use :meth:`for_device` to bind a device id,
    then read the per-channel properties.
    """

    # Global (non device-scoped) topics.
    BROADCAST_CONFIG_UPDATE = "owl/broadcasts/config-update"

    # Templates ({device} substituted by ``resolve``/``for_device``).
    BASE = "owl/printers/{device}"
    STATUS = "owl/printers/{device}/status"
    SENSORS = "owl/printers/{device}/sensors"
    EVENTS = "owl/printers/{device}/events"
    EVENTS_DISCONNECT = "owl/printers/{device}/events/disconnect"
    OBJECTS = "owl/printers/{device}/objects"
    HEALTH = "owl/printers/{device}/health"
    CONFIG_NOTIFY = "owl/printers/{device}/config/notify"
    COMMANDS_PREFIX = "owl/printers/{device}/commands"
    ACKS_PREFIX = "owl/printers/{device}/acks"

    @staticmethod
    def resolve(template: str, device_id: str) -> str:
        """Render a topic ``template`` for a specific ``device_id``."""
        return template.format(device=device_id)

    @classmethod
    def for_device(cls, device_id: str) -> "DeviceTopics":
        """Return a :class:`DeviceTopics` bound to ``device_id``."""
        return DeviceTopics(device_id)


class DeviceTopics:
    """Resolved, device-scoped MQTT topics for a single printer."""

    __slots__ = ("device_id",)

    def __init__(self, device_id: str) -> None:
        self.device_id = device_id

    @property
    def base(self) -> str:
        return MQTTTopics.resolve(MQTTTopics.BASE, self.device_id)

    @property
    def status(self) -> str:
        return MQTTTopics.resolve(MQTTTopics.STATUS, self.device_id)

    @property
    def sensors(self) -> str:
        return MQTTTopics.resolve(MQTTTopics.SENSORS, self.device_id)

    @property
    def events(self) -> str:
        return MQTTTopics.resolve(MQTTTopics.EVENTS, self.device_id)

    @property
    def events_disconnect(self) -> str:
        return MQTTTopics.resolve(MQTTTopics.EVENTS_DISCONNECT, self.device_id)

    @property
    def objects(self) -> str:
        return MQTTTopics.resolve(MQTTTopics.OBJECTS, self.device_id)

    @property
    def health(self) -> str:
        return MQTTTopics.resolve(MQTTTopics.HEALTH, self.device_id)

    @property
    def config_notify(self) -> str:
        return MQTTTopics.resolve(MQTTTopics.CONFIG_NOTIFY, self.device_id)

    @property
    def commands_prefix(self) -> str:
        return MQTTTopics.resolve(MQTTTopics.COMMANDS_PREFIX, self.device_id)

    @property
    def acks_prefix(self) -> str:
        return MQTTTopics.resolve(MQTTTopics.ACKS_PREFIX, self.device_id)

    def ack(self, safe_command_name: str) -> str:
        """Return the ack topic for a (sanitized) command name."""
        return f"{self.acks_prefix}/{safe_command_name}"


