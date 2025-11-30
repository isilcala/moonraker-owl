"""Configuration loader for moonraker-owl."""

from __future__ import annotations

from configparser import ConfigParser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional

from . import constants

DEFAULT_TELEMETRY_FIELDS = [
    "print_stats.state",
    "print_stats.message",
    "print_stats.filename",
    "print_stats.info",
    "webhooks.state",
    "webhooks.state_message",
    "gcode_move.speed_factor",
    "gcode_move.extrude_factor",
    "history",
    "gcode_macro _OBICO_LAYER_CHANGE",
    "fan.speed",
    "toolhead",
    "virtual_sdcard",
    "display_status",
    "idle_timeout",
    "gcode_macro TIMELAPSE_TAKE_FRAME",
    "extruder",
    "heater_bed",
]


DEFAULT_TELEMETRY_EXCLUDE_FIELDS: list[str] = []


DEFAULT_TELEMETRY_RATE_HZ: float = 1 / 30


@dataclass(slots=True)
class TelemetryCadenceConfig:
    status_heartbeat_seconds: int = 60
    status_idle_interval_seconds: float = 60.0
    status_active_interval_seconds: float = 15.0
    sensors_force_publish_seconds: float = 300.0  # Maximum seconds without sensor publish before forcing one
    events_max_per_second: int = 1
    events_max_per_minute: int = 20


@dataclass(slots=True)
class CloudConfig:
    base_url: str = constants.DEFAULT_LINK_BASE_URL
    broker_host: str = constants.DEFAULT_BROKER_HOST
    broker_port: int = 8883
    username: Optional[str] = None
    password: Optional[str] = None
    device_private_key: Optional[str] = None  # Base64-encoded Ed25519 private key for JWT authentication


@dataclass(slots=True)
class MoonrakerConfig:
    url: str = (
        f"http://{constants.DEFAULT_MOONRAKER_HOST}:{constants.DEFAULT_MOONRAKER_PORT}"
    )
    transport: str = "websocket"
    api_key: Optional[str] = None


@dataclass(slots=True)
class TelemetryConfig:
    rate_hz: float = DEFAULT_TELEMETRY_RATE_HZ
    include_raw_payload: bool = False  # Set to True to include raw Moonraker payload (adds ~450 bytes per message)
    include_fields: List[str] = field(
        default_factory=lambda: list(DEFAULT_TELEMETRY_FIELDS)
    )
    exclude_fields: List[str] = field(
        default_factory=lambda: list(DEFAULT_TELEMETRY_EXCLUDE_FIELDS)
    )


@dataclass(slots=True)
class CommandConfig:
    ack_timeout_seconds: float = 30.0


@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"
    path: Optional[Path] = constants.DEFAULT_LOG_PATH
    log_network: bool = False


@dataclass(slots=True)
class ResilienceConfig:
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    reconnect_jitter_ratio: float = 0.5
    session_expiry_seconds: int = 86400
    buffer_window_seconds: float = 60.0
    moonraker_breaker_threshold: int = 5
    heartbeat_interval_seconds: int = 30
    health_enabled: bool = False
    health_host: str = "127.0.0.1"
    health_port: int = 0


@dataclass(slots=True)
class OwlConfig:
    cloud: CloudConfig
    moonraker: MoonrakerConfig
    telemetry: TelemetryConfig
    telemetry_cadence: TelemetryCadenceConfig
    commands: CommandConfig
    logging: LoggingConfig
    resilience: ResilienceConfig
    raw: ConfigParser
    path: Path

    @property
    def include_fields(self) -> List[str]:
        return list(self.telemetry.include_fields)

    @property
    def exclude_fields(self) -> List[str]:
        return list(self.telemetry.exclude_fields)


def _parse_list(value: str, *, default: Iterable[str]) -> List[str]:
    if not value:
        return list(default)
    return [item.strip() for item in value.split(",") if item.strip()]


def load_config(path: Optional[Path] = None) -> OwlConfig:
    """Load configuration from disk, applying defaults where necessary."""

    config_path = path or constants.DEFAULT_CONFIG_PATH
    parser = ConfigParser()
    parser.read_dict(
        {
            "cloud": {
                "base_url": constants.DEFAULT_LINK_BASE_URL,
                "broker_host": constants.DEFAULT_BROKER_HOST,
                "broker_port": str(8883),
            },
            "moonraker": {
                "url": f"http://{constants.DEFAULT_MOONRAKER_HOST}:{constants.DEFAULT_MOONRAKER_PORT}",
                "transport": "websocket",
            },
            "telemetry": {
                "rate_hz": str(DEFAULT_TELEMETRY_RATE_HZ),
                "include_fields": ",".join(DEFAULT_TELEMETRY_FIELDS),
                "exclude_fields": ",".join(DEFAULT_TELEMETRY_EXCLUDE_FIELDS),
            },
            "commands": {
                "ack_timeout_seconds": "30.0",
            },
            "telemetry_cadence": {
                "status_heartbeat_seconds": "60",
                "status_idle_interval_seconds": "60",
                "status_active_interval_seconds": "15",
                "sensors_force_publish_seconds": "300",
                "events_max_per_second": "1",
                "events_max_per_minute": "20",
            },
            "logging": {
                "level": "INFO",
                "path": str(constants.DEFAULT_LOG_PATH),
                "log_network": "false",
            },
            "resilience": {
                "reconnect_initial_seconds": "1.0",
                "reconnect_max_seconds": "30.0",
                "reconnect_jitter_ratio": "0.5",
                "session_expiry_seconds": "86400",
                "buffer_window_seconds": "60.0",
                "moonraker_breaker_threshold": "5",
                "heartbeat_interval_seconds": "30",
                "health_enabled": "false",
                "health_host": "127.0.0.1",
                "health_port": "0",
            },
        }
    )

    if config_path.exists():
        parser.read(config_path)

    broker_host_value = parser.get("cloud", "broker_host")
    broker_port_value = parser.getint("cloud", "broker_port", fallback=8883)

    if ":" in broker_host_value:
        host_part, port_part = broker_host_value.rsplit(":", 1)
        try:
            parsed_port = int(port_part)
        except ValueError:
            pass
        else:
            broker_host_value = host_part
            broker_port_value = parsed_port
            parser.set("cloud", "broker_host", host_part)
            parser.set("cloud", "broker_port", str(parsed_port))

    cloud = CloudConfig(
        base_url=parser.get("cloud", "base_url"),
        broker_host=broker_host_value,
        broker_port=broker_port_value,
        username=parser.get("cloud", "username", fallback=None),
        password=parser.get("cloud", "password", fallback=None),
        device_private_key=parser.get("cloud", "device_private_key", fallback=None),
    )

    moonraker = MoonrakerConfig(
        url=parser.get("moonraker", "url"),
        transport=parser.get("moonraker", "transport"),
        api_key=parser.get("moonraker", "api_key", fallback=None),
    )

    default_rate_hz = TelemetryConfig().rate_hz
    try:
        rate_hz_value = parser.getfloat("telemetry", "rate_hz", fallback=default_rate_hz)
    except ValueError:
        rate_hz_value = default_rate_hz

    telemetry = TelemetryConfig(
        rate_hz=rate_hz_value,
        include_raw_payload=parser.getboolean(
            "telemetry", "include_raw_payload", fallback=False
        ),
        include_fields=_parse_list(
            parser.get(
                "telemetry",
                "include_fields",
                fallback=",".join(DEFAULT_TELEMETRY_FIELDS),
            ),
            default=DEFAULT_TELEMETRY_FIELDS,
        ),
        exclude_fields=_parse_list(
            parser.get(
                "telemetry",
                "exclude_fields",
                fallback=",".join(DEFAULT_TELEMETRY_EXCLUDE_FIELDS),
            ),
            default=DEFAULT_TELEMETRY_EXCLUDE_FIELDS,
        ),
    )

    commands = CommandConfig(
        ack_timeout_seconds=parser.getfloat(
            "commands", "ack_timeout_seconds", fallback=30.0
        ),
    )

    cadence_defaults = TelemetryCadenceConfig()

    telemetry_cadence = TelemetryCadenceConfig(
        status_heartbeat_seconds=parser.getint(
            "telemetry_cadence",
            "status_heartbeat_seconds",
            fallback=cadence_defaults.status_heartbeat_seconds,
        ),
        status_idle_interval_seconds=parser.getfloat(
            "telemetry_cadence",
            "status_idle_interval_seconds",
            fallback=cadence_defaults.status_idle_interval_seconds,
        ),
        status_active_interval_seconds=parser.getfloat(
            "telemetry_cadence",
            "status_active_interval_seconds",
            fallback=cadence_defaults.status_active_interval_seconds,
        ),
        sensors_force_publish_seconds=parser.getfloat(
            "telemetry_cadence",
            "sensors_force_publish_seconds",
            fallback=cadence_defaults.sensors_force_publish_seconds,
        ),
        events_max_per_second=parser.getint(
            "telemetry_cadence",
            "events_max_per_second",
            fallback=cadence_defaults.events_max_per_second,
        ),
        events_max_per_minute=parser.getint(
            "telemetry_cadence",
            "events_max_per_minute",
            fallback=cadence_defaults.events_max_per_minute,
        ),
    )

    logging_config = LoggingConfig(
        level=parser.get("logging", "level", fallback="INFO"),
        path=Path(
            parser.get("logging", "path", fallback=str(constants.DEFAULT_LOG_PATH))
        ).expanduser(),
        log_network=parser.getboolean("logging", "log_network", fallback=False),
    )

    resilience = ResilienceConfig(
        reconnect_initial_seconds=parser.getfloat(
            "resilience", "reconnect_initial_seconds", fallback=1.0
        ),
        reconnect_max_seconds=parser.getfloat(
            "resilience", "reconnect_max_seconds", fallback=30.0
        ),
        reconnect_jitter_ratio=max(
            0.0,
            min(
                1.0,
                parser.getfloat("resilience", "reconnect_jitter_ratio", fallback=0.5),
            ),
        ),
        session_expiry_seconds=max(
            0,
            parser.getint("resilience", "session_expiry_seconds", fallback=86400),
        ),
        buffer_window_seconds=max(
            0.0,
            parser.getfloat("resilience", "buffer_window_seconds", fallback=60.0),
        ),
        moonraker_breaker_threshold=max(
            1,
            parser.getint("resilience", "moonraker_breaker_threshold", fallback=5),
        ),
        heartbeat_interval_seconds=max(
            1,
            parser.getint("resilience", "heartbeat_interval_seconds", fallback=30),
        ),
        health_enabled=parser.getboolean(
            "resilience", "health_enabled", fallback=False
        ),
        health_host=parser.get("resilience", "health_host", fallback="127.0.0.1"),
        health_port=parser.getint("resilience", "health_port", fallback=0),
    )

    return OwlConfig(
        cloud=cloud,
        moonraker=moonraker,
        telemetry=telemetry,
    telemetry_cadence=telemetry_cadence,
        commands=commands,
        logging=logging_config,
        resilience=resilience,
        raw=parser,
        path=config_path,
    )


def save_config(config: OwlConfig) -> None:
    """Persist the current configuration to disk."""

    config_path = config.path
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8") as stream:
        config.raw.write(stream)
