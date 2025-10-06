"""Configuration loader for moonraker-owl."""

from __future__ import annotations

from configparser import ConfigParser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional

from . import constants


@dataclass(slots=True)
class CloudConfig:
    base_url: str = constants.DEFAULT_LINK_BASE_URL
    broker_host: str = constants.DEFAULT_BROKER_HOST
    broker_port: int = 8883
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass(slots=True)
class MoonrakerConfig:
    url: str = (
        f"http://{constants.DEFAULT_MOONRAKER_HOST}:{constants.DEFAULT_MOONRAKER_PORT}"
    )
    transport: str = "websocket"
    api_key: Optional[str] = None


@dataclass(slots=True)
class TelemetryConfig:
    rate_hz: float = 1.0
    include_fields: List[str] = field(
        default_factory=lambda: ["status", "progress", "telemetry"]
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
    health_enabled: bool = False
    health_host: str = "127.0.0.1"
    health_port: int = 0


@dataclass(slots=True)
class OwlConfig:
    cloud: CloudConfig
    moonraker: MoonrakerConfig
    telemetry: TelemetryConfig
    commands: CommandConfig
    logging: LoggingConfig
    resilience: ResilienceConfig
    raw: ConfigParser
    path: Path

    @property
    def include_fields(self) -> List[str]:
        return list(self.telemetry.include_fields)


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
                "rate_hz": "1.0",
                "include_fields": ",".join(["status", "progress", "telemetry"]),
            },
            "commands": {
                "ack_timeout_seconds": "30.0",
            },
            "logging": {
                "level": "INFO",
                "path": str(constants.DEFAULT_LOG_PATH),
                "log_network": "false",
            },
            "resilience": {
                "reconnect_initial_seconds": "1.0",
                "reconnect_max_seconds": "30.0",
                "health_enabled": "false",
                "health_host": "127.0.0.1",
                "health_port": "0",
            },
        }
    )

    if config_path.exists():
        parser.read(config_path)

    cloud = CloudConfig(
        base_url=parser.get("cloud", "base_url"),
        broker_host=parser.get("cloud", "broker_host"),
        broker_port=parser.getint("cloud", "broker_port", fallback=8883),
        username=parser.get("cloud", "username", fallback=None),
        password=parser.get("cloud", "password", fallback=None),
    )

    moonraker = MoonrakerConfig(
        url=parser.get("moonraker", "url"),
        transport=parser.get("moonraker", "transport"),
        api_key=parser.get("moonraker", "api_key", fallback=None),
    )

    telemetry = TelemetryConfig(
        rate_hz=parser.getfloat("telemetry", "rate_hz", fallback=1.0),
        include_fields=_parse_list(
            parser.get(
                "telemetry", "include_fields", fallback="status,progress,telemetry"
            ),
            default=["status", "progress", "telemetry"],
        ),
    )

    commands = CommandConfig(
        ack_timeout_seconds=parser.getfloat(
            "commands", "ack_timeout_seconds", fallback=30.0
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
