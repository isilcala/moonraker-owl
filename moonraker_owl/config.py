"""Configuration loader for moonraker-owl."""

from __future__ import annotations

import copy
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]

import tomli_w

from . import constants

LOGGER = logging.getLogger(__name__)



DEFAULT_TELEMETRY_FIELDS = [
    # Subscribe to entire print_stats object (null) to ensure we receive all state changes.
    # Previously we subscribed to specific fields (state, message, filename, info) but
    # Moonraker's optimization may omit unchanged fields. Using null subscription ensures
    # we get all updates including state transitions.
    "print_stats",
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
    # Exclude object support (ADR-0016) - provides object definitions, exclusions, and current object
    "exclude_object",
]


DEFAULT_TELEMETRY_EXCLUDE_FIELDS: list[str] = []


# Default sensors idle interval: 30 seconds between sensor publishes.
DEFAULT_SENSORS_INTERVAL_SECONDS: float = 30.0


@dataclass(slots=True)
class TelemetryCadenceConfig:
    status_heartbeat_seconds: int = 60
    status_idle_interval_seconds: float = 60.0
    status_active_interval_seconds: float = 15.0
    status_min_interval_seconds: float = 2.0  # Defensive rate cap for status channel
    sensors_force_publish_seconds: float = 300.0  # Maximum seconds without sensor publish before forcing one
    events_max_per_second: int = 1
    events_max_per_minute: int = 20
    thumbnail_fetch_timeout_ms: int = 5000  # Timeout for fetching GCode metadata and moonrakerJobId from History API
    timelapse_poll_interval_seconds: float = 5.0


@dataclass(slots=True)
class CloudConfig:
    base_url: str = constants.DEFAULT_LINK_BASE_URL
    broker_host: str = constants.DEFAULT_BROKER_HOST
    broker_port: int = 8883
    broker_use_tls: bool = True  # Enable TLS for MQTT connection (default: True for port 8883)
    device_id: Optional[str] = None
    tenant_id: Optional[str] = None
    printer_id: Optional[str] = None
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


# Sensors that are always reported regardless of allow/deny lists.
# These are the minimum set required for basic printer monitoring.
CORE_SENSORS: frozenset[str] = frozenset({
    "extruder",
    "extruder1",
    "extruder2",
    "extruder3",
    "heater_bed",
    "fan",
})


@dataclass(slots=True)
class TelemetryConfig:
    sensors_interval_seconds: float = DEFAULT_SENSORS_INTERVAL_SECONDS
    include_raw_payload: bool = False  # Set to True to include raw Moonraker payload (adds ~450 bytes per message)
    include_fields: List[str] = field(
        default_factory=lambda: list(DEFAULT_TELEMETRY_FIELDS)
    )
    exclude_fields: List[str] = field(
        default_factory=lambda: list(DEFAULT_TELEMETRY_EXCLUDE_FIELDS)
    )
    sensor_allowlist: List[str] = field(default_factory=list)
    """Explicit list of sensor object names to report (in addition to core sensors).
    When non-empty, only core sensors + these sensors are reported.
    Example: ['temperature_sensor chamber', 'heater_generic chamber_heater']
    """
    sensor_denylist: List[str] = field(default_factory=list)
    """Sensor object names to exclude from reporting.
    Applied after allowlist. Can exclude core sensors if desired.
    Example: ['temperature_sensor mcu_temp']
    """
    max_custom_sensors: int = 0
    """Maximum non-core (custom) sensors allowed. Tier-assigned: Free=0, Plus=3, Pro=8."""
    max_sensor_count: int = 6
    """Hard cap on total sensor count (core + custom). Tier-assigned: Free=6, Plus=9, Pro=14."""


@dataclass(slots=True)
class CommandConfig:
    ack_timeout_seconds: float = 30.0


@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"
    path: Optional[Path] = constants.DEFAULT_LOG_PATH
    log_network: bool = False
    # Log rotation settings (RotatingFileHandler)
    max_bytes: int = 10 * 1024 * 1024  # 10 MB per file
    backup_count: int = 3  # Keep 3 backup files (~40 MB total max)


@dataclass(slots=True)
class ResilienceConfig:
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    reconnect_jitter_ratio: float = 0.5
    reconnect_perpetual_seconds: float = 900.0
    session_expiry_seconds: int = 86400
    buffer_window_seconds: float = 60.0
    moonraker_breaker_threshold: int = 5
    heartbeat_interval_seconds: int = 30
    health_enabled: bool = False
    health_host: str = "127.0.0.1"
    health_port: int = 0


@dataclass(slots=True)
class CompressionConfig:
    """Configuration for telemetry compression.
    
    Compression can be disabled globally via the OWL_COMPRESSION_DISABLED
    environment variable (set to 'true', '1', or 'yes').
    """
    enabled: bool = True
    channels: List[str] = field(default_factory=lambda: ["sensors", "objects"])
    min_size_bytes: int = 1024

    @classmethod
    def is_disabled_by_env(cls) -> bool:
        """Check if compression is disabled via environment variable."""
        return os.environ.get("OWL_COMPRESSION_DISABLED", "").lower() in ("true", "1", "yes")


@dataclass(slots=True)
class MetadataConfig:
    """Configuration for device metadata reporting (ADR-0032)."""

    enabled: bool = True
    """Whether metadata reporting is enabled."""

    refresh_interval_hours: float = 24.0
    """Interval between metadata reports in hours."""

    initial_retry_delay_seconds: float = 30.0
    """Initial delay before retrying a failed report."""

    max_retry_delay_seconds: float = 600.0
    """Maximum delay between retry attempts (10 minutes)."""

    request_timeout_seconds: float = 30.0
    """Timeout for metadata upload requests."""


@dataclass(slots=True)
class CameraConfig:
    """Configuration for camera capture functionality.

    Note: The ``enabled`` field has been removed — camera capture enablement
    is now controlled by the cloud-side CaptureConfig entity, not the agent
    config pipeline.  The agent always initialises the camera client when a
    snapshot URL is available; the cloud decides whether to schedule captures.
    """

    snapshot_url: str = "auto"
    """URL for webcam snapshot capture. Use 'auto' for automatic discovery via Moonraker API."""

    camera_name: str = "auto"
    """Webcam name to use when snapshot_url is 'auto'. Use 'auto' for first available."""

    capture_timeout_seconds: float = 5.0
    """Timeout for camera capture requests."""

    max_retries: int = 2
    """Maximum number of retry attempts for failed captures."""

    preprocess_enabled: bool = True
    """Whether to preprocess (resize/compress) images before upload."""

    preprocess_target_width: int = 1280
    """Target width for image resizing. Images wider than this will be resized."""

    preprocess_jpeg_quality: int = 85
    """JPEG quality for compressed images (1-100)."""


@dataclass(slots=True)
class OwlConfig:
    cloud: CloudConfig
    moonraker: MoonrakerConfig
    telemetry: TelemetryConfig
    telemetry_cadence: TelemetryCadenceConfig
    commands: CommandConfig
    logging: LoggingConfig
    resilience: ResilienceConfig
    compression: CompressionConfig
    camera: CameraConfig
    metadata: MetadataConfig
    raw: Dict[str, Any]
    path: Path

    @property
    def include_fields(self) -> List[str]:
        return list(self.telemetry.include_fields)

    @property
    def exclude_fields(self) -> List[str]:
        return list(self.telemetry.exclude_fields)


def _get(section: Dict[str, Any], key: str, default: Any) -> Any:
    """Get a value from a TOML section dict, coercing numeric types as needed."""
    value = section.get(key, default)
    if isinstance(default, float) and isinstance(value, int):
        return float(value)
    if isinstance(default, int) and isinstance(value, float) and value == int(value):
        return int(value)
    return value


def load_config(path: Optional[Path] = None) -> OwlConfig:
    """Load configuration from a TOML file, applying defaults where necessary."""

    config_path = path or constants.DEFAULT_CONFIG_PATH
    raw: Dict[str, Any] = {}

    if config_path.exists():
        with open(config_path, "rb") as f:
            raw = tomllib.load(f)

    cloud_raw = raw.get("cloud", {})

    broker_host_value = str(cloud_raw.get("broker_host", constants.DEFAULT_BROKER_HOST))
    broker_port_value = int(cloud_raw.get("broker_port", 8883))

    # Support "host:port" shorthand in broker_host
    if ":" in broker_host_value:
        host_part, port_part = broker_host_value.rsplit(":", 1)
        try:
            parsed_port = int(port_part)
        except ValueError:
            pass
        else:
            broker_host_value = host_part
            broker_port_value = parsed_port

    # Default TLS to True for port 8883, False for 1883
    default_use_tls = broker_port_value == 8883

    cloud = CloudConfig(
        base_url=str(cloud_raw.get("base_url", constants.DEFAULT_LINK_BASE_URL)),
        broker_host=broker_host_value,
        broker_port=broker_port_value,
        broker_use_tls=bool(cloud_raw.get("broker_use_tls", default_use_tls)),
        device_id=cloud_raw.get("device_id") or None,
        tenant_id=cloud_raw.get("tenant_id") or None,
        printer_id=cloud_raw.get("printer_id") or None,
        username=cloud_raw.get("username") or None,
        password=cloud_raw.get("password") or None,
        device_private_key=cloud_raw.get("device_private_key") or None,
    )

    mr_raw = raw.get("moonraker", {})
    moonraker = MoonrakerConfig(
        url=str(mr_raw.get("url", f"http://{constants.DEFAULT_MOONRAKER_HOST}:{constants.DEFAULT_MOONRAKER_PORT}")),
        transport=str(mr_raw.get("transport", "websocket")),
        api_key=mr_raw.get("api_key") or None,
    )

    tel_raw = raw.get("telemetry", {})

    telemetry = TelemetryConfig(
        sensors_interval_seconds=float(tel_raw.get("sensors_interval_seconds", DEFAULT_SENSORS_INTERVAL_SECONDS)),
        include_raw_payload=bool(tel_raw.get("include_raw_payload", False)),
        include_fields=list(tel_raw.get("include_fields", DEFAULT_TELEMETRY_FIELDS)),
        exclude_fields=list(tel_raw.get("exclude_fields", DEFAULT_TELEMETRY_EXCLUDE_FIELDS)),
        sensor_allowlist=list(tel_raw.get("sensor_allowlist", [])),
        sensor_denylist=list(tel_raw.get("sensor_denylist", [])),
    )

    cmd_raw = raw.get("commands", {})
    commands = CommandConfig(
        ack_timeout_seconds=float(_get(cmd_raw, "ack_timeout_seconds", 30.0)),
    )

    cad_raw = raw.get("telemetry_cadence", {})
    cad_defaults = TelemetryCadenceConfig()
    telemetry_cadence = TelemetryCadenceConfig(
        status_heartbeat_seconds=int(_get(cad_raw, "status_heartbeat_seconds", cad_defaults.status_heartbeat_seconds)),
        status_idle_interval_seconds=float(_get(cad_raw, "status_idle_interval_seconds", cad_defaults.status_idle_interval_seconds)),
        status_active_interval_seconds=float(_get(cad_raw, "status_active_interval_seconds", cad_defaults.status_active_interval_seconds)),
        sensors_force_publish_seconds=float(_get(cad_raw, "sensors_force_publish_seconds", cad_defaults.sensors_force_publish_seconds)),
        events_max_per_second=int(_get(cad_raw, "events_max_per_second", cad_defaults.events_max_per_second)),
        events_max_per_minute=int(_get(cad_raw, "events_max_per_minute", cad_defaults.events_max_per_minute)),
        thumbnail_fetch_timeout_ms=int(_get(cad_raw, "thumbnail_fetch_timeout_ms", cad_defaults.thumbnail_fetch_timeout_ms)),
        timelapse_poll_interval_seconds=float(_get(cad_raw, "timelapse_poll_interval_seconds", cad_defaults.timelapse_poll_interval_seconds)),
    )

    log_raw = raw.get("logging", {})
    log_defaults = LoggingConfig()
    log_path_str = log_raw.get("path", str(constants.DEFAULT_LOG_PATH))
    logging_config = LoggingConfig(
        level=str(log_raw.get("level", "INFO")),
        path=Path(str(log_path_str)).expanduser() if log_path_str else log_defaults.path,
        log_network=bool(log_raw.get("log_network", False)),
        max_bytes=int(_get(log_raw, "max_bytes", log_defaults.max_bytes)),
        backup_count=int(_get(log_raw, "backup_count", log_defaults.backup_count)),
    )

    res_raw = raw.get("resilience", {})
    resilience = ResilienceConfig(
        reconnect_initial_seconds=float(_get(res_raw, "reconnect_initial_seconds", 1.0)),
        reconnect_max_seconds=float(_get(res_raw, "reconnect_max_seconds", 30.0)),
        reconnect_jitter_ratio=max(0.0, min(1.0, float(_get(res_raw, "reconnect_jitter_ratio", 0.5)))),
        reconnect_perpetual_seconds=max(60.0, float(_get(res_raw, "reconnect_perpetual_seconds", 900.0))),
        session_expiry_seconds=max(0, int(_get(res_raw, "session_expiry_seconds", 86400))),
        buffer_window_seconds=max(0.0, float(_get(res_raw, "buffer_window_seconds", 60.0))),
        moonraker_breaker_threshold=max(1, int(_get(res_raw, "moonraker_breaker_threshold", 5))),
        heartbeat_interval_seconds=max(1, int(_get(res_raw, "heartbeat_interval_seconds", 30))),
        health_enabled=bool(res_raw.get("health_enabled", False)),
        health_host=str(res_raw.get("health_host", "127.0.0.1")),
        health_port=int(_get(res_raw, "health_port", 0)),
    )

    # Compression config - environment variable takes precedence
    comp_defaults = CompressionConfig()
    if CompressionConfig.is_disabled_by_env():
        compression = CompressionConfig(enabled=False)
    else:
        comp_raw = raw.get("compression", {})
        compression = CompressionConfig(
            enabled=bool(comp_raw.get("enabled", comp_defaults.enabled)),
            channels=list(comp_raw.get("channels", comp_defaults.channels)),
            min_size_bytes=int(_get(comp_raw, "min_size_bytes", comp_defaults.min_size_bytes)),
        )

    # Camera config
    cam_raw = raw.get("camera", {})
    cam_defaults = CameraConfig()
    camera = CameraConfig(
        snapshot_url=str(cam_raw.get("snapshot_url", cam_defaults.snapshot_url)),
        camera_name=str(cam_raw.get("camera_name", cam_defaults.camera_name)),
        capture_timeout_seconds=float(_get(cam_raw, "capture_timeout_seconds", cam_defaults.capture_timeout_seconds)),
        max_retries=int(_get(cam_raw, "max_retries", cam_defaults.max_retries)),
        preprocess_enabled=bool(cam_raw.get("preprocess_enabled", cam_defaults.preprocess_enabled)),
        preprocess_target_width=int(_get(cam_raw, "preprocess_target_width", cam_defaults.preprocess_target_width)),
        preprocess_jpeg_quality=int(_get(cam_raw, "preprocess_jpeg_quality", cam_defaults.preprocess_jpeg_quality)),
    )

    # Metadata config
    meta_raw = raw.get("metadata", {})
    meta_defaults = MetadataConfig()
    metadata = MetadataConfig(
        enabled=bool(meta_raw.get("enabled", meta_defaults.enabled)),
        refresh_interval_hours=float(_get(meta_raw, "refresh_interval_hours", meta_defaults.refresh_interval_hours)),
        initial_retry_delay_seconds=float(_get(meta_raw, "initial_retry_delay_seconds", meta_defaults.initial_retry_delay_seconds)),
        max_retry_delay_seconds=float(_get(meta_raw, "max_retry_delay_seconds", meta_defaults.max_retry_delay_seconds)),
        request_timeout_seconds=float(_get(meta_raw, "request_timeout_seconds", meta_defaults.request_timeout_seconds)),
    )

    return OwlConfig(
        cloud=cloud,
        moonraker=moonraker,
        telemetry=telemetry,
        telemetry_cadence=telemetry_cadence,
        commands=commands,
        logging=logging_config,
        resilience=resilience,
        compression=compression,
        camera=camera,
        metadata=metadata,
        raw=copy.deepcopy(raw),
        path=config_path,
    )


def save_config(config: OwlConfig) -> None:
    """Persist the current configuration to disk as TOML."""

    config_path = config.path
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("wb") as stream:
        tomli_w.dump(config.raw, stream)


def load_credentials(
    path: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    """Load device credentials from a JSON file.

    Returns:
        Parsed credential dict, or None if the file does not exist.
    """
    target = path or constants.DEFAULT_CREDENTIALS_PATH

    if not target.exists():
        return None

    with target.open("r", encoding="utf-8") as f:
        return json.load(f)


def merge_credentials(config: OwlConfig, credentials_path: Optional[Path] = None) -> None:
    """Load credentials from JSON and merge into the config's CloudConfig.

    Credential fields from the JSON file take precedence over TOML values.
    This allows TOML to provide fallback values while the authoritative
    credentials live in the secure JSON file.
    """
    creds = load_credentials(credentials_path)
    if creds is None:
        return

    cloud = config.cloud
    if creds.get("deviceId"):
        cloud.device_id = str(creds["deviceId"])
    if creds.get("tenantId"):
        cloud.tenant_id = str(creds["tenantId"])
    if creds.get("printerId"):
        cloud.printer_id = str(creds["printerId"])
    if creds.get("devicePrivateKey"):
        cloud.device_private_key = str(creds["devicePrivateKey"])

    # Derive MQTT username from credentials
    if cloud.tenant_id and cloud.device_id:
        cloud.username = f"{cloud.tenant_id}:{cloud.device_id}"
    elif cloud.device_id:
        cloud.username = cloud.device_id


