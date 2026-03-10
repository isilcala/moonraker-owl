from pathlib import Path

from moonraker_owl.config import load_config, DEFAULT_SENSORS_INTERVAL_SECONDS, CORE_SENSORS


def test_load_config_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config = load_config(config_path)

    assert config.cloud.broker_host
    assert config.moonraker.url.startswith("http")
    assert config.telemetry.sensors_interval_seconds == DEFAULT_SENSORS_INTERVAL_SECONDS
    assert config.telemetry_cadence.status_heartbeat_seconds == 60
    assert config.telemetry_cadence.status_idle_interval_seconds == 60.0
    assert config.telemetry_cadence.status_active_interval_seconds == 15.0
    assert config.telemetry_cadence.sensors_force_publish_seconds == 300.0
    assert config.telemetry_cadence.events_max_per_second == 1
    assert config.telemetry_cadence.events_max_per_minute == 20
    assert config.resilience.reconnect_initial_seconds == 1.0
    assert config.resilience.health_port == 0
    assert config.resilience.buffer_window_seconds == 60.0
    assert config.resilience.session_expiry_seconds == 86400
    assert config.resilience.moonraker_breaker_threshold == 5


def test_load_config_parses_broker_host_with_port(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text('[cloud]\nbroker_host = "localhost:61198"\n', encoding="utf-8")

    config = load_config(config_path)

    assert config.cloud.broker_host == "localhost"
    assert config.cloud.broker_port == 61198
    assert config.raw["cloud"]["broker_host"] == "localhost:61198"


def test_load_config_overrides_cadence(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        """
[telemetry_cadence]
status_heartbeat_seconds = 45
events_max_per_second = 3
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.telemetry_cadence.status_heartbeat_seconds == 45
    assert config.telemetry_cadence.events_max_per_second == 3


def test_load_config_overrides_defaults(tmp_path):
    config_file = tmp_path / "moonraker-owl.toml"
    config_file.write_text(
        """
[cloud]
base_url = "https://example.com"
broker_host = "mqtt.example.com"
broker_port = 1883

[moonraker]
url = "http://localhost:7125"

[telemetry]
device_id = "device-123"
tenant_id = "tenant-456"
sensors_interval_seconds = 5.0

[telemetry_cadence]
status_heartbeat_seconds = 45
"""
    )

    config = load_config(config_file)

    assert config.cloud.base_url == "https://example.com"
    assert config.telemetry.sensors_interval_seconds == 5.0
    assert config.telemetry_cadence.status_heartbeat_seconds == 45


def test_load_config_sensor_filter_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config = load_config(config_path)

    assert config.telemetry.sensor_allowlist == []
    assert config.telemetry.sensor_denylist == []


def test_load_config_sensor_filter_allowlist(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        '[telemetry]\nsensor_allowlist = ["temperature_sensor chamber", "heater_generic bed_heater"]\n',
        encoding="utf-8",
    )
    config = load_config(config_path)

    assert config.telemetry.sensor_allowlist == [
        "temperature_sensor chamber",
        "heater_generic bed_heater",
    ]
    assert config.telemetry.sensor_denylist == []


def test_load_config_sensor_filter_denylist(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        '[telemetry]\nsensor_denylist = ["temperature_sensor mcu_temp"]\n',
        encoding="utf-8",
    )
    config = load_config(config_path)

    assert config.telemetry.sensor_denylist == ["temperature_sensor mcu_temp"]
    assert config.telemetry.sensor_allowlist == []


def test_core_sensors_set_is_frozen() -> None:
    assert isinstance(CORE_SENSORS, frozenset)
    assert "extruder" in CORE_SENSORS
    assert "heater_bed" in CORE_SENSORS
    assert "fan" in CORE_SENSORS
