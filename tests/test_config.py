from pathlib import Path

from moonraker_owl.config import load_config, DEFAULT_TELEMETRY_RATE_HZ


def test_load_config_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.cfg"
    config = load_config(config_path)

    assert config.cloud.broker_host
    assert config.moonraker.url.startswith("http")
    assert config.telemetry.rate_hz == DEFAULT_TELEMETRY_RATE_HZ
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
    config_path = tmp_path / "moonraker-owl.cfg"
    config_path.write_text("[cloud]\nbroker_host = localhost:61198\n", encoding="utf-8")

    config = load_config(config_path)

    assert config.cloud.broker_host == "localhost"
    assert config.cloud.broker_port == 61198
    assert config.raw.get("cloud", "broker_host") == "localhost"
    assert config.raw.get("cloud", "broker_port") == "61198"


def test_load_config_overrides_cadence(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.cfg"
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
    config_file = tmp_path / "moonraker-owl.cfg"
    config_file.write_text(
        """
[cloud]
base_url = https://example.com
broker_host = mqtt.example.com
broker_port = 1883

[moonraker]
url = http://localhost:7125

[telemetry]
device_id = device-123
tenant_id = tenant-456
rate_hz = 10

[telemetry_cadence]
status_heartbeat_seconds = 45
"""
    )

    config = load_config(config_file)

    assert config.cloud.base_url == "https://example.com"
    assert config.telemetry.rate_hz == 10.0
    assert config.telemetry_cadence.status_heartbeat_seconds == 45
