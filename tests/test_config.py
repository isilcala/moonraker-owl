from pathlib import Path

import logging

import pytest

from moonraker_owl import managed_defaults
from moonraker_owl.config import (
    CORE_SENSORS,
    DEFAULT_SENSORS_INTERVAL_SECONDS,
    ConfigurationError,
    load_config,
    validate_runtime_config,
)


def test_load_config_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config = load_config(config_path)

    # Cloud bootstrap endpoints come from vendor-managed defaults
    # (managed_defaults.py) when the user TOML omits them.
    assert config.cloud.base_url == managed_defaults.MANAGED_BASE_URL
    assert config.cloud.broker_host == managed_defaults.MANAGED_BROKER_HOST
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
    assert config.resilience.session_expiry_seconds == 3600
    assert config.resilience.moonraker_breaker_threshold == 5
    assert config.resilience.health_report_enabled is True
    assert config.resilience.health_report_interval_seconds == 60.0


def test_owl_toml_example_parses() -> None:
    """The shipped example config must be valid TOML and loadable.

    Guards against duplicate-key / syntax regressions that would make a fresh
    install fail to start (audit D-1 / D-3).
    """
    example = Path(__file__).resolve().parents[1] / "owl.toml.example"
    assert example.is_file()
    config = load_config(example)
    assert config.moonraker.url.startswith("http")


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


# ---------------------------------------------------------------------------
# Cloud bootstrap endpoints: vendor-managed defaults + user override
# ---------------------------------------------------------------------------


def test_managed_defaults_apply_when_cloud_absent(tmp_path: Path) -> None:
    """A user TOML that omits the endpoints falls back to managed defaults."""
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text('[moonraker]\nurl = "http://127.0.0.1:7125"\n', encoding="utf-8")

    config = load_config(config_path)

    assert config.cloud.base_url == managed_defaults.MANAGED_BASE_URL
    assert config.cloud.broker_host == managed_defaults.MANAGED_BROKER_HOST
    assert config.cloud.broker_port == managed_defaults.MANAGED_BROKER_PORT
    assert config.cloud.broker_use_tls is managed_defaults.MANAGED_BROKER_USE_TLS
    # Effective endpoints are reflected into raw for show-config visibility.
    assert config.raw["cloud"]["base_url"] == managed_defaults.MANAGED_BASE_URL
    assert config.raw["cloud"]["broker_host"] == managed_defaults.MANAGED_BROKER_HOST


def test_user_toml_overrides_managed_defaults(tmp_path: Path) -> None:
    """An explicit [cloud] value overrides the managed default (dev escape hatch)."""
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        '[cloud]\n'
        'base_url = "https://owl.example.com"\n'
        'broker_host = "mqtt.owl.example.com"\n',
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.cloud.base_url == "https://owl.example.com"
    assert config.cloud.broker_host == "mqtt.owl.example.com"
    # An explicit user value is preserved verbatim in raw.
    assert config.raw["cloud"]["base_url"] == "https://owl.example.com"


def test_validate_runtime_config_passes_with_managed_defaults(tmp_path: Path) -> None:
    """A fresh install with no [cloud] section must satisfy the runtime guard."""
    config = load_config(tmp_path / "missing.toml")

    # Should not raise: managed defaults provide the endpoints.
    validate_runtime_config(config)


def test_validate_runtime_config_raises_when_endpoints_blank(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Defence-in-depth: the guard still fires if managed defaults are blanked."""
    monkeypatch.setattr(managed_defaults, "MANAGED_BASE_URL", "")
    monkeypatch.setattr(managed_defaults, "MANAGED_BROKER_HOST", "")
    config = load_config(tmp_path / "missing.toml")

    with pytest.raises(ConfigurationError) as exc_info:
        validate_runtime_config(config)
    msg = str(exc_info.value)
    assert "cloud.base_url" in msg
    assert "cloud.broker_host" in msg


def test_validate_runtime_config_passes_when_both_set(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        '[cloud]\nbase_url = "https://example.com"\nbroker_host = "mqtt.example.com"\n',
        encoding="utf-8",
    )
    config = load_config(config_path)

    # Should not raise.
    validate_runtime_config(config)


# ---------------------------------------------------------------------------
# Audit OQ-5: deprecation warnings for TOML-bound credentials
# ---------------------------------------------------------------------------


def test_load_config_warns_on_deprecated_password(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        '[cloud]\nbase_url = "https://example.com"\n'
        'broker_host = "mqtt.example.com"\npassword = "hunter2"\n',
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING, logger="moonraker_owl.config"):
        load_config(config_path)

    messages = [rec.getMessage() for rec in caplog.records]
    assert any(
        "cloud.password is deprecated" in m for m in messages
    ), f"expected deprecation warning, got: {messages}"


def test_load_config_warns_on_deprecated_device_private_key(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        '[cloud]\nbase_url = "https://example.com"\n'
        'broker_host = "mqtt.example.com"\n'
        'device_private_key = "secret-key-data"\n',
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING, logger="moonraker_owl.config"):
        load_config(config_path)

    messages = [rec.getMessage() for rec in caplog.records]
    assert any(
        "cloud.device_private_key is deprecated" in m for m in messages
    ), f"expected deprecation warning, got: {messages}"


def test_load_config_no_warning_when_credentials_absent(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        '[cloud]\nbase_url = "https://example.com"\n'
        'broker_host = "mqtt.example.com"\n',
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING, logger="moonraker_owl.config"):
        load_config(config_path)

    deprecation_records = [
        r for r in caplog.records if "deprecated" in r.getMessage()
    ]
    assert deprecation_records == []
