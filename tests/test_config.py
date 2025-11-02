from pathlib import Path

from moonraker_owl.config import load_config, DEFAULT_TELEMETRY_RATE_HZ


def test_load_config_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.cfg"
    config = load_config(config_path)

    assert config.cloud.broker_host
    assert config.moonraker.url.startswith("http")
    assert config.telemetry.rate_hz == DEFAULT_TELEMETRY_RATE_HZ
    assert config.resilience.reconnect_initial_seconds == 1.0
    assert config.resilience.health_port == 0


def test_load_config_parses_broker_host_with_port(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.cfg"
    config_path.write_text("[cloud]\nbroker_host = localhost:61198\n", encoding="utf-8")

    config = load_config(config_path)

    assert config.cloud.broker_host == "localhost"
    assert config.cloud.broker_port == 61198
    assert config.raw.get("cloud", "broker_host") == "localhost"
    assert config.raw.get("cloud", "broker_port") == "61198"
