from pathlib import Path

from moonraker_owl.config import load_config


def test_load_config_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.cfg"
    config = load_config(config_path)

    assert config.cloud.broker_host
    assert config.moonraker.url.startswith("http")
    assert config.telemetry.rate_hz == 1.0
    assert config.resilience.reconnect_initial_seconds == 1.0
    assert config.resilience.health_port == 0
