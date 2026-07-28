from pathlib import Path

from moonraker_owl.config_migration import migrate_config_file


def test_migrate_config_file_updates_known_legacy_staging_domains(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        """
[cloud]
base_url = "https://owl.elencala.com"
broker_host = "mqtt.owl.elencala.com"
broker_port = 1883
device_id = "printer-specific"

[moonraker]
url = "http://127.0.0.1:7125"
""".lstrip(),
        encoding="utf-8",
    )

    result = migrate_config_file(config_path)

    assert result.changed is True
    assert result.changes == ["cloud.base_url", "cloud.broker_host", "cloud.broker_port", "cloud.broker_use_tls"]
    updated = config_path.read_text(encoding="utf-8")
    assert 'base_url = "https://staging.mewcon.net"' in updated
    assert 'broker_host = "mqtt.staging.mewcon.net"' in updated
    assert "broker_port = 8883" in updated
    assert "broker_use_tls = true" in updated
    assert 'device_id = "printer-specific"' in updated
    assert result.backup_path is not None
    assert result.backup_path.exists()


def test_migrate_config_file_handles_broker_host_port_shorthand(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        """
[cloud]
base_url = "https://owl.elencala.com/"
broker_host = "mqtt.owl.elencala.com:8883"
""".lstrip(),
        encoding="utf-8",
    )

    result = migrate_config_file(config_path)

    assert result.changed is True
    updated = config_path.read_text(encoding="utf-8")
    assert 'base_url = "https://staging.mewcon.net"' in updated
    assert 'broker_host = "mqtt.staging.mewcon.net"' in updated
    assert "broker_port = 8883" in updated
    assert "broker_use_tls = true" in updated


def test_migrate_config_file_overwrites_custom_cloud_target(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    original = """
[cloud]
base_url = "https://custom.example.com"
broker_host = "mqtt.custom.example.com"
broker_port = 1883
broker_use_tls = false
""".lstrip()
    config_path.write_text(original, encoding="utf-8")

    result = migrate_config_file(config_path)

    assert result.changed is True
    updated = config_path.read_text(encoding="utf-8")
    assert 'base_url = "https://staging.mewcon.net"' in updated
    assert 'broker_host = "mqtt.staging.mewcon.net"' in updated
    assert "broker_port = 8883" in updated
    assert "broker_use_tls = true" in updated
    assert result.backup_path is not None
    assert result.backup_path.read_text(encoding="utf-8") == original


def test_migrate_config_file_is_idempotent_after_update(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        """
[cloud]
base_url = "https://staging.mewcon.net"
broker_host = "mqtt.staging.mewcon.net"
broker_port = 8883
broker_use_tls = true
""".lstrip(),
        encoding="utf-8",
    )

    result = migrate_config_file(config_path)

    assert result.changed is False
    assert result.skipped_reason == "already_current"


def test_migrate_config_file_creates_missing_cloud_section(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text('[moonraker]\nurl = "http://127.0.0.1:7125"\n', encoding="utf-8")

    result = migrate_config_file(config_path)

    assert result.changed is True
    updated = config_path.read_text(encoding="utf-8")
    assert "[cloud]" in updated
    assert 'base_url = "https://staging.mewcon.net"' in updated
    assert 'broker_host = "mqtt.staging.mewcon.net"' in updated
    assert "broker_port = 8883" in updated
    assert "broker_use_tls = true" in updated


def test_migrate_config_file_populates_blank_cloud_bootstrap_values(tmp_path: Path) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        """
[cloud]
base_url = ""
broker_host = ""

[moonraker]
url = "http://127.0.0.1:7125"
""".lstrip(),
        encoding="utf-8",
    )

    result = migrate_config_file(config_path)

    assert result.changed is True
    updated = config_path.read_text(encoding="utf-8")
    assert 'base_url = "https://staging.mewcon.net"' in updated
    assert 'broker_host = "mqtt.staging.mewcon.net"' in updated