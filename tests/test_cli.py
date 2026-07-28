"""Tests for the moonraker-owl CLI surface (audit A-04 / Q-1).

Specifically: `show-config` must not echo secret-bearing fields verbatim.
Operators frequently capture the output into bug reports; redaction has to
happen at the print layer, not be left to the human pasting the log.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from moonraker_owl.cli import main


def _write_config(path: Path) -> None:
    path.write_text(
        """
[cloud]
base_url = "https://example.com"
broker_host = "mqtt.example.com"
password = "supersecretpassword"
device_private_key = "-----BEGIN PRIVATE KEY-----\\nAAAA\\n-----END PRIVATE KEY-----"

[moonraker]
url = "http://127.0.0.1:7125"
api_key = "moonraker-api-key-do-not-leak"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_show_config_redacts_password(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    _write_config(config_path)

    rc = main(["-c", str(config_path), "show-config"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "supersecretpassword" not in out
    assert "***redacted***" in out


def test_show_config_redacts_device_private_key(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    _write_config(config_path)

    rc = main(["-c", str(config_path), "show-config"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "BEGIN PRIVATE KEY" not in out
    assert "AAAA" not in out


def test_show_config_redacts_moonraker_api_key(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    _write_config(config_path)

    rc = main(["-c", str(config_path), "show-config"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "moonraker-api-key-do-not-leak" not in out


def test_show_config_still_emits_non_secret_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    _write_config(config_path)

    rc = main(["-c", str(config_path), "show-config"])

    assert rc == 0
    out = capsys.readouterr().out
    # Sanity: redaction must not nuke the rest of the section.
    assert "base_url" in out
    assert "https://example.com" in out
    assert "broker_host" in out


def test_show_config_no_redaction_when_secret_absent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """If a secret field is absent / empty, no placeholder is emitted for it."""
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        '[cloud]\nbase_url = "https://example.com"\n'
        'broker_host = "mqtt.example.com"\n',
        encoding="utf-8",
    )

    rc = main(["-c", str(config_path), "show-config"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "***redacted***" not in out


def test_migrate_config_updates_known_staging_endpoint(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "moonraker-owl.toml"
    config_path.write_text(
        '[cloud]\nbase_url = "https://owl.elencala.com"\n'
        'broker_host = "mqtt.owl.elencala.com"\n',
        encoding="utf-8",
    )

    rc = main(["-c", str(config_path), "migrate-config"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Configuration migration applied" in out
    updated = config_path.read_text(encoding="utf-8")
    assert 'base_url = "https://staging.mewcon.net"' in updated
    assert 'broker_host = "mqtt.staging.mewcon.net"' in updated
    assert "broker_use_tls = true" in updated
