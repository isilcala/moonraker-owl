import json
from configparser import ConfigParser
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from moonraker_owl.config import load_config, save_config
from moonraker_owl.link import (
    DeviceCredentials,
    DeviceLinkingError,
    link_device,
    perform_linking,
)


@pytest.mark.asyncio
async def test_link_device_polls_until_success():
    attempts = 0

    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal attempts
        attempts += 1
        payload = await request.json()
        assert payload["linkCode"] == "CODE123"
        if attempts < 2:
            return web.Response(status=404)
        payload = {
            "printerId": "printer-1",
            "deviceId": "device-1",
            "devicePrivateKey": "dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA==",  # Base64 test key
            "linkedAt": "2025-10-06T00:00:00Z",
        }
        if attempts == 2:
            payload["tenantId"] = "tenant-1"
        return web.json_response(payload)

    app = web.Application()
    app.router.add_post("/api/v1/devices/link", handler)

    async with TestServer(app) as server:
        credentials = await link_device(
            str(server.make_url("/")),
            "CODE123",
            poll_interval=0.01,
            timeout=1.0,
        )

    assert attempts == 2
    assert credentials.device_id == "device-1"
    assert credentials.device_private_key == "dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA=="
    assert credentials.tenant_id == "tenant-1"


@pytest.mark.asyncio
async def test_link_device_allows_missing_tenant_id():
    async def handler(request: web.Request) -> web.StreamResponse:
        payload = await request.json()
        assert payload["linkCode"] == "CODE123"
        return web.json_response(
            {
                "printerId": "printer-1",
                "deviceId": "device-1",
                "devicePrivateKey": "dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA==",
            }
        )

    app = web.Application()
    app.router.add_post("/api/v1/devices/link", handler)

    async with TestServer(app) as server:
        credentials = await link_device(
            str(server.make_url("/")),
            "CODE123",
            poll_interval=0.01,
            timeout=1.0,
        )

    assert credentials.tenant_id == ""


def test_perform_linking_updates_config_and_credentials(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "moonraker-owl.cfg"
    config = load_config(config_path)

    fake_creds = DeviceCredentials(
        tenant_id="tenant-42",
        printer_id="printer-42",
        device_id="device-42",
        device_private_key="dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA==",
        linked_at="2025-10-06T00:00:00Z",
    )

    async def fake_link_device(*_args, **_kwargs):  # noqa: ANN001
        return fake_creds

    monkeypatch.setattr("moonraker_owl.link.link_device", fake_link_device)

    credentials_path = tmp_path / "credentials.json"

    result = perform_linking(
        config,
        force=True,
        link_code="ABC",
        credentials_path=credentials_path,
        poll_interval=0.01,
        timeout=1.0,
    )

    assert result == fake_creds

    stored = json.loads(credentials_path.read_text(encoding="utf-8"))
    assert stored["deviceId"] == "device-42"
    assert stored["tenantId"] == "tenant-42"
    assert stored["devicePrivateKey"] == "dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA=="

    parser = ConfigParser()
    parser.read(config_path)

    assert parser.get("cloud", "username") == "tenant-42:device-42"
    assert parser.get("cloud", "device_private_key") == "dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA=="
    assert parser.get("cloud", "tenant_id") == "tenant-42"
    assert parser.get("cloud", "device_id") == "device-42"


def test_perform_linking_requires_force_when_credentials_exist(
    monkeypatch, tmp_path: Path
):
    config_path = tmp_path / "moonraker-owl.cfg"
    config = load_config(config_path)
    credentials_path = tmp_path / "existing.json"
    credentials_path.write_text("{}", encoding="utf-8")

    with pytest.raises(DeviceLinkingError):
        perform_linking(
            config,
            force=False,
            link_code="ABC",
            credentials_path=credentials_path,
        )


def test_update_config_with_credentials_without_tenant(tmp_path: Path):
    config_path = tmp_path / "moonraker-owl.cfg"
    config = load_config(config_path)

    creds = DeviceCredentials(
        tenant_id="",
        printer_id="printer-1",
        device_id="device-1",
        device_private_key="dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA==",
        linked_at="",
    )

    from moonraker_owl.link import _update_config_with_credentials

    _update_config_with_credentials(config, creds)
    save_config(config)

    parser = ConfigParser()
    parser.read(config_path)

    assert parser.has_section("cloud")
    assert parser.get("cloud", "username") == "device-1"
    assert not parser.has_option("cloud", "tenant_id")


def test_credentials_file_has_secure_permissions(monkeypatch, tmp_path: Path):
    """Test that credentials.json is created with 0600 permissions (Unix only)."""
    import os
    import sys
    
    # Skip test on Windows (no chmod support)
    if not hasattr(os, "chmod") or sys.platform.startswith("win"):
        pytest.skip("File permissions test only runs on Unix systems")
    
    config_path = tmp_path / "moonraker-owl.cfg"
    config = load_config(config_path)

    fake_creds = DeviceCredentials(
        tenant_id="tenant-42",
        printer_id="printer-42",
        device_id="device-42",
        device_private_key="dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA==",
        linked_at="2025-10-06T00:00:00Z",
    )

    async def fake_link_device(*_args, **_kwargs):  # noqa: ANN001
        return fake_creds

    monkeypatch.setattr("moonraker_owl.link.link_device", fake_link_device)

    credentials_path = tmp_path / "credentials.json"

    perform_linking(
        config,
        force=True,
        link_code="ABC",
        credentials_path=credentials_path,
        poll_interval=0.01,
        timeout=1.0,
    )

    # Check file permissions (should be 0600 = rw-------)
    import stat
    file_stat = credentials_path.stat()
    file_mode = stat.S_IMODE(file_stat.st_mode)
    
    # On Unix, file should be readable and writable only by owner
    assert file_mode == 0o600, f"Expected 0o600, got {oct(file_mode)}"


def test_device_private_key_stored_in_config(tmp_path: Path):
    """Test that device_private_key is properly stored in owl.cfg."""
    config_path = tmp_path / "moonraker-owl.cfg"
    config = load_config(config_path)

    creds = DeviceCredentials(
        tenant_id="tenant-test",
        printer_id="printer-test",
        device_id="device-test",
        device_private_key="dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA==",
        linked_at="2025-10-06T00:00:00Z",
    )

    from moonraker_owl.link import _update_config_with_credentials

    _update_config_with_credentials(config, creds)
    save_config(config)

    # Reload config and verify private key is present
    reloaded_config = load_config(config_path)
    
    assert reloaded_config.cloud.device_private_key == "dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA=="
    assert reloaded_config.raw.get("cloud", "device_private_key") == "dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA=="
