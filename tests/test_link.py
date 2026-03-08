import json
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from moonraker_owl.config import load_config, load_credentials, merge_credentials
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
    config_path = tmp_path / "moonraker-owl.toml"
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


def test_perform_linking_requires_force_when_credentials_exist(
    monkeypatch, tmp_path: Path
):
    config_path = tmp_path / "moonraker-owl.toml"
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


def test_merge_credentials_without_tenant(tmp_path: Path):
    config_path = tmp_path / "moonraker-owl.toml"
    config = load_config(config_path)

    creds_path = tmp_path / "credentials.json"
    creds_path.write_text(
        json.dumps({
            "printerId": "printer-1",
            "deviceId": "device-1",
            "devicePrivateKey": "dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA==",
        }),
        encoding="utf-8",
    )

    merge_credentials(config, creds_path)

    assert config.cloud.device_id == "device-1"
    assert config.cloud.username == "device-1"
    assert config.cloud.tenant_id is None


def test_credentials_file_has_secure_permissions(monkeypatch, tmp_path: Path):
    """Test that credentials.json is created with 0600 permissions (Unix only)."""
    import os
    import sys
    
    # Skip test on Windows (no chmod support)
    if not hasattr(os, "chmod") or sys.platform.startswith("win"):
        pytest.skip("File permissions test only runs on Unix systems")
    
    config_path = tmp_path / "moonraker-owl.toml"
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


def test_merge_credentials_populates_cloud_config(tmp_path: Path):
    """Test that merge_credentials loads JSON and populates CloudConfig."""
    config_path = tmp_path / "moonraker-owl.toml"
    config = load_config(config_path)

    creds_path = tmp_path / "credentials.json"
    creds_path.write_text(
        json.dumps({
            "tenantId": "tenant-test",
            "printerId": "printer-test",
            "deviceId": "device-test",
            "devicePrivateKey": "dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA==",
        }),
        encoding="utf-8",
    )

    merge_credentials(config, creds_path)

    assert config.cloud.device_private_key == "dGVzdF9wcml2YXRlX2tleV80NF9ieXRlc19iYXNlNjRfZW5jb2RlZA=="
    assert config.cloud.device_id == "device-test"
    assert config.cloud.printer_id == "printer-test"
    assert config.cloud.tenant_id == "tenant-test"
    assert config.cloud.username == "tenant-test:device-test"


def test_merge_credentials_skips_when_file_missing(tmp_path: Path):
    """Test that merge_credentials handles missing credentials file gracefully."""
    config_path = tmp_path / "moonraker-owl.toml"
    config = load_config(config_path)

    merge_credentials(config, tmp_path / "nonexistent.json")

    assert config.cloud.device_id is None
    assert config.cloud.device_private_key is None


def test_load_credentials_returns_none_when_missing(tmp_path: Path, monkeypatch):
    """Test load_credentials returns None for non-existent file."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "fakehome")
    result = load_credentials(tmp_path / "nonexistent.json")
    assert result is None


def test_load_credentials_reads_json(tmp_path: Path):
    """Test load_credentials correctly parses JSON."""
    creds_path = tmp_path / "credentials.json"
    creds_path.write_text(
        json.dumps({"deviceId": "dev-1", "tenantId": "t-1"}),
        encoding="utf-8",
    )
    result = load_credentials(creds_path)
    assert result is not None
    assert result["deviceId"] == "dev-1"
    assert result["tenantId"] == "t-1"
