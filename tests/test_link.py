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
            "deviceToken": "secret-token",
            "linkedAt": "2025-10-06T00:00:00Z",
        }
        if attempts == 2:
            payload["tenantId"] = "tenant-1"
        return web.json_response(payload)

    app = web.Application()
    app.router.add_post("/device/link", handler)

    async with TestServer(app) as server:
        credentials = await link_device(
            str(server.make_url("/")),
            "CODE123",
            poll_interval=0.01,
            timeout=1.0,
        )

    assert attempts == 2
    assert credentials.device_id == "device-1"
    assert credentials.device_token == "secret-token"
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
                "deviceToken": "secret-token",
            }
        )

    app = web.Application()
    app.router.add_post("/device/link", handler)

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
        device_token="token-42",
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

    parser = ConfigParser()
    parser.read(config_path)

    assert parser.get("cloud", "username") == "tenant-42:device-42"
    assert parser.get("cloud", "password") == "token-42"
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
        device_token="token-1",
        linked_at="",
    )

    from moonraker_owl.link import _update_config_with_credentials

    _update_config_with_credentials(config, creds)
    save_config(config)

    parser = ConfigParser()
    parser.read(config_path)

    assert parser.has_section("cloud")
    assert parser.get("cloud", "username") == "device-1"
    assert parser.get("cloud", "password") == "token-1"
    assert not parser.has_option("cloud", "tenant_id")
