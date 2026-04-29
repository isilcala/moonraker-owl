"""Sprint 2 hardening tests for task:download-gcode and task:capture-image.

Audit findings: A-02 (download URL allowlist + size cap + content-type),
A-03 (capture upload URL pinning + agent hard cap on size).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from aiohttp import web

from moonraker_owl.commands.processor import CommandProcessor
from moonraker_owl.commands.handlers._url_validation import (
    MAX_CAPTURE_UPLOAD_BYTES,
    MAX_DOWNLOAD_BYTES,
    build_allowlist,
    validate_external_url,
    validate_gcode_content_type,
)
from moonraker_owl.commands.types import CommandMessage, CommandProcessingError

from helpers import build_config

# Re-use FakeMoonraker / FakeMQTT from the main command-test module.
from test_commands import FakeMQTT, FakeMoonraker  # type: ignore[import-not-found]


# ────────────────────────────────────────────────────────────────────
# Pure unit tests for _url_validation (no event loop, no network).
# ────────────────────────────────────────────────────────────────────


def _allow(*hosts: str) -> tuple[str, ...]:
    return build_allowlist(
        cloud_base_url="https://api.example.com",
        extra_allowed_hosts=list(hosts),
    )


class TestValidateExternalUrl:
    def test_accepts_base_url_host_implicitly(self) -> None:
        host = validate_external_url(
            "https://api.example.com/files/x.gcode",
            field="downloadUrl",
            command_id="c-1",
            allowlist=_allow(),
        )
        assert host == "api.example.com"

    def test_accepts_exact_extra_host(self) -> None:
        host = validate_external_url(
            "https://files.example.org/x.gcode",
            field="downloadUrl",
            command_id="c-1",
            allowlist=_allow("files.example.org"),
        )
        assert host == "files.example.org"

    def test_accepts_suffix_match(self) -> None:
        host = validate_external_url(
            "https://my-bucket.s3.amazonaws.com/x.gcode",
            field="downloadUrl",
            command_id="c-1",
            allowlist=_allow(".amazonaws.com"),
        )
        assert host == "my-bucket.s3.amazonaws.com"

    def test_rejects_non_allowlisted_host(self) -> None:
        with pytest.raises(CommandProcessingError) as exc:
            validate_external_url(
                "https://attacker.example.net/x.gcode",
                field="downloadUrl",
                command_id="c-1",
                allowlist=_allow(),
            )
        assert exc.value.code == "invalid_downloadUrl_url"

    def test_rejects_private_ip_when_not_explicitly_allowed(self) -> None:
        with pytest.raises(CommandProcessingError) as exc:
            validate_external_url(
                "http://192.168.1.50/x.gcode",
                field="downloadUrl",
                command_id="c-1",
                allowlist=_allow(),
            )
        assert exc.value.code == "invalid_downloadUrl_url"

    def test_rejects_loopback_when_not_explicitly_allowed(self) -> None:
        with pytest.raises(CommandProcessingError) as exc:
            validate_external_url(
                "http://127.0.0.1:7125/x.gcode",
                field="downloadUrl",
                command_id="c-1",
                allowlist=_allow(),
            )
        assert exc.value.code == "invalid_downloadUrl_url"

    def test_accepts_loopback_when_explicitly_allowed(self) -> None:
        host = validate_external_url(
            "http://127.0.0.1:9000/files/x.gcode",
            field="downloadUrl",
            command_id="c-1",
            allowlist=_allow("127.0.0.1"),
        )
        assert host == "127.0.0.1"

    def test_rejects_link_local_ipv6_literal(self) -> None:
        with pytest.raises(CommandProcessingError) as exc:
            validate_external_url(
                "http://[fe80::1]/x.gcode",
                field="uploadUrl",
                command_id="c-1",
                allowlist=_allow(),
            )
        assert exc.value.code == "invalid_uploadUrl_url"

    def test_rejects_non_http_scheme(self) -> None:
        with pytest.raises(CommandProcessingError) as exc:
            validate_external_url(
                "file:///etc/passwd",
                field="downloadUrl",
                command_id="c-1",
                allowlist=_allow("api.example.com"),
            )
        assert exc.value.code == "invalid_downloadUrl_url"

    def test_rejects_missing_url(self) -> None:
        with pytest.raises(CommandProcessingError):
            validate_external_url(
                "", field="downloadUrl", command_id="c-1", allowlist=_allow()
            )

    def test_does_not_echo_full_url_in_error(self) -> None:
        # Defense against log injection: a malicious cloud may craft a
        # URL containing newlines/control chars in the path. The error
        # should expose the host only.
        try:
            validate_external_url(
                "https://attacker.example.net/path\n[FAKE LOG ENTRY]",
                field="downloadUrl",
                command_id="c-1",
                allowlist=_allow(),
            )
        except CommandProcessingError as exc:
            assert "[FAKE LOG ENTRY]" not in str(exc)
        else:
            pytest.fail("expected rejection")


class TestValidateContentType:
    def test_accepts_text_plain(self) -> None:
        validate_gcode_content_type("text/plain", command_id="c-1")

    def test_accepts_octet_stream_with_charset(self) -> None:
        validate_gcode_content_type(
            "application/octet-stream; charset=utf-8", command_id="c-1"
        )

    def test_accepts_x_gcode(self) -> None:
        validate_gcode_content_type("application/x-gcode", command_id="c-1")

    def test_permissive_on_missing_header(self) -> None:
        validate_gcode_content_type(None, command_id="c-1")
        validate_gcode_content_type("", command_id="c-1")

    def test_rejects_text_html(self) -> None:
        with pytest.raises(CommandProcessingError) as exc:
            validate_gcode_content_type("text/html; charset=utf-8", command_id="c-1")
        assert exc.value.code == "invalid_content_type"

    def test_rejects_application_json(self) -> None:
        with pytest.raises(CommandProcessingError) as exc:
            validate_gcode_content_type("application/json", command_id="c-1")
        assert exc.value.code == "invalid_content_type"


# ────────────────────────────────────────────────────────────────────
# Integration tests against a local aiohttp server.
# ────────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def download_server(unused_tcp_port_factory):
    """Serves canned GCode payloads with configurable content-type / size."""

    state: dict[str, Any] = {
        "body": b"; hello\nG28\n",
        "content_type": "text/plain",
        "stream_megabytes": None,  # set to int to stream that many MiB of zeros
    }

    async def gcode_handler(request: web.Request) -> web.StreamResponse:
        if state["stream_megabytes"] is not None:
            # Simulate a malicious server streaming forever.
            response = web.StreamResponse(
                status=200,
                headers={"Content-Type": state["content_type"]},
            )
            await response.prepare(request)
            chunk = b"\x00" * (1024 * 1024)
            for _ in range(state["stream_megabytes"]):
                await response.write(chunk)
            await response.write_eof()
            return response
        return web.Response(
            body=state["body"],
            content_type=state["content_type"],
        )

    app = web.Application()
    app.router.add_get("/files/{name}", gcode_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = unused_tcp_port_factory()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()

    class _Server:
        def __init__(self, p: int) -> None:
            self.port = p

        def url(self, name: str = "x.gcode") -> str:
            return f"http://127.0.0.1:{self.port}/files/{name}"

        @property
        def state(self) -> dict[str, Any]:
            return state

    try:
        yield _Server(port)
    finally:
        await runner.cleanup()


def _make_processor_with_allowlist(allow: list[str]):
    cfg = build_config()
    cfg.cloud.allowed_storage_hosts = allow
    moonraker = FakeMoonraker()
    moonraker.upload_gcode_file = AsyncMock(return_value="uploaded.gcode")  # type: ignore[method-assign]
    mqtt = FakeMQTT()
    return CommandProcessor(cfg, moonraker, mqtt), moonraker, mqtt


def _last_completed_result(mqtt: FakeMQTT) -> dict[str, Any]:
    """Return the result dict from the last 'completed' ACK."""
    for topic, payload, _, _ in reversed(mqtt.published):
        ack = json.loads(payload.decode("utf-8"))
        if ack["payload"].get("status") == "completed":
            return ack["payload"].get("result", {})
    raise AssertionError("no completed ACK published")


@pytest.mark.asyncio
async def test_download_gcode_rejects_url_outside_allowlist():
    cfg = build_config()
    cfg.cloud.allowed_storage_hosts = []  # only base_url (api.owl.dev) allowed
    moonraker = FakeMoonraker()
    moonraker.upload_gcode_file = AsyncMock(return_value="x")  # type: ignore[method-assign]
    mqtt = FakeMQTT()
    proc = CommandProcessor(cfg, moonraker, mqtt)
    await proc.start()
    msg = {
        "$id": "cmd-d1",
        "payload": {
            "command": "task:download-gcode",
            "parameters": {
                "transferId": "t-1",
                "downloadUrl": "https://attacker.example.net/x.gcode",
                "fileName": "x.gcode",
                "fileSizeBytes": 100,
            },
        },
    }
    await mqtt.emit(
        "owl/printers/device-123/commands/task:download-gcode", msg
    )

    moonraker.upload_gcode_file.assert_not_awaited()
    statuses = [
        json.loads(p.decode("utf-8"))["payload"]["status"]
        for _, p, _, _ in mqtt.published
    ]
    assert any(s in {"rejected", "failed"} for s in statuses), statuses
    await proc.stop()


@pytest.mark.asyncio
async def test_download_gcode_rejects_loopback_when_not_in_allowlist(download_server):
    proc, moonraker, mqtt = _make_processor_with_allowlist([])
    await proc.start()
    msg = {
        "$id": "cmd-d2",
        "payload": {
            "command": "task:download-gcode",
            "parameters": {
                "transferId": "t-2",
                "downloadUrl": download_server.url(),
                "fileName": "x.gcode",
                "fileSizeBytes": 100,
            },
        },
    }
    await mqtt.emit(
        "owl/printers/device-123/commands/task:download-gcode", msg
    )

    # No upload should have reached Moonraker.
    moonraker.upload_gcode_file.assert_not_awaited()
    # A rejected/failed ack should be published; either status=rejected
    # (CommandProcessingError) or status=completed with success=False.
    statuses = [
        json.loads(p.decode("utf-8"))["payload"]["status"]
        for _, p, _, _ in mqtt.published
    ]
    assert any(s in {"rejected", "failed"} for s in statuses) or any(
        s == "completed" for s in statuses
    )
    await proc.stop()


@pytest.mark.asyncio
async def test_download_gcode_accepts_loopback_when_explicitly_allowed(
    download_server,
):
    proc, moonraker, mqtt = _make_processor_with_allowlist(["127.0.0.1"])
    download_server.state["body"] = b"; happy path\nG28\nM104 S200\n"
    download_server.state["content_type"] = "text/plain"
    await proc.start()
    msg = {
        "$id": "cmd-d3",
        "payload": {
            "command": "task:download-gcode",
            "parameters": {
                "transferId": "t-3",
                "downloadUrl": download_server.url(),
                "fileName": "happy.gcode",
                "fileSizeBytes": 30,
            },
        },
    }
    await mqtt.emit(
        "owl/printers/device-123/commands/task:download-gcode", msg
    )

    moonraker.upload_gcode_file.assert_awaited_once()
    result = _last_completed_result(mqtt)
    assert result["success"] is True
    assert result["fileSizeBytes"] == len(download_server.state["body"])
    await proc.stop()


@pytest.mark.asyncio
async def test_download_gcode_rejects_html_content_type(download_server):
    proc, moonraker, mqtt = _make_processor_with_allowlist(["127.0.0.1"])
    download_server.state["body"] = b"<html><body>not gcode</body></html>"
    download_server.state["content_type"] = "text/html"
    await proc.start()
    msg = {
        "$id": "cmd-d4",
        "payload": {
            "command": "task:download-gcode",
            "parameters": {
                "transferId": "t-4",
                "downloadUrl": download_server.url(),
                "fileName": "x.gcode",
                "fileSizeBytes": 30,
            },
        },
    }
    await mqtt.emit(
        "owl/printers/device-123/commands/task:download-gcode", msg
    )

    moonraker.upload_gcode_file.assert_not_awaited()
    result = _last_completed_result(mqtt)
    assert result["success"] is False
    assert result.get("error") == "invalid_content_type"
    await proc.stop()


@pytest.mark.asyncio
async def test_download_gcode_rejects_declared_size_above_hard_cap():
    cfg = build_config()
    cfg.cloud.allowed_storage_hosts = ["127.0.0.1"]
    moonraker = FakeMoonraker()
    moonraker.upload_gcode_file = AsyncMock(return_value="x")  # type: ignore[method-assign]
    mqtt = FakeMQTT()
    proc = CommandProcessor(cfg, moonraker, mqtt)
    await proc.start()
    msg = {
        "$id": "cmd-d5",
        "payload": {
            "command": "task:download-gcode",
            "parameters": {
                "transferId": "t-5",
                "downloadUrl": "http://127.0.0.1:1/x.gcode",
                "fileName": "x.gcode",
                "fileSizeBytes": MAX_DOWNLOAD_BYTES + 1,
            },
        },
    }
    await mqtt.emit(
        "owl/printers/device-123/commands/task:download-gcode", msg
    )

    moonraker.upload_gcode_file.assert_not_awaited()
    result = _last_completed_result(mqtt)
    assert result["success"] is False
    assert result.get("error") == "file_too_large"
    await proc.stop()


# ────────────────────────────────────────────────────────────────────
# A-03 capture-image upload URL + hard cap.
# ────────────────────────────────────────────────────────────────────


def _make_capture_processor(*, allow: list[str], camera_image: bytes):
    cfg = build_config()
    cfg.cloud.allowed_storage_hosts = allow

    s3_upload = MagicMock()
    s3_upload.upload = AsyncMock(
        return_value=MagicMock(
            success=True,
            file_size_bytes=len(camera_image),
            error_code=None,
            error_message=None,
        )
    )

    capture_result = MagicMock()
    capture_result.success = True
    capture_result.image_data = camera_image
    capture_result.content_type = "image/jpeg"
    capture_result.captured_at = None
    capture_result.error_message = None
    capture_result.error_code = None
    capture_result.image_width = 640
    capture_result.image_height = 480

    camera = MagicMock()
    camera.capture = AsyncMock(return_value=capture_result)

    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    proc = CommandProcessor(
        cfg,
        moonraker,
        mqtt,
        s3_upload=s3_upload,
        camera=camera,
        image_preprocessor=None,
    )
    return proc, mqtt, s3_upload


@pytest.mark.asyncio
async def test_capture_rejects_upload_url_outside_allowlist():
    proc, mqtt, s3_upload = _make_capture_processor(
        allow=[], camera_image=b"\xff\xd8\xff" + b"\x00" * 1024
    )
    await proc.start()
    msg = {
        "$id": "cmd-c1",
        "payload": {
            "command": "task:capture-image",
            "parameters": {
                "frameId": "f-1",
                "uploadUrl": "https://attacker.example.net/upload",
                "blobKey": "tenant/cap/x.jpg",
            },
        },
    }
    await mqtt.emit(
        "owl/printers/device-123/commands/task:capture-image", msg
    )

    s3_upload.upload.assert_not_awaited()
    statuses = [
        json.loads(p.decode("utf-8"))["payload"]["status"]
        for _, p, _, _ in mqtt.published
    ]
    assert any(s in {"rejected", "failed"} for s in statuses)
    await proc.stop()


@pytest.mark.asyncio
async def test_capture_rejects_private_ip_upload_url_when_not_allowlisted():
    proc, mqtt, s3_upload = _make_capture_processor(
        allow=[], camera_image=b"\xff\xd8\xff" + b"\x00" * 1024
    )
    await proc.start()
    msg = {
        "$id": "cmd-c2",
        "payload": {
            "command": "task:capture-image",
            "parameters": {
                "frameId": "f-2",
                "uploadUrl": "http://10.0.0.5/upload",
                "blobKey": "tenant/cap/x.jpg",
            },
        },
    }
    await mqtt.emit(
        "owl/printers/device-123/commands/task:capture-image", msg
    )

    s3_upload.upload.assert_not_awaited()
    await proc.stop()


@pytest.mark.asyncio
async def test_capture_caps_size_at_agent_hard_max_regardless_of_cloud():
    # Cloud asks for 1 GiB; agent should truncate to 10 MiB. Image is
    # bigger than the hard cap so we expect image_too_large.
    huge = b"\xff\xd8\xff" + b"\x00" * (MAX_CAPTURE_UPLOAD_BYTES + 1)
    proc, mqtt, s3_upload = _make_capture_processor(
        allow=["api.example.com"],
        camera_image=huge,
    )
    await proc.start()
    msg = {
        "$id": "cmd-c3",
        "payload": {
            "command": "task:capture-image",
            "parameters": {
                "frameId": "f-3",
                "uploadUrl": "https://api.example.com/upload",
                "blobKey": "tenant/cap/big.jpg",
                "maxFileSizeBytes": 1024 * 1024 * 1024,  # 1 GiB lie
            },
        },
    }
    await mqtt.emit(
        "owl/printers/device-123/commands/task:capture-image", msg
    )

    s3_upload.upload.assert_not_awaited()
    # Most recent completed ack carries the image_too_large error.
    result = None
    for _, p, _, _ in reversed(mqtt.published):
        ack = json.loads(p.decode("utf-8"))
        if ack["payload"].get("status") == "completed":
            result = ack["payload"].get("result")
            break
    assert result is not None
    assert result["success"] is False
    assert result["errorCode"] == "image_too_large"
    await proc.stop()


@pytest.mark.asyncio
async def test_capture_succeeds_when_url_allowlisted_and_size_ok():
    small = b"\xff\xd8\xff" + b"\x00" * 1024
    proc, mqtt, s3_upload = _make_capture_processor(
        allow=[".s3.amazonaws.com"],
        camera_image=small,
    )
    await proc.start()
    msg = {
        "$id": "cmd-c4",
        "payload": {
            "command": "task:capture-image",
            "parameters": {
                "frameId": "f-4",
                "uploadUrl": "https://my-bucket.s3.amazonaws.com/upload?sig=...",
                "blobKey": "tenant/cap/ok.jpg",
            },
        },
    }
    await mqtt.emit(
        "owl/printers/device-123/commands/task:capture-image", msg
    )

    s3_upload.upload.assert_awaited_once()
    await proc.stop()
