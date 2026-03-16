"""Tests for GCode file upload to Moonraker — non-ASCII filename handling."""

import os
import tempfile

import pytest
import pytest_asyncio
from aiohttp import web

from moonraker_owl.adapters import MoonrakerClient
from moonraker_owl.config import MoonrakerConfig


@pytest_asyncio.fixture
async def moonraker_upload_server(unused_tcp_port_factory):
    """Server fixture that captures multipart upload requests."""
    received_uploads: list[dict] = []

    async def upload_handler(request: web.Request):
        reader = await request.multipart()
        fields: dict = {}
        async for part in reader:
            name = part.name
            if name == "file":
                # Capture the raw filename from Content-Disposition
                fields["filename"] = part.filename
                fields["content"] = await part.read()
            else:
                fields[name] = (await part.read()).decode()
        received_uploads.append(fields)
        return web.json_response(
            {"result": {"item": {"path": fields.get("filename", "")}}}
        )

    app = web.Application()
    app.router.add_post("/server/files/upload", upload_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    port = unused_tcp_port_factory()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()

    class _Server:
        def __init__(self, server_port: int):
            self._port = server_port

        def make_url(self, path: str = "/") -> str:
            if not path.startswith("/"):
                path = "/" + path
            return f"http://127.0.0.1:{self._port}{path}"

        @property
        def uploads(self) -> list[dict]:
            return received_uploads

    try:
        yield _Server(port)
    finally:
        await runner.cleanup()


def _make_temp_gcode(content: bytes = b"; test gcode\nG28\n") -> str:
    """Create a temporary GCode file and return its path."""
    fd, path = tempfile.mkstemp(suffix=".gcode")
    os.write(fd, content)
    os.close(fd)
    return path


@pytest.mark.asyncio
async def test_upload_chinese_filename_preserved(moonraker_upload_server):
    """Chinese filename arrives at Moonraker as raw UTF-8, not percent-encoded."""
    base_url = moonraker_upload_server.make_url("/")
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)
    tmp = _make_temp_gcode()

    try:
        result = await client.upload_gcode_file(filename="立方体.gcode", file_path=tmp)
    finally:
        await client.aclose()
        os.unlink(tmp)

    assert len(moonraker_upload_server.uploads) == 1
    upload = moonraker_upload_server.uploads[0]
    assert upload["filename"] == "立方体.gcode"
    assert "%E7" not in (upload.get("filename") or "")  # not percent-encoded
    assert result == "立方体.gcode"


@pytest.mark.asyncio
async def test_upload_japanese_filename_preserved(moonraker_upload_server):
    """Japanese filename arrives unmodified."""
    base_url = moonraker_upload_server.make_url("/")
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)
    tmp = _make_temp_gcode()

    try:
        result = await client.upload_gcode_file(filename="テスト印刷.gcode", file_path=tmp)
    finally:
        await client.aclose()
        os.unlink(tmp)

    upload = moonraker_upload_server.uploads[0]
    assert upload["filename"] == "テスト印刷.gcode"
    assert result == "テスト印刷.gcode"


@pytest.mark.asyncio
async def test_upload_mixed_ascii_and_unicode_filename(moonraker_upload_server):
    """Filename with mixed ASCII and Unicode characters is preserved."""
    base_url = moonraker_upload_server.make_url("/")
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)
    tmp = _make_temp_gcode()
    name = "Benchy_底座_v2.gcode"

    try:
        result = await client.upload_gcode_file(filename=name, file_path=tmp)
    finally:
        await client.aclose()
        os.unlink(tmp)

    upload = moonraker_upload_server.uploads[0]
    assert upload["filename"] == name
    assert result == name


@pytest.mark.asyncio
async def test_upload_filename_with_spaces_preserved(moonraker_upload_server):
    """Filenames with spaces are not URL-encoded."""
    base_url = moonraker_upload_server.make_url("/")
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)
    tmp = _make_temp_gcode()

    try:
        await client.upload_gcode_file(filename="my model v2.gcode", file_path=tmp)
    finally:
        await client.aclose()
        os.unlink(tmp)

    upload = moonraker_upload_server.uploads[0]
    assert upload["filename"] == "my model v2.gcode"
    assert "%20" not in upload["filename"]


@pytest.mark.asyncio
async def test_upload_sends_root_gcodes_field(moonraker_upload_server):
    """Upload request includes root=gcodes form field."""
    base_url = moonraker_upload_server.make_url("/")
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)
    tmp = _make_temp_gcode()

    try:
        await client.upload_gcode_file(filename="test.gcode", file_path=tmp)
    finally:
        await client.aclose()
        os.unlink(tmp)

    upload = moonraker_upload_server.uploads[0]
    assert upload["root"] == "gcodes"


@pytest_asyncio.fixture
async def moonraker_error_server(unused_tcp_port_factory):
    """Server fixture that always returns 500 for uploads."""

    async def error_handler(request: web.Request):
        # Consume the body so the connection closes cleanly
        await request.read()
        return web.Response(status=500, text="Internal Server Error")

    app = web.Application()
    app.router.add_post("/server/files/upload", error_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = unused_tcp_port_factory()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()

    class _Server:
        def __init__(self, server_port: int):
            self._port = server_port

        def make_url(self, path: str = "/") -> str:
            return f"http://127.0.0.1:{self._port}{path}"

    try:
        yield _Server(port)
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_upload_server_error_raises(moonraker_error_server):
    """Server returning 500 raises RuntimeError."""
    base_url = moonraker_error_server.make_url("/")
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)
    tmp = _make_temp_gcode()

    try:
        with pytest.raises(RuntimeError, match="GCode upload failed with status 500"):
            await client.upload_gcode_file(filename="test.gcode", file_path=tmp)
    finally:
        await client.aclose()
        os.unlink(tmp)


@pytest.mark.asyncio
async def test_upload_empty_filename_raises():
    """Empty filename raises ValueError before any HTTP call."""
    config = MoonrakerConfig(url="http://localhost:7125", api_key="")
    client = MoonrakerClient(config)

    try:
        with pytest.raises(ValueError, match="Filename cannot be empty"):
            await client.upload_gcode_file(filename="", file_path="/nonexistent")
    finally:
        await client.aclose()
