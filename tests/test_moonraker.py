"""Tests for the Moonraker adapter."""

import asyncio
from typing import Any

import pytest
import pytest_asyncio
from aiohttp import web

from moonraker_owl.adapters import MoonrakerClient
from moonraker_owl.config import MoonrakerConfig


@pytest_asyncio.fixture
async def moonraker_server(unused_tcp_port_factory):
    async def websocket_handler(request: web.Request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json(
            {"method": "notify_status_update", "params": [{"state": "ready"}]}
        )
        await asyncio.sleep(0)
        await ws.close()
        return ws

    async def query_handler(request: web.Request):
        body = await request.json()
        return web.json_response({"result": body})

    app = web.Application()
    app.router.add_get("/websocket", websocket_handler)
    app.router.add_post("/printer/objects/query", query_handler)

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

    try:
        yield _Server(port)
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_moonraker_client_receives_websocket_messages(moonraker_server):
    base_url = str(moonraker_server.make_url("/"))
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)

    received: list[dict[str, Any]] = []
    event = asyncio.Event()

    async def on_message(payload: dict[str, Any]) -> None:
        received.append(payload)
        event.set()

    await client.start(on_message)
    await asyncio.wait_for(event.wait(), timeout=1.0)
    await client.stop()

    assert received
    assert received[0]["method"] == "notify_status_update"


@pytest.mark.asyncio
async def test_fetch_printer_state_round_trips_payload(moonraker_server):
    base_url = str(moonraker_server.make_url("/"))
    config = MoonrakerConfig(url=base_url, api_key="token")
    client = MoonrakerClient(config)

    payload = {"status": ["state"]}
    result = await client.fetch_printer_state(payload)

    await client.aclose()

    assert result["result"] == {"objects": payload}


@pytest_asyncio.fixture
async def moonraker_gcode_server(unused_tcp_port_factory):
    """Server fixture that handles GCode script execution."""
    executed_scripts: list[str] = []
    fail_next: list[bool] = [False]

    async def gcode_handler(request: web.Request):
        body = await request.json()
        script = body.get("script", "")
        if fail_next[0]:
            fail_next[0] = False
            return web.Response(status=500, text="Klipper error: Unknown command")
        executed_scripts.append(script)
        return web.json_response({"result": "ok"})

    app = web.Application()
    app.router.add_post("/printer/gcode/script", gcode_handler)

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
        def executed_scripts(self) -> list[str]:
            return executed_scripts

        def set_fail_next(self) -> None:
            fail_next[0] = True

    try:
        yield _Server(port)
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_execute_gcode_sends_script(moonraker_gcode_server):
    """Test that execute_gcode sends the script to Moonraker."""
    base_url = str(moonraker_gcode_server.make_url("/"))
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)

    await client.execute_gcode("M104 S200")
    await client.aclose()

    assert moonraker_gcode_server.executed_scripts == ["M104 S200"]


@pytest.mark.asyncio
async def test_execute_gcode_handles_multi_line_script(moonraker_gcode_server):
    """Test that multi-line GCode scripts are sent correctly."""
    base_url = str(moonraker_gcode_server.make_url("/"))
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)

    script = "M104 S200\nM140 S60\nM106 S127"
    await client.execute_gcode(script)
    await client.aclose()

    assert moonraker_gcode_server.executed_scripts == [script]


@pytest.mark.asyncio
async def test_execute_gcode_raises_on_failure(moonraker_gcode_server):
    """Test that execute_gcode raises RuntimeError on server error."""
    base_url = str(moonraker_gcode_server.make_url("/"))
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)

    moonraker_gcode_server.set_fail_next()

    with pytest.raises(RuntimeError, match="GCode execution failed"):
        await client.execute_gcode("INVALID_COMMAND")

    await client.aclose()


# --- Thumbnail path construction tests ---


@pytest_asyncio.fixture
async def moonraker_thumbnail_server(unused_tcp_port_factory):
    """Server fixture that handles thumbnail file requests."""
    requested_paths: list[str] = []

    async def files_handler(request: web.Request):
        # Extract the path after /server/files/gcodes/
        full_path = request.path
        requested_paths.append(full_path)
        # Return dummy image data
        return web.Response(body=b"FAKE_IMAGE_DATA", content_type="image/png")

    app = web.Application()
    # Match any path under /server/files/gcodes/
    app.router.add_get("/server/files/gcodes/{path:.*}", files_handler)

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
        def paths(self) -> list[str]:
            return requested_paths

        def clear(self) -> None:
            requested_paths.clear()

    try:
        yield _Server(port)
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_fetch_thumbnail_root_file_uses_relative_path(moonraker_thumbnail_server):
    """Test that thumbnails for root-level gcode files use only the relative path."""
    base_url = str(moonraker_thumbnail_server.make_url("/"))
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)

    # For root-level file "model.gcode", thumbnail should be at ".thumbs/model-300x300.png"
    result = await client.fetch_thumbnail(
        relative_path=".thumbs/model-300x300.png",
        gcode_filename="model.gcode",
    )
    await client.aclose()

    assert result == b"FAKE_IMAGE_DATA"
    assert len(moonraker_thumbnail_server.paths) == 1
    assert moonraker_thumbnail_server.paths[0] == "/server/files/gcodes/.thumbs/model-300x300.png"


@pytest.mark.asyncio
async def test_fetch_thumbnail_subdirectory_prepends_path(moonraker_thumbnail_server):
    """Test that thumbnails for subdirectory gcode files include the subdirectory prefix.
    
    This follows Mainsail's pattern: for "4N/model.gcode", thumbnail 
    ".thumbs/model-300x300.png" is at "4N/.thumbs/model-300x300.png".
    """
    base_url = str(moonraker_thumbnail_server.make_url("/"))
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)

    # For "4N/model.gcode", thumbnail should be at "4N/.thumbs/model-300x300.png"
    result = await client.fetch_thumbnail(
        relative_path=".thumbs/model-300x300.png",
        gcode_filename="4N/model.gcode",
    )
    await client.aclose()

    assert result == b"FAKE_IMAGE_DATA"
    assert len(moonraker_thumbnail_server.paths) == 1
    assert moonraker_thumbnail_server.paths[0] == "/server/files/gcodes/4N/.thumbs/model-300x300.png"


@pytest.mark.asyncio
async def test_fetch_thumbnail_nested_subdirectory(moonraker_thumbnail_server):
    """Test thumbnail path for deeply nested subdirectories."""
    base_url = str(moonraker_thumbnail_server.make_url("/"))
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)

    # For "folder/subfolder/model.gcode", thumbnail should be at 
    # "folder/subfolder/.thumbs/model-300x300.png"
    result = await client.fetch_thumbnail(
        relative_path=".thumbs/model-300x300.png",
        gcode_filename="folder/subfolder/model.gcode",
    )
    await client.aclose()

    assert result == b"FAKE_IMAGE_DATA"
    assert len(moonraker_thumbnail_server.paths) == 1
    assert moonraker_thumbnail_server.paths[0] == "/server/files/gcodes/folder/subfolder/.thumbs/model-300x300.png"


@pytest.mark.asyncio
async def test_fetch_thumbnail_without_filename_uses_relative_path_only(moonraker_thumbnail_server):
    """Test backward compatibility: when no gcode_filename is provided, use relative_path as-is."""
    base_url = str(moonraker_thumbnail_server.make_url("/"))
    config = MoonrakerConfig(url=base_url, api_key="")
    client = MoonrakerClient(config)

    result = await client.fetch_thumbnail(
        relative_path=".thumbs/model-300x300.png",
        # No gcode_filename provided
    )
    await client.aclose()

    assert result == b"FAKE_IMAGE_DATA"
    assert len(moonraker_thumbnail_server.paths) == 1
    assert moonraker_thumbnail_server.paths[0] == "/server/files/gcodes/.thumbs/model-300x300.png"
