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
