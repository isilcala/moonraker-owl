"""Tests for CameraClient snapshot size limits (P1-4)."""

import io

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from PIL import Image

from moonraker_owl.adapters.camera import CameraClient


def _tiny_png() -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (4, 4), (10, 20, 30)).save(buf, format="PNG")
    return buf.getvalue()


@pytest.mark.asyncio
async def test_capture_rejects_oversized_content_length():
    async def handler(request: web.Request) -> web.StreamResponse:
        # Claim a huge image via Content-Length without sending it.
        return web.Response(
            body=b"x",
            headers={"Content-Type": "image/jpeg", "Content-Length": str(50_000_000)},
        )

    app = web.Application()
    app.router.add_get("/snapshot", handler)

    async with TestServer(app) as server:
        url = str(server.make_url("/snapshot"))
        client = CameraClient(url, max_image_bytes=1024)
        try:
            result = await client.capture()
        finally:
            await client.close()

    assert result.success is False
    assert result.error_code == "image_too_large"


@pytest.mark.asyncio
async def test_capture_rejects_oversized_stream_without_content_length():
    async def handler(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            headers={"Content-Type": "image/jpeg"}
        )
        await response.prepare(request)
        # Stream more than the cap in chunks (chunked encoding, no length).
        for _ in range(40):
            await response.write(b"a" * 1024)
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_get("/snapshot", handler)

    async with TestServer(app) as server:
        url = str(server.make_url("/snapshot"))
        client = CameraClient(url, max_image_bytes=10 * 1024)
        try:
            result = await client.capture()
        finally:
            await client.close()

    assert result.success is False
    assert result.error_code == "image_too_large"


@pytest.mark.asyncio
async def test_capture_accepts_image_within_limit():
    png = _tiny_png()

    async def handler(request: web.Request) -> web.StreamResponse:
        return web.Response(body=png, headers={"Content-Type": "image/png"})

    app = web.Application()
    app.router.add_get("/snapshot", handler)

    async with TestServer(app) as server:
        url = str(server.make_url("/snapshot"))
        client = CameraClient(url, max_image_bytes=1024 * 1024)
        try:
            result = await client.capture()
        finally:
            await client.close()

    assert result.success is True
    assert result.image_data == png
    assert result.image_width == 4
    assert result.image_height == 4
