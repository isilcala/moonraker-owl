"""Moonraker adapter providing HTTP and WebSocket helpers."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Mapping, Optional
from urllib.parse import urlparse, urlunparse

import aiohttp

from ..config import MoonrakerConfig
from ..core import CallbackType, PrinterAdapter

LOGGER = logging.getLogger(__name__)


class MoonrakerClient(PrinterAdapter):
    """Non-blocking utility for Moonraker connectivity."""

    def __init__(
        self,
        config: MoonrakerConfig,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        reconnect_initial: float = 1.0,
        reconnect_max: float = 10.0,
    ) -> None:
        self.config = config
        self.reconnect_initial = reconnect_initial
        self.reconnect_max = reconnect_max

        self._base_url = self.config.url.rstrip("/")
        self._headers = {}
        if self.config.api_key:
            self._headers["X-Api-Key"] = self.config.api_key

        self._session: Optional[aiohttp.ClientSession] = session
        self._owns_session = session is None
        self._callbacks: list[CallbackType] = []
        self._listener_task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._subscription_objects: Optional[dict[str, Optional[list[str]]]] = None
        self._rpc_id = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def start(self, callback: CallbackType) -> None:
        """Start listening to Moonraker WebSocket notifications."""

        if callback in self._callbacks:
            raise ValueError("Callback already registered")

        self._callbacks.append(callback)

        if self._listener_task is not None:
            return

        if self._owns_session and self._session is None:
            timeout = aiohttp.ClientTimeout(total=None)
            self._session = aiohttp.ClientSession(timeout=timeout)

        self._stop_event.clear()
        self._listener_task = asyncio.create_task(self._listen_loop())
        await asyncio.sleep(0)

    async def stop(self) -> None:
        """Stop listening and close the underlying resources."""

        self._callbacks.clear()
        self._stop_event.set()

        if self._listener_task is not None:
            self._listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listener_task
            self._listener_task = None

        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def aclose(self) -> None:  # alias for explicit closing
        await self.stop()

    def remove_callback(self, callback: CallbackType) -> None:
        """Remove a previously registered callback."""

        with contextlib.suppress(ValueError):
            self._callbacks.remove(callback)

    async def fetch_printer_state(
        self, objects: Optional[Mapping[str, Optional[list[str]]]] = None
    ) -> dict:
        """Fetch the current printer state via HTTP."""

        session = await self._ensure_session()
        url = f"{self._base_url}/printer/objects/query"
        payload = {"objects": objects or {}}

        async with session.post(url, json=payload, headers=self._headers) as response:
            response.raise_for_status()
            return await response.json()

    def set_subscription_objects(
        self, objects: Mapping[str, Optional[list[str]]] | None
    ) -> None:
        """Declare which Moonraker objects should be subscribed to on connect."""

        if objects is None:
            self._subscription_objects = None
        else:
            # ensure payload is JSON-serialisable and lists are copied
            self._subscription_objects = {
                key: (list(value) if isinstance(value, list) else None)
                for key, value in objects.items()
            }

    async def execute_print_action(self, action: str) -> None:
        """Invoke pause/resume/cancel actions on Moonraker."""

        action_normalized = action.strip().lower()
        endpoint = {
            "pause": "pause",
            "resume": "resume",
            "cancel": "cancel",
        }.get(action_normalized)

        if endpoint is None:
            raise ValueError(f"Unsupported Moonraker action: {action!r}")

        session = await self._ensure_session()
        url = f"{self._base_url}/printer/print/{endpoint}"

        async with session.post(url, headers=self._headers) as response:
            if response.status >= 400:
                detail = await response.text()
                raise RuntimeError(
                    f"Moonraker action '{action_normalized}' failed with status {response.status}: {detail.strip()}"
                )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=None)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._owns_session = True
        return self._session

    async def _listen_loop(self) -> None:
        backoff = self.reconnect_initial

        while not self._stop_event.is_set():
            try:
                session = await self._ensure_session()
                ws_url = _build_ws_url(self._base_url)
                async with session.ws_connect(ws_url, headers=self._headers) as ws:
                    LOGGER.info("Connected to Moonraker websocket at %s", ws_url)
                    backoff = self.reconnect_initial

                    await self._send_subscription(ws)
                    async for message in ws:
                        if self._stop_event.is_set():
                            break
                        if message.type == aiohttp.WSMsgType.TEXT:
                            await self._dispatch(message.data)
                        elif message.type == aiohttp.WSMsgType.BINARY:
                            LOGGER.debug("Ignoring binary message from Moonraker")
                        elif message.type == aiohttp.WSMsgType.ERROR:
                            raise ws.exception() or RuntimeError("Websocket error")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive net handling
                if self._stop_event.is_set():
                    break
                LOGGER.warning("Moonraker websocket error: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.reconnect_max)

    async def _dispatch(self, raw_data: str) -> None:
        try:
            payload = json.loads(raw_data)
        except json.JSONDecodeError:
            LOGGER.debug("Discarding non-JSON message: %s", raw_data)
            return

        for callback in list(self._callbacks):
            try:
                result = callback(payload)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("Moonraker callback failed")

    async def _send_subscription(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        # Subscribe to printer status objects
        if self._subscription_objects:
            self._rpc_id += 1
            payload = {
                "jsonrpc": "2.0",
                "method": "printer.objects.subscribe",
                "id": self._rpc_id,
                "params": {"objects": self._subscription_objects},
            }

            try:
                await ws.send_json(payload)
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception(
                    "Failed to send Moonraker printer objects subscription"
                )


def _build_ws_url(http_url: str) -> str:
    parsed = urlparse(http_url)
    scheme = "ws"
    if parsed.scheme == "https":
        scheme = "wss"

    path = parsed.path.rstrip("/") + "/websocket"
    return urlunparse((scheme, parsed.netloc, path, "", "", ""))
