"""Health reporting utilities for moonraker-owl."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional

from aiohttp import web

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ComponentStatus:
    name: str
    healthy: bool
    detail: Optional[str] = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def as_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "healthy": self.healthy,
            "detail": self.detail,
            "updatedAt": self.updated_at.isoformat(timespec="seconds"),
        }


class HealthReporter:
    """Tracks component statuses for the running service."""

    _AGENT_KEY = "__agent_state__"

    def __init__(self) -> None:
        self._status: Dict[str, ComponentStatus] = {}
        self._lock = asyncio.Lock()

    async def update(
        self, name: str, healthy: bool, detail: Optional[str] = None
    ) -> None:
        async with self._lock:
            self._status[name] = ComponentStatus(
                name=name, healthy=healthy, detail=detail
            )

    async def set_agent_state(
        self, state: str, *, healthy: bool, detail: Optional[str] = None
    ) -> None:
        detail_value = detail if detail is not None else state
        await self.update(self._AGENT_KEY, healthy, detail_value)

    async def snapshot(self) -> Dict[str, object]:
        async with self._lock:
            entries = list(self._status.values())

        agent_state: Optional[ComponentStatus] = None
        components: list[Dict[str, object]] = []
        for status in entries:
            if status.name == self._AGENT_KEY:
                agent_state = status
                continue
            components.append(status.as_dict())

        overall_components_healthy = all(item["healthy"] for item in components)
        overall = "ok" if overall_components_healthy else "degraded"
        if agent_state is not None and not agent_state.healthy:
            overall = "degraded"

        payload: Dict[str, object] = {"status": overall, "components": components}
        if agent_state is not None:
            payload["agentState"] = {
                "state": agent_state.detail,
                "healthy": agent_state.healthy,
                "updatedAt": agent_state.updated_at.isoformat(timespec="seconds"),
            }

        return payload


class HealthServer:
    """Minimal HTTP server exposing `/healthz` for status checks."""

    def __init__(self, reporter: HealthReporter, host: str, port: int) -> None:
        self._reporter = reporter
        self._host = host
        self._port = port
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/healthz", self._handle_health)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()
        LOGGER.info(
            "Health endpoint listening on http://%s:%s/healthz", self._host, self._port
        )

    async def stop(self) -> None:
        with contextlib.suppress(Exception):
            if self._site is not None:
                await self._site.stop()
        if self._runner is not None:
            await self._runner.cleanup()
        self._site = None
        self._runner = None

    async def _handle_health(self, request: web.Request) -> web.Response:
        snapshot = await self._reporter.snapshot()
        status = 200 if snapshot["status"] == "ok" else 503
        return web.json_response(snapshot, status=status)
