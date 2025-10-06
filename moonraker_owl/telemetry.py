"""Telemetry publishing pipeline."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Protocol

from .adapters import MQTTConnectionError
from .config import OwlConfig

LOGGER = logging.getLogger(__name__)


class TelemetryConfigurationError(RuntimeError):
    """Raised when required telemetry configuration values are missing."""


class MoonrakerClientLike(Protocol):
    async def start(
        self, callback: Callable[[Dict[str, Any]], Awaitable[None] | None]
    ) -> None: ...

    def remove_callback(
        self, callback: Callable[[Dict[str, Any]], Awaitable[None] | None]
    ) -> None: ...

    async def fetch_printer_state(self) -> Dict[str, Any]: ...


class MQTTClientLike(Protocol):
    def publish(
        self, topic: str, payload: bytes, qos: int = 1, retain: bool = False
    ) -> None: ...


class TelemetryPublisher:
    """Listens to Moonraker updates and pushes filtered telemetry to MQTT."""

    def __init__(
        self,
        config: OwlConfig,
        moonraker: MoonrakerClientLike,
        mqtt: MQTTClientLike,
        *,
        queue_size: int = 1,
    ) -> None:
        self._config = config
        self._moonraker = moonraker
        self._mqtt = mqtt

        self._tenant_id, self._device_id, self._printer_id = _resolve_printer_identity(
            config
        )
        self._topic = f"owl/printers/{self._device_id}/telemetry"

        self._min_interval = _derive_interval_seconds(config.telemetry.rate_hz)
        self._include_fields = tuple(config.include_fields)

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max(queue_size, 1))
        self._event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._worker: Optional[asyncio.Task[None]] = None
        self._callback_registered = False

    async def start(self) -> None:
        if self._worker is not None:
            raise RuntimeError("TelemetryPublisher already started")

        self._loop = asyncio.get_running_loop()
        self._stop_event.clear()

        await self._moonraker.start(self._handle_moonraker_update)
        self._callback_registered = True

        await self._prime_initial_state()

        self._worker = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        self._event.set()

        if self._worker is not None:
            self._worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker
            self._worker = None

        if self._callback_registered:
            self._moonraker.remove_callback(self._handle_moonraker_update)
            self._callback_registered = False

    @property
    def topic(self) -> str:
        return self._topic

    async def _prime_initial_state(self) -> None:
        try:
            snapshot = await self._moonraker.fetch_printer_state()
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning("Fetching initial Moonraker state failed: %s", exc)
            return

        await self._enqueue(snapshot)

    async def _handle_moonraker_update(self, payload: Dict[str, Any]) -> None:
        await self._enqueue(payload)

    async def _enqueue(self, payload: Dict[str, Any]) -> None:
        if self._stop_event.is_set():
            return

        if self._queue.full():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass

        await self._queue.put(payload)
        self._event.set()

    async def _run(self) -> None:
        loop = self._loop or asyncio.get_running_loop()
        last_sent = 0.0

        while not self._stop_event.is_set():
            await self._event.wait()
            self._event.clear()

            while not self._queue.empty():
                payload = await self._queue.get()
                sample = _build_payload(
                    payload,
                    include_fields=self._include_fields,
                    tenant_id=self._tenant_id,
                    printer_id=self._printer_id,
                    device_id=self._device_id,
                )

                if sample is None:
                    continue

                now = loop.time()
                elapsed = now - last_sent
                if elapsed < self._min_interval:
                    remaining = self._min_interval - elapsed
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(), timeout=remaining
                        )
                        return
                    except asyncio.TimeoutError:
                        pass

                try:
                    payload_bytes = json.dumps(sample, default=_json_default).encode(
                        "utf-8"
                    )
                    self._mqtt.publish(self._topic, payload_bytes, qos=1, retain=False)
                    last_sent = loop.time()
                except MQTTConnectionError as exc:
                    LOGGER.warning("Telemetry publish failed: %s", exc)
                except Exception:  # pragma: no cover - defensive logging
                    LOGGER.exception("Unexpected error publishing telemetry")

        LOGGER.debug("TelemetryPublisher loop terminated")


def _resolve_printer_identity(config: OwlConfig) -> tuple[str, str, str]:
    raw = config.raw

    tenant_id = raw.get("cloud", "tenant_id", fallback=config.cloud.username or "")
    device_id = raw.get("cloud", "device_id", fallback="")
    printer_id = raw.get("cloud", "printer_id", fallback="")

    if not device_id and config.cloud.username and ":" in config.cloud.username:
        _, maybe_device = config.cloud.username.split(":", 1)
        device_id = maybe_device

    if not tenant_id and config.cloud.username:
        tenant_id = config.cloud.username.split(":", 1)[0]

    if not device_id:
        raise TelemetryConfigurationError(
            "Device ID is required for telemetry publishing"
        )

    if not printer_id:
        LOGGER.debug(
            "Printer ID missing from configuration; telemetry payload will omit it"
        )

    return tenant_id, device_id, printer_id


def _derive_interval_seconds(rate_hz: float) -> float:
    if rate_hz <= 0:
        return 1.0
    return max(0.05, 1.0 / rate_hz)


def _build_payload(
    raw_payload: Dict[str, Any],
    *,
    include_fields: tuple[str, ...],
    tenant_id: Optional[str],
    printer_id: Optional[str],
    device_id: str,
) -> Optional[Dict[str, Any]]:
    channels = _extract_channels(raw_payload, include_fields)
    if not channels:
        return None

    document: Dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "deviceId": device_id,
        "channels": channels,
    }

    if tenant_id:
        document["tenantId"] = tenant_id
    if printer_id:
        document["printerId"] = printer_id

    method = raw_payload.get("method")
    if isinstance(method, str):
        document["moonrakerEvent"] = method

    return document


def _extract_channels(
    payload: Dict[str, Any], include_fields: tuple[str, ...]
) -> Dict[str, Any]:
    candidates: Dict[str, Any] = {}

    result = payload.get("result")
    if isinstance(result, dict):
        candidates.update(result)

    params = payload.get("params")
    if isinstance(params, list):
        for item in params:
            if isinstance(item, dict):
                candidates.update(item)

    channels: Dict[str, Any] = {}
    if not include_fields:
        include_iterable = candidates.items()
    else:
        include_iterable = ((field, candidates.get(field)) for field in include_fields)

    for key, value in include_iterable:
        if value is None:
            continue
        channels[key] = value

    return channels


def _json_default(value: Any) -> Any:
    try:
        return str(value)
    except Exception:  # pragma: no cover - defensive fallback
        return repr(value)
