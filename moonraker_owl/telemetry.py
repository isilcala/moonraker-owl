"""Telemetry publishing pipeline."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Protocol

from .adapters import MQTTConnectionError
from .config import OwlConfig
from .core import PrinterAdapter

LOGGER = logging.getLogger(__name__)


class TelemetryConfigurationError(RuntimeError):
    """Raised when required telemetry configuration values are missing."""


class MQTTClientLike(Protocol):
    def publish(
        self, topic: str, payload: bytes, qos: int = 1, retain: bool = False
    ) -> None: ...


class TelemetryPublisher:
    """Listens to Moonraker updates and pushes filtered telemetry to MQTT."""

    def __init__(
        self,
        config: OwlConfig,
        moonraker: PrinterAdapter,
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
        self._include_fields = _normalise_fields(config.include_fields)
        self._exclude_fields = _normalise_fields(config.exclude_fields)
        self._subscription_objects: dict[str, Optional[list[str]]] = (
            build_subscription_manifest(self._include_fields, self._exclude_fields)
        )
        self._moonraker.set_subscription_objects(self._subscription_objects)

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
            snapshot = await self._moonraker.fetch_printer_state(
                self._subscription_objects
            )
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
                    exclude_fields=self._exclude_fields,
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

    tenant_id = raw.get("cloud", "tenant_id", fallback="")
    device_id = raw.get("cloud", "device_id", fallback="")
    printer_id = raw.get("cloud", "printer_id", fallback="")

    username = config.cloud.username or ""

    if not device_id and username:
        if ":" in username:
            _, maybe_device = username.split(":", 1)
            device_id = maybe_device
        else:
            device_id = username

    if not tenant_id and username and ":" in username:
        tenant_id = username.split(":", 1)[0]

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
    exclude_fields: tuple[str, ...],
    tenant_id: Optional[str],
    printer_id: Optional[str],
    device_id: str,
) -> Optional[Dict[str, Any]]:
    channels = _extract_channels(raw_payload, include_fields)
    if exclude_fields:
        channels = _apply_exclude_fields(channels, exclude_fields)
        if not channels:
            return None
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

    expanded = dict(candidates)
    pending = [value for value in candidates.values() if isinstance(value, dict)]
    while pending:
        nested = pending.pop()
        for key, value in nested.items():
            if key not in expanded:
                expanded[key] = value
                if isinstance(value, dict):
                    pending.append(value)

    channels: Dict[str, Any] = {}
    if not include_fields:
        include_iterable = expanded.items()
    else:
        include_iterable = ((field, expanded.get(field)) for field in include_fields)

    for key, value in include_iterable:
        if value is None:
            continue
        channels[key] = value

    return channels


def _apply_exclude_fields(
    channels: Dict[str, Any], exclude_fields: tuple[str, ...]
) -> Dict[str, Any]:
    filtered = copy.deepcopy(channels)
    for path in exclude_fields:
        parts = [segment.strip() for segment in path.split(".") if segment.strip()]
        if not parts:
            continue
        _remove_path(filtered, parts)

    return {
        key: value
        for key, value in filtered.items()
        if not (isinstance(value, dict) and not value)
    }


def _remove_path(target: Any, parts: list[str]) -> None:
    if not parts or not isinstance(target, dict):
        return

    key = parts[0]

    if key == "*":
        remaining = parts[1:]
        for sub_key in list(target.keys()):
            if not remaining:
                target.pop(sub_key, None)
            else:
                _remove_path(target.get(sub_key), remaining)
                _cleanup_empty(target, sub_key)
        return

    if key not in target:
        return

    if len(parts) == 1:
        target.pop(key, None)
        return

    _remove_path(target.get(key), parts[1:])
    _cleanup_empty(target, key)


def _cleanup_empty(target: Dict[str, Any], key: str) -> None:
    value = target.get(key)
    if isinstance(value, dict) and not value:
        target.pop(key, None)


def build_subscription_manifest(
    include_fields: Iterable[str], exclude_fields: Iterable[str]
) -> dict[str, Optional[list[str]]]:
    excluded_objects = {
        _normalise_field(field)
        for field in exclude_fields
        if field and "." not in field and "*" not in field
    }

    subscribe_all: set[str] = set()
    attribute_map: dict[str, set[str]] = {}

    for field in include_fields:
        field = _normalise_field(field)
        if not field:
            continue

        base, has_dot, attribute = field.partition(".")
        base = base.strip()
        if not base or base in excluded_objects:
            continue

        if not has_dot:
            subscribe_all.add(base)
            continue

        attribute = attribute.strip()
        if not attribute:
            subscribe_all.add(base)
            continue

        attribute_map.setdefault(base, set()).add(attribute)

    objects: dict[str, Optional[list[str]]] = {}
    for base in sorted(subscribe_all | attribute_map.keys()):
        if base in subscribe_all:
            objects[base] = None
        else:
            objects[base] = sorted(attribute_map.get(base, set()))

    return objects


def _normalise_fields(fields: Iterable[str]) -> tuple[str, ...]:
    return tuple(
        normalised
        for normalised in (_normalise_field(field) for field in fields)
        if normalised
    )


def _normalise_field(field: str) -> str:
    field = field.strip()
    if len(field) >= 2 and field[0] == field[-1] and field[0] in {'"', "'"}:
        field = field[1:-1].strip()
    return field


def _json_default(value: Any) -> Any:
    try:
        return str(value)
    except Exception:  # pragma: no cover - defensive fallback
        return repr(value)
