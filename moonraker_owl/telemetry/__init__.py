"""Telemetry publishing pipeline for the Owl contract."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import logging
import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Protocol

from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .. import constants
from ..adapters import MQTTConnectionError
from ..config import OwlConfig
from ..core import PrinterAdapter, deep_merge
from .orchestrator import TelemetryOrchestrator
from .state_store import MoonrakerStateStore

LOGGER = logging.getLogger(__name__)


def is_heater_object(obj_name: str) -> bool:
    """Return True when the Moonraker object represents a heater."""

    return obj_name in ("extruder", "heater_bed") or obj_name.startswith(
        ("extruder", "heater_generic")
    )


def get_heater_objects(objects: Dict[str, Any]) -> Dict[str, Optional[list[str]]]:
    """Extract heater objects from a subscription manifest."""

    return {obj: None for obj in objects if is_heater_object(obj)}


class TelemetryConfigurationError(RuntimeError):
    """Raised when required telemetry configuration values are missing."""


class MQTTClientLike(Protocol):
    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 1,
        retain: bool = False,
        *,
        properties=None,
    ) -> None: ...


class TelemetryPublisher:
    """Consumes Moonraker updates and forwards telemetry envelopes to MQTT."""

    def __init__(
        self,
        config: OwlConfig,
        moonraker: PrinterAdapter,
        mqtt: MQTTClientLike,
        *,
        queue_size: int = 16,
    ) -> None:
        self._config = config
        self._moonraker = moonraker
        self._mqtt = mqtt

        (
            self._tenant_id,
            self._device_id,
            self._printer_id,
            self._device_token,
        ) = _resolve_printer_identity(config)

        self._base_topic = f"owl/printers/{self._device_id}"
        self._channel_topics: Dict[str, str] = {
            "overview": f"{self._base_topic}/overview",
            "telemetry": f"{self._base_topic}/telemetry",
            "events": f"{self._base_topic}/events",
        }
        self._channel_qos = {
            "overview": 1,
            "telemetry": 0,
            "events": 2,
        }

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

        self._heater_merge_cache: Dict[str, Any] = {}
        self._last_overview_publish_time = 0.0
        self._last_overview_status = "Idle"
        self._last_payload_snapshot: Optional[Dict[str, Any]] = None
        self._overview_idle_interval = 60.0
        self._overview_active_interval = 15.0

        self._orchestrator = TelemetryOrchestrator(
            clock=lambda: datetime.now(timezone.utc)
        )
        idle_hz = config.telemetry.rate_hz or 0.0
        if idle_hz <= 0:
            idle_hz = 0.033
        self._orchestrator.set_telemetry_mode(
            mode="idle",
            max_hz=idle_hz,
            watch_window_expires=None,
        )

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
        return self._channel_topics["telemetry"]

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
        if payload.get("method") == "notify_status_update":
            try:
                heater_objects = get_heater_objects(self._subscription_objects or {})

                if heater_objects:
                    heater_state = await self._moonraker.fetch_printer_state(
                        heater_objects, timeout=5.0
                    )
                    heater_status = (
                        heater_state.get("result", {}).get("status")
                        if isinstance(heater_state, dict)
                        else None
                    )

                    if heater_status:
                        has_new_details = self._track_heater_snapshot(heater_status)

                        if has_new_details:
                            await self._enqueue({"result": {"status": heater_status}})
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.debug("Failed to query heater state: %s", exc)

        await self._enqueue(payload)

    def _track_heater_snapshot(self, heater_status: Dict[str, Any]) -> bool:
        has_new_details = False

        for key, data in heater_status.items():
            if isinstance(data, dict):
                snapshot = _normalise_heater_snapshot(data)
                if not snapshot:
                    if key in self._heater_merge_cache:
                        self._heater_merge_cache.pop(key, None)
                        has_new_details = True
                    continue

                previous_snapshot = self._heater_merge_cache.get(key)
                self._heater_merge_cache[key] = snapshot

                if not isinstance(previous_snapshot, dict):
                    has_new_details = True
                    continue

                previous_contract = (
                    previous_snapshot.get("temperature"),
                    previous_snapshot.get("target"),
                )
                current_contract = (
                    snapshot.get("temperature"),
                    snapshot.get("target"),
                )

                if previous_contract != current_contract:
                    has_new_details = True
            else:
                if self._heater_merge_cache.get(key) != data:
                    self._heater_merge_cache[key] = copy.deepcopy(data)
                    has_new_details = True

        return has_new_details

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

        pending: Optional[Dict[str, Any]] = None

        while not self._stop_event.is_set():
            if pending is None:
                heartbeat_payload = self._prepare_heartbeat_payload()
                if heartbeat_payload is not None:
                    pending = heartbeat_payload
                else:
                    await self._event.wait()
                    self._event.clear()
                    pending = self._gather_payloads()
                    if pending is None:
                        continue

            while not self._stop_event.is_set():
                now = loop.time()
                elapsed = now - last_sent
                if last_sent == 0.0 or elapsed >= self._min_interval:
                    break

                remaining = self._min_interval - elapsed
                try:
                    await asyncio.wait_for(self._event.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    break

                self._event.clear()
                pending = self._gather_payloads(pending)
                if pending is None:
                    break

            if pending is None:
                continue

            try:
                self._process_payload(pending)
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("Telemetry pipeline failed")

            last_sent = loop.time()
            pending = self._gather_payloads()

        LOGGER.debug("TelemetryPublisher loop terminated")

    def _gather_payloads(
        self, base: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        merged: Optional[Dict[str, Any]] = copy.deepcopy(base) if base else None

        while True:
            try:
                payload = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if merged is None:
                merged = copy.deepcopy(payload)
            else:
                _merge_payload_dicts(merged, payload)

        return merged

    def _process_payload(self, payload: Dict[str, Any]) -> None:
        self._orchestrator.ingest(payload)
        self._last_payload_snapshot = copy.deepcopy(payload)

        frames = self._orchestrator.build_envelopes()
        if not frames:
            return

        include_raw = bool(self._config.telemetry.include_raw_payload)
        raw_json: Optional[str] = None
        if include_raw:
            raw_json = json.dumps(payload, default=_json_default)

        for channel, document in frames.items():
            topic = self._channel_topics.get(channel)
            if topic is None:
                continue

            publish_document = copy.deepcopy(document)
            publish_document["deviceId"] = self._device_id
            if self._tenant_id:
                publish_document["tenantId"] = self._tenant_id
            if self._printer_id:
                publish_document["printerId"] = self._printer_id
            if include_raw and raw_json is not None:
                publish_document["raw"] = raw_json

            if channel == "overview":
                overview_body = publish_document.get("overview")
                if isinstance(overview_body, dict):
                    status_value = overview_body.get("printerStatus")
                    if isinstance(status_value, str) and status_value:
                        self._last_overview_status = status_value
                        self._last_overview_publish_time = time.monotonic()

            self._publish(channel, topic, publish_document)

    def _prepare_heartbeat_payload(self) -> Optional[Dict[str, Any]]:
        if not self._should_emit_overview_heartbeat():
            return None

        if self._last_payload_snapshot is None:
            return None

        return copy.deepcopy(self._last_payload_snapshot)

    def _should_emit_overview_heartbeat(self) -> bool:
        if self._last_overview_publish_time == 0.0:
            return False

        interval = (
            self._overview_active_interval
            if self._last_overview_status in {"Printing", "Paused"}
            else self._overview_idle_interval
        )

        return time.monotonic() - self._last_overview_publish_time >= interval

    def _publish(self, channel: str, topic: str, document: Dict[str, Any]) -> None:
        payload_bytes = json.dumps(document, default=_json_default).encode("utf-8")
        properties = Properties(PacketTypes.PUBLISH)
        properties.UserProperty = [
            (constants.DEVICE_TOKEN_MQTT_PROPERTY_NAME, self._device_token)
        ]

        try:
            self._mqtt.publish(
                topic,
                payload_bytes,
                qos=self._channel_qos.get(channel, 1),
                retain=False,
                properties=properties,
            )
        except MQTTConnectionError as exc:
            LOGGER.warning("Telemetry publish failed: %s", exc)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception(
                "Unexpected error publishing telemetry for channel %s", channel
            )


def _normalise_heater_snapshot(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    snapshot: Dict[str, Any] = {}
    for field in ("temperature", "target", "power"):
        value = data.get(field)
        if value is None:
            continue

        rounded = _round_temperature(value)
        if rounded is not None:
            snapshot[field] = rounded
            continue

        snapshot[field] = value

    return snapshot or None


def _round_temperature(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return math.floor(numeric * 10.0) / 10.0


def _resolve_printer_identity(config: OwlConfig) -> tuple[str, str, str, str]:
    raw = config.raw

    tenant_id = raw.get("cloud", "tenant_id", fallback="")
    device_id = raw.get("cloud", "device_id", fallback="")
    printer_id = raw.get("cloud", "printer_id", fallback="")
    device_token = (config.cloud.password or "").strip()
    if not device_token:
        device_token = raw.get("cloud", "password", fallback="").strip()

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

    if not device_token:
        raise TelemetryConfigurationError(
            "Device token is required for telemetry publishing"
        )

    return tenant_id, device_id, printer_id, device_token


def _derive_interval_seconds(rate_hz: float) -> float:
    if rate_hz <= 0:
        return 1.0
    return max(0.05, 1.0 / rate_hz)


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
            if is_heater_object(base):
                objects[base] = ["temperature", "target"]
            else:
                objects[base] = None
        else:
            attrs = attribute_map.get(base, set())
            if is_heater_object(base):
                attrs = attrs | {"temperature", "target"}
            objects[base] = sorted(attrs)

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


def _merge_payload_dicts(target: Dict[str, Any], updates: Dict[str, Any]) -> None:
    """Merge payload dictionaries using shared deep_merge utility."""

    deep_merge(target, updates)


def _json_default(value: Any) -> Any:
    try:
        return str(value)
    except Exception:  # pragma: no cover - defensive fallback
        return repr(value)


__all__ = [
    "TelemetryConfigurationError",
    "TelemetryPublisher",
    "TelemetryOrchestrator",
    "MoonrakerStateStore",
    "build_subscription_manifest",
]
