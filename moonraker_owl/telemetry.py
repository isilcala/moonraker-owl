"""Telemetry publishing pipeline."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Protocol

from .adapters import MQTTConnectionError
from .config import OwlConfig
from .core import PrinterAdapter
from .telemetry_normalizer import TelemetryNormalizer
from . import constants

from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

LOGGER = logging.getLogger(__name__)


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
    """Listens to Moonraker updates and pushes filtered telemetry to MQTT."""

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
        self._channel_topics = {
            "status": f"{self._base_topic}/status",
            "progress": f"{self._base_topic}/progress",
            "telemetry": f"{self._base_topic}/telemetry",
            "events": f"{self._base_topic}/events",
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
        self._normalizer = TelemetryNormalizer()
        self._channel_snapshots: Dict[str, Optional[Dict[str, Any]]] = {
            "status": None,
            "progress": None,
            "telemetry": None,
        }
        self._sequence_counter: Dict[str, int] = defaultdict(int)
        self._force_full_publish = False

    async def start(self) -> None:
        if self._worker is not None:
            raise RuntimeError("TelemetryPublisher already started")

        self._loop = asyncio.get_running_loop()
        self._stop_event.clear()

        await self._moonraker.start(self._handle_moonraker_update)
        self._callback_registered = True

        self._force_full_publish = True
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

    def request_full_snapshot(self) -> None:
        """Force the next payload batch to be emitted as full snapshots."""

        self._force_full_publish = True

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
        # Obico strategy: When receiving notify_status_update, query heaters explicitly
        # to ensure we always get fresh target values (Moonraker omits unchanged fields)
        if payload.get("method") == "notify_status_update":
            try:
                # Query all heater objects with None (= all fields including target)
                heater_objects = {
                    obj: None
                    for obj in self._subscription_objects or {}
                    if obj in ("extruder", "heater_bed")
                    or obj.startswith(("extruder", "heater_generic"))
                }

                if heater_objects:
                    heater_state = await self._moonraker.fetch_printer_state(
                        heater_objects
                    )
                    # Merge the heater state into the notification payload
                    if "result" in heater_state and "status" in heater_state["result"]:
                        if (
                            "params" in payload
                            and isinstance(payload["params"], list)
                            and len(payload["params"]) > 0
                        ):
                            # Merge heater data into the first params entry
                            if isinstance(payload["params"][0], dict):
                                payload["params"][0].update(
                                    heater_state["result"]["status"]
                                )
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.debug("Failed to query heater state: %s", exc)

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

        pending: Optional[Dict[str, Any]] = None

        while not self._stop_event.is_set():
            if pending is None:
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
                LOGGER.exception("Telemetry normalization failed")

            last_sent = loop.time()
            self._force_full_publish = False
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
        normalized = self._normalizer.ingest(payload)
        raw_json = json.dumps(payload, default=_json_default)

        force_full = self._force_full_publish

        self._publish_channel(
            "status", normalized.status, raw_json, force_full=force_full
        )
        self._publish_channel(
            "progress", normalized.progress, raw_json, force_full=force_full
        )
        self._publish_channel(
            "telemetry", normalized.telemetry, raw_json, force_full=force_full
        )

        if normalized.events:
            self._publish_channel(
                "events",
                {"events": normalized.events},
                raw_json,
                force_full=False,
                ephemeral=True,
            )

    def _publish_channel(
        self,
        channel: str,
        payload: Optional[Dict[str, Any]],
        raw_payload: str,
        *,
        force_full: bool,
        ephemeral: bool = False,
    ) -> None:
        if not payload:
            return

        if ephemeral:
            kind = "delta"
            body = payload
        else:
            previous = self._channel_snapshots.get(channel)

            if not force_full and previous is not None and previous == payload:
                return

            kind = "full"
            body = payload
            self._channel_snapshots[channel] = copy.deepcopy(payload)

        document = self._build_envelope(channel, kind, body, raw_payload)
        topic = self._channel_topics[channel]
        self._publish(channel, topic, document)

    def _build_envelope(
        self,
        channel: str,
        kind: str,
        payload: Dict[str, Any],
        raw_payload: str,
    ) -> Dict[str, Any]:
        document: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "sequence": self._next_sequence(channel),
            "schemaVersion": 1,
            "kind": kind,
            "deviceId": self._device_id,
            "source": "moonraker",
            "raw": raw_payload,
        }

        if self._printer_id:
            document["printerId"] = self._printer_id
        if self._tenant_id:
            document["tenantId"] = self._tenant_id

        document.update(payload)
        return document

    def _next_sequence(self, channel: str) -> int:
        self._sequence_counter[channel] += 1
        return self._sequence_counter[channel]

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
                qos=1,
                retain=False,
                properties=properties,
            )
        except MQTTConnectionError as exc:
            LOGGER.warning("Telemetry publish failed: %s", exc)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception(
                "Unexpected error publishing telemetry for channel %s", channel
            )


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
            # For heater objects (extruder, heater_bed), explicitly subscribe to temperature AND target fields
            # to ensure we always get both values in every update, avoiding race conditions
            # where target may be omitted in rapid temperature updates.
            # Temperature sensors don't have target, so subscribe to all their fields.
            if base in ("extruder", "heater_bed") or base.startswith("extruder"):
                objects[base] = ["temperature", "target"]
            else:
                objects[base] = None
        else:
            # If specific attributes were requested, ensure heaters always include both temp fields
            attrs = attribute_map.get(base, set())
            if base in ("extruder", "heater_bed") or base.startswith("extruder"):
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
    for key, value in updates.items():
        existing = target.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            _merge_payload_dicts(existing, value)
        else:
            target[key] = copy.deepcopy(value)


def _json_default(value: Any) -> Any:
    try:
        return str(value)
    except Exception:  # pragma: no cover - defensive fallback
        return repr(value)
