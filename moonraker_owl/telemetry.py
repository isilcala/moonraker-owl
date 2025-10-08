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
        queue_size: int = 1,
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
                    self._process_payload(payload)
                except Exception:  # pragma: no cover - defensive logging
                    LOGGER.exception("Telemetry normalization failed")

                last_sent = loop.time()
                self._force_full_publish = False

        LOGGER.debug("TelemetryPublisher loop terminated")

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
            if force_full or previous is None:
                kind = "full"
                body = payload
            else:
                diff = _diff(previous, payload)
                if not diff:
                    return
                kind = "delta"
                body = diff

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


def _diff(
    previous: Optional[Dict[str, Any]], current: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    if previous is None:
        return copy.deepcopy(current)

    return _diff_dict(previous, current)


def _diff_dict(
    previous: Dict[str, Any], current: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    diff: Dict[str, Any] = {}

    previous_keys = set(previous.keys())
    current_keys = set(current.keys())

    for key in current_keys:
        current_value = current[key]
        if key not in previous:
            diff[key] = copy.deepcopy(current_value)
            continue

        previous_value = previous[key]
        if isinstance(previous_value, dict) and isinstance(current_value, dict):
            nested = _diff_dict(previous_value, current_value)
            if nested:
                diff[key] = nested
            continue

        if isinstance(previous_value, list) and isinstance(current_value, list):
            if previous_value != current_value:
                diff[key] = copy.deepcopy(current_value)
            continue

        if previous_value != current_value:
            diff[key] = copy.deepcopy(current_value)

    for key in previous_keys - current_keys:
        diff[key] = None

    return diff or None


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
