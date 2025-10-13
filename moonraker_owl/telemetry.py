"""Telemetry publishing pipeline."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import hashlib
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Protocol, Union, List

from .adapters import MQTTConnectionError
from .config import OwlConfig
from .core import PrinterAdapter, deep_merge
from .telemetry_normalizer import TelemetryNormalizer, _round_temperature
from .telemetry_state import TelemetryHasher, TelemetryStateCache
from . import constants
from .version import __version__

from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

LOGGER = logging.getLogger(__name__)

_ORIGIN = f"moonraker-owl@{__version__}"


# ----------------------------------------------------------------------
# Heater detection utilities
# ----------------------------------------------------------------------


def is_heater_object(obj_name: str) -> bool:
    """Check if an object name represents a heater (extruder, bed, or generic heater).

    Args:
        obj_name: Moonraker object name (e.g., "extruder", "heater_bed", "heater_generic chamber")

    Returns:
        True if the object is a heater, False otherwise

    Examples:
        >>> is_heater_object("extruder")
        True
        >>> is_heater_object("extruder1")
        True
        >>> is_heater_object("heater_bed")
        True
        >>> is_heater_object("heater_generic chamber")
        True
        >>> is_heater_object("fan")
        False
    """
    return obj_name in ("extruder", "heater_bed") or obj_name.startswith(
        ("extruder", "heater_generic")
    )


def get_heater_objects(objects: Dict[str, Any]) -> Dict[str, Optional[list[str]]]:
    """Extract heater objects from a subscription manifest.

    Args:
        objects: Dictionary of Moonraker object subscriptions

    Returns:
        Dictionary containing only heater objects, with None values to query all fields

    Examples:
        >>> get_heater_objects({"extruder": ["temp"], "fan": ["speed"]})
        {"extruder": None}
    """
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
        # Hard-coded QoS levels keep MQTT requirements explicit and consistent across deploys.
        self._channel_qos = {
            "status": 1,
            "progress": 1,
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
        self._normalizer = TelemetryNormalizer()
        self._hasher = TelemetryHasher()
        self._state_cache = TelemetryStateCache(
            ("status", "progress", "telemetry"), self._hasher
        )
        self._sequence_counter: Dict[str, int] = defaultdict(int)
        self._force_full_publish = False
        self._heater_merge_cache: Dict[str, Any] = {}

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
                heater_objects = get_heater_objects(self._subscription_objects or {})

                if heater_objects:
                    heater_state = await self._moonraker.fetch_printer_state(
                        heater_objects, timeout=5.0
                    )
                    # Merge the heater state into the notification payload
                    heater_status = (
                        heater_state.get("result", {}).get("status")
                        if isinstance(heater_state, dict)
                        else None
                    )

                    if heater_status:
                        has_new_details = self._track_heater_snapshot(heater_status)

                        if has_new_details:
                            await self._enqueue({"result": {"status": heater_status}})
                            self._state_cache.force_next_publish(
                                "telemetry", reason="heater refresh"
                            )
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
                if previous_snapshot != snapshot:
                    self._heater_merge_cache[key] = snapshot
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
                normalized.events,
                raw_json,
                force_full=False,
                ephemeral=True,
            )

    def _publish_channel(
        self,
        channel: str,
        payload: Optional[Union[Dict[str, Any], list[Any]]],
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
            sequence: Optional[int] = None
        else:
            if not self._validate_contract_payload(channel, payload):
                LOGGER.debug(
                    "Skipping publish for channel %s: invalid payload structure",
                    channel,
                )
                return

            decision = self._state_cache.evaluate(
                channel,
                payload,
                force_publish=force_full or self._force_full_publish,
                diff_callback=_summarise_change,
            )

            if not decision.should_publish:
                return

            kind = "full"
            body = payload
            sequence = decision.sequence

        document = self._build_envelope(
            channel, kind, body, raw_payload, sequence=sequence
        )
        topic = self._channel_topics[channel]
        self._publish(channel, topic, document)

    def _build_envelope(
        self,
        channel: str,
        kind: str,
        payload: Union[Dict[str, Any], list[Any]],
        raw_payload: str,
        *,
        sequence: Optional[int] = None,
    ) -> Dict[str, Any]:
        captured_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if sequence is None:
            sequence = self._next_sequence(channel)

        document: Dict[str, Any] = {
            "_ts": captured_at,
            "_seq": sequence,
            "schemaVersion": 1,
            "kind": kind,
            "channel": channel,
            "deviceId": self._device_id,
            "_origin": _ORIGIN,
        }

        # Only include raw Moonraker payload if configured
        # This saves ~450 bytes per message (41% bandwidth reduction)
        if self._config.telemetry.include_raw_payload:
            document["raw"] = raw_payload

        if self._printer_id:
            document["printerId"] = self._printer_id
        if self._tenant_id:
            document["tenantId"] = self._tenant_id

        if channel == "events":
            contract_events = _build_contract_events(
                payload if isinstance(payload, list) else []
            )
            document["events"] = contract_events
            return document

        if isinstance(payload, dict):
            if channel == "telemetry":
                contract_section = _build_contract_telemetry_section(payload)
                if contract_section:
                    document["telemetry"] = contract_section
            elif channel == "status":
                document["status"] = _build_contract_status_section(payload)
            elif channel == "progress":
                document["progress"] = _build_contract_progress_section(payload)

        return document

    def _next_sequence(self, channel: str) -> int:
        self._sequence_counter[channel] += 1
        return self._sequence_counter[channel]

    def _validate_contract_payload(
        self, channel: str, payload: Union[Dict[str, Any], list[Any]]
    ) -> bool:
        if channel in {"status", "progress", "telemetry"} and not isinstance(
            payload, dict
        ):
            LOGGER.debug(
                "Rejecting channel %s payload: expected mapping, got %s",
                channel,
                type(payload).__name__,
            )
            return False

        if channel == "telemetry" and isinstance(payload, dict):
            temperatures = payload.get("temperatures")
            if temperatures is not None:
                if not isinstance(temperatures, list):
                    LOGGER.debug(
                        "Telemetry temperatures block malformed: %r",
                        temperatures,
                    )
                    return False
                for entry in temperatures:
                    if not isinstance(entry, dict) or not entry.get("channel"):
                        LOGGER.debug(
                            "Telemetry temperature entry malformed: %r",
                            entry,
                        )
                        return False
        return True

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


def _build_contract_telemetry_section(payload: Dict[str, Any]) -> Dict[str, Any]:
    sensors: Dict[str, Dict[str, Any]] = {}

    for entry in payload.get("temperatures", []):
        if not isinstance(entry, dict):
            continue

        channel = entry.get("channel")
        if not isinstance(channel, str) or not channel.strip():
            continue

        sensor_key = _normalise_sensor_key(channel)
        sensor_payload = {
            "type": "heater",
            "unit": "celsius",
            "status": "ok",
        }

        actual = entry.get("actual")
        target = entry.get("target")

        if actual is not None:
            sensor_payload["value"] = actual
        if target is not None:
            sensor_payload["target"] = target

        sensors[sensor_key] = _prune_none(sensor_payload)

    for entry in payload.get("fans", []):
        if not isinstance(entry, dict):
            continue

        name = entry.get("name") or entry.get("channel")
        if not isinstance(name, str) or not name.strip():
            continue

        sensor_key = _normalise_sensor_key(name)
        percent = entry.get("percent")

        sensor_payload = {
            "type": "fan",
            "unit": "percent",
            "status": "ok",
        }

        if percent is not None:
            sensor_payload["value"] = percent

        sensors.setdefault(sensor_key, {}).update(_prune_none(sensor_payload))

    return {"sensors": sensors}


def _build_contract_status_section(payload: Dict[str, Any]) -> Dict[str, Any]:
    job_obj = payload.get("job")
    job: Dict[str, Any] = job_obj if isinstance(job_obj, dict) else {}

    progress_obj = job.get("progress") if job else None
    progress: Dict[str, Any] = progress_obj if isinstance(progress_obj, dict) else {}

    file_obj = job.get("file") if job else None
    file_info: Dict[str, Any] = file_obj if isinstance(file_obj, dict) else {}

    contract: Dict[str, Any] = {
        "state": _normalise_status_state(job.get("status")),
        "subState": _normalise_sub_state(job.get("subState"), job.get("status")),
        "jobId": _select_job_id(job, file_info),
    }

    message = job.get("message")
    if isinstance(message, str) and message.strip():
        contract["message"] = message.strip()

    percent = progress.get("percent")
    if isinstance(percent, (int, float)):
        contract["progressPercent"] = int(round(float(percent)))

    return contract


def _build_contract_progress_section(payload: Dict[str, Any]) -> Dict[str, Any]:
    job_obj = payload.get("job")
    job: Dict[str, Any] = job_obj if isinstance(job_obj, dict) else {}

    progress_obj = job.get("progress") if job else None
    progress: Dict[str, Any] = progress_obj if isinstance(progress_obj, dict) else {}

    file_obj = job.get("file") if job else None
    file_info: Dict[str, Any] = file_obj if isinstance(file_obj, dict) else {}

    contract: Dict[str, Any] = {
        "jobId": _select_job_id(job, file_info),
        "completionPercent": int(round(_coerce_float(progress.get("percent"), 0.0))),
        "elapsedSeconds": _coerce_int(progress.get("elapsedSeconds"), 0),
    }

    remaining = progress.get("remainingSeconds")
    if remaining is not None:
        contract["estimatedTimeRemainingSeconds"] = _coerce_int(remaining, 0)

    return contract


def _build_contract_events(entries: List[Any]) -> List[Dict[str, Any]]:
    contract_events: List[Dict[str, Any]] = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue

        base_code = entry.get("code") or entry.get("type") or "event"
        event_name = _normalise_event_name(base_code)
        severity_raw = entry.get("severity") or "info"
        severity = str(severity_raw).lower().strip() or "info"

        details: Dict[str, Any] = {
            "code": str(base_code),
        }

        message = entry.get("message")
        if not message:
            data = entry.get("data")
            if isinstance(data, dict):
                message = data.get("message") or data.get("detail")

        if isinstance(message, str) and message.strip():
            details["message"] = message.strip()

        payload: Dict[str, Any] = {"details": details}

        data = entry.get("data")
        if isinstance(data, dict) and data:
            payload["data"] = data

        contract_entry: Dict[str, Any] = {
            "eventName": event_name,
            "severity": severity,
            "payload": payload,
        }

        trace_id = entry.get("traceId")
        if isinstance(trace_id, str) and trace_id.strip():
            contract_entry["traceId"] = trace_id.strip()

        contract_events.append(contract_entry)

    return contract_events


def _normalise_sensor_key(name: str) -> str:
    stripped = name.strip()
    if not stripped:
        return name

    tokens = [token for token in _TOKEN_SPLIT_PATTERN.split(stripped) if token]
    if not tokens:
        return stripped

    head, *tail = tokens
    return head.lower() + "".join(part.capitalize() for part in tail)


_TOKEN_SPLIT_PATTERN = re.compile(r"[^0-9A-Za-z]+")


def _prune_none(mapping: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in mapping.items() if value is not None}


def _normalise_status_state(state: Any) -> str:
    if not isinstance(state, str) or not state.strip():
        return "Unknown"

    normalized = state.strip().replace("_", " ").lower()
    mapping = {
        "printing": "Printing",
        "paused": "Paused",
        "idle": "Idle",
        "error": "Error",
        "completed": "Completed",
        "cancelled": "Cancelled",
        "offline": "Offline",
    }
    return mapping.get(normalized, normalized.title())


def _normalise_sub_state(sub_state: Any, fallback_state: Any) -> str:
    if isinstance(sub_state, str) and sub_state.strip():
        return _normalise_status_state(sub_state)

    return _normalise_status_state(fallback_state)


def _select_job_id(job: Dict[str, Any], file_info: Dict[str, Any]) -> str:
    for candidate in (
        job.get("id"),
        job.get("jobId"),
        file_info.get("name") if isinstance(file_info, dict) else None,
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()

    return "unknown"


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _normalise_event_name(value: Any) -> str:
    if not isinstance(value, str):
        return "event"

    tokens = [token for token in _TOKEN_SPLIT_PATTERN.split(value.strip()) if token]
    if not tokens:
        return "event"

    head, *tail = tokens
    return head.lower() + "".join(part.capitalize() for part in tail)


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
            if is_heater_object(base):
                objects[base] = ["temperature", "target"]
            else:
                objects[base] = None
        else:
            # If specific attributes were requested, ensure heaters always include both temp fields
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


def _summarise_change(previous: Optional[Any], current: Any) -> str:
    if previous is None:
        return "initial snapshot"

    if previous == current:
        return "unchanged"

    if isinstance(previous, dict) and isinstance(current, dict):
        return _summarise_mapping_change(previous, current)

    if isinstance(previous, list) and isinstance(current, list):
        return _summarise_sequence_change(previous, current)

    return "payload changed"


def _summarise_mapping_change(previous: Dict[str, Any], current: Dict[str, Any]) -> str:
    diffs: List[str] = []
    for key in sorted(set(previous.keys()) | set(current.keys())):
        before = previous.get(key)
        after = current.get(key)
        if before == after:
            continue
        diffs.append(f"{key}={_summarise_value(before)}->{_summarise_value(after)}")

    if diffs:
        return ", ".join(diffs)

    return "structure mutated"


def _summarise_sequence_change(previous: List[Any], current: List[Any]) -> str:
    if len(previous) != len(current):
        return f"len {len(previous)}->{len(current)}"

    diffs: List[str] = []
    for idx, (before, after) in enumerate(zip(previous, current)):
        if before == after:
            continue
        diffs.append(f"[{idx}]={_summarise_value(before)}->{_summarise_value(after)}")
        if len(diffs) >= 3:
            break

    if diffs:
        return ", ".join(diffs)

    return "sequence mutated"


def _summarise_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return _stable_hash(value)
    return repr(value)


def _stable_hash(value: Any) -> str:
    try:
        payload = json.dumps(value, sort_keys=True, default=str).encode("utf-8")
    except Exception:
        payload = repr(value).encode("utf-8", errors="ignore")
    return hashlib.md5(payload).hexdigest()[:8]


def _summarise_heater_state(state: Any) -> str:
    if not isinstance(state, dict):
        return "missing"

    actual = state.get("temperature")
    target = state.get("target")
    return f"actual={actual} target={target}"
