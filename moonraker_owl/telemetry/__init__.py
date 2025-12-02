"""Telemetry publishing pipeline for the Owl contract."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import inspect
import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, Optional, Protocol, Sequence, Set, Tuple

from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .. import constants
from ..adapters import MQTTConnectionError
from ..config import OwlConfig
from ..core import PrinterAdapter, deep_merge
from .cadence import (
    ChannelCadenceController,
    ChannelDecision,
    ChannelRuntimeState,
    ChannelSchedule,
    TelemetryCadenceError,
)
from .event_types import Event, EventName, EventPriority, EventSeverity
from .events import EventCollector, RateLimitConfig
from .orchestrator import ChannelPayload, TelemetryOrchestrator
from .polling import DEFAULT_POLL_SPECS, PollSpec
from .state_store import MoonrakerStateStore
from .telemetry_state import TelemetryHasher

LOGGER = logging.getLogger(__name__)

# Re-export event types for external use
__all__ = [
    "ChannelCadenceController",
    "ChannelDecision",
    "ChannelSchedule",
    "Event",
    "EventCollector",
    "EventName",
    "EventPriority",
    "EventSeverity",
    "PollSpec",
    "RateLimitConfig",
    "TelemetryCadenceError",
    "TelemetryConfigurationError",
    "TelemetryPublisher",
]


def is_heater_object(obj_name: str) -> bool:
    """Return True when the Moonraker object represents a temperature device.

    This includes:
    - Heaters: extruder, heater_bed, heater_generic xxx (can set target)
    - Temperature fans: temperature_fan xxx (can set target)
    - Temperature sensors: temperature_sensor xxx (read-only)
    """
    return (
        obj_name in ("extruder", "heater_bed")
        or obj_name.startswith("extruder")
        or obj_name.startswith("heater_generic")
        or obj_name.startswith("temperature_sensor")
        or obj_name.startswith("temperature_fan")
    )


def heater_has_target(obj_name: str) -> bool:
    """Return True when the temperature object supports setting a target.

    Heaters and temperature fans can have their target set.
    Temperature sensors are read-only.
    """
    if obj_name.startswith("temperature_sensor"):
        return False
    return is_heater_object(obj_name)

class TelemetryConfigurationError(RuntimeError):
    """Raised when required telemetry configuration values are missing."""


@dataclass
class _ChannelPublishState:
    sequence: int = 0


@dataclass
class _PollGroup:
    name: str
    objects: dict[str, Optional[list[str]]]
    interval: float
    initial_delay: float


@dataclass
class _RateRequest:
    mode: str
    max_hz: float
    requested_at: datetime
    duration_seconds: Optional[int]
    expires_at: Optional[datetime]


@dataclass
class _PendingChannel:
    forced: bool = False
    respect_cadence: bool = False

    def merge(self, *, forced: bool, respect_cadence: bool) -> None:
        self.forced = self.forced or forced
        self.respect_cadence = self.respect_cadence or respect_cadence

    def clone(self) -> "_PendingChannel":
        return _PendingChannel(
            forced=self.forced,
            respect_cadence=self.respect_cadence,
        )


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
        poll_specs: Optional[Iterable[PollSpec]] = None,
    ) -> None:
        self._config = config
        self._moonraker = moonraker
        self._mqtt = mqtt

        (
            self._tenant_id,
            self._device_id,
            self._printer_id,
        ) = _resolve_printer_identity(config)

        self._cadence = config.telemetry_cadence

        self._base_topic = f"owl/printers/{self._device_id}"
        self._channel_topics: Dict[str, str] = {
            "status": f"{self._base_topic}/status",
            "sensors": f"{self._base_topic}/sensors",
            "events": f"{self._base_topic}/events",
        }
        self._channel_qos = {
            "status": 1,
            "sensors": 0,
            "events": 2,
        }
        self._channel_state: Dict[str, _ChannelPublishState] = {
            name: _ChannelPublishState() for name in self._channel_topics
        }
        self._hasher = TelemetryHasher()
        self._cadence_controller = ChannelCadenceController(
            monotonic=time.monotonic,
            hasher=self._hasher,
        )
        self._status_error_snapshot_active = False
        self._status_listeners: list[Callable[[Dict[str, Any]], Any]] = []

        self._min_interval = 0.05
        idle_hz = config.telemetry.rate_hz or 0.033
        self._idle_hz = idle_hz
        self._idle_interval = _hz_to_interval(idle_hz) or 30.0
        self._watch_interval = _hz_to_interval(1.0) or 1.0
        self._sensors_interval = self._idle_interval
        # Note: events channel does NOT use cadence controller.
        # Rate limiting for events is handled by EventCollector's token bucket algorithm.
        # See EventCollector.harvest() and RateLimitConfig in events.py.
        self._include_fields = _normalise_fields(config.include_fields)
        self._exclude_fields = _normalise_fields(config.exclude_fields)
        self._subscription_objects: dict[str, Optional[list[str]]] = (
            build_subscription_manifest(self._include_fields, self._exclude_fields)
        )
        self._moonraker.set_subscription_objects(self._subscription_objects)

        self._poll_specs = tuple(poll_specs or DEFAULT_POLL_SPECS)
        self._poll_groups = self._build_poll_groups()
        self._poll_tasks: list[asyncio.Task[None]] = []

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max(queue_size, 1))
        self._event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._worker: Optional[asyncio.Task[None]] = None
        self._callback_registered = False
        self._pending_payload: Optional[Dict[str, Any]] = None
        self._pending_timer_handle: Optional[asyncio.TimerHandle] = None
        self._pending_channels: Dict[str, _PendingChannel] = {}
        self._force_full_channels_after_reset: Set[str] = set()
        self._active_rate_request: Optional[_RateRequest] = None
        self._last_printer_state: Optional[str] = None
        self._resubscribe_task: Optional[asyncio.Task[None]] = None
        # Cache the last listener state we logged so debug output only fires on transitions.
        self._last_listener_state: Optional[Tuple[Optional[str], Optional[str], Optional[str]]] = None
        self._klippy_ready_applied = False

        self._last_status_publish_time = 0.0
        self._last_status_status = "Idle"
        self._last_payload_snapshot: Optional[Dict[str, Any]] = None
        self._status_idle_interval = self._cadence.status_idle_interval_seconds
        self._status_active_interval = (
            self._cadence.status_active_interval_seconds
        )
        self._sensors_force_publish_seconds = self._cadence.sensors_force_publish_seconds
        self._watch_window_expires: Optional[datetime] = None
        self._current_mode = "idle"
        self._bootstrapped = False

        self._orchestrator = TelemetryOrchestrator(
            clock=lambda: datetime.now(timezone.utc),
            cadence=self._cadence,
        )
        self._orchestrator.set_sensors_mode(
            mode="idle",
            max_hz=1.0 / self._idle_interval if self._idle_interval > 0 else 0.0,
            watch_window_expires=None,
        )

        # Emit an initial cadence log to capture baseline configuration.
        self.apply_sensors_rate(
            mode="idle",
            max_hz=1.0 / self._idle_interval if self._idle_interval > 0 else 0.0,
            duration_seconds=None,
            requested_at=datetime.now(timezone.utc),
        )

    async def start(self) -> None:
        await self._dispose_worker(remove_callback=True)

        self._loop = asyncio.get_running_loop()
        self._stop_event.clear()
        self._reset_runtime_state()

        # Discover available heaters and sensors before subscribing
        await self._discover_and_subscribe_sensors()

        await self._prime_initial_state()

        callback_registered = False
        try:
            await self._moonraker.start(self._handle_moonraker_update)
        except ValueError as exc:
            message = str(exc)
            if "callback already registered" not in message.lower():
                raise
            # Normal re-entry scenario - callback already registered
            callback_registered = True
        else:
            callback_registered = True

        self._callback_registered = callback_registered

        self._worker = asyncio.create_task(self._run())
        self._start_pollers()

    async def _discover_and_subscribe_sensors(self) -> None:
        """Discover all available heaters and sensors and update subscriptions.

        This queries Moonraker's heaters object to find all configured temperature
        devices (heaters, sensors, fans) and dynamically adds them to the
        subscription manifest.
        """
        try:
            heater_info = await self._moonraker.fetch_available_heaters()
        except Exception as exc:
            LOGGER.warning("Failed to discover heaters/sensors: %s", exc)
            return

        available_heaters = heater_info.get("available_heaters", [])
        available_sensors = heater_info.get("available_sensors", [])

        added_objects: list[str] = []

        # Always subscribe to main fan (part cooling fan) if not already subscribed
        if "fan" not in self._subscription_objects:
            self._subscription_objects["fan"] = ["speed"]
            added_objects.append("fan")

        # Add heaters (extruder, heater_bed, heater_generic xxx)
        for heater in available_heaters:
            if heater and heater not in self._subscription_objects:
                # Skip private objects (starting with _)
                if heater.startswith("_") or (
                    " " in heater and heater.split(" ", 1)[1].startswith("_")
                ):
                    continue
                self._subscription_objects[heater] = ["temperature", "target"]
                added_objects.append(heater)

        # Add sensors (temperature_sensor xxx, temperature_fan xxx, etc.)
        for sensor in available_sensors:
            if sensor and sensor not in self._subscription_objects:
                # Skip private objects (starting with _)
                if sensor.startswith("_") or (
                    " " in sensor and sensor.split(" ", 1)[1].startswith("_")
                ):
                    continue
                # Skip if it's already a heater (to avoid duplicate subscriptions)
                if sensor in available_heaters:
                    continue
                # temperature_fan has target, temperature_sensor does not
                if sensor.startswith("temperature_fan"):
                    self._subscription_objects[sensor] = ["temperature", "target", "speed"]
                else:
                    self._subscription_objects[sensor] = ["temperature"]
                added_objects.append(sensor)

        if added_objects:
            LOGGER.debug(
                "Discovered %d temperature sensors: %s",
                len(added_objects),
                ", ".join(added_objects),
            )
            # Update subscription on the moonraker client
            self._moonraker.set_subscription_objects(self._subscription_objects)

    async def stop(self) -> None:
        self._stop_event.set()
        self._event.set()

        await self._cancel_pollers()

        await self._dispose_worker(remove_callback=True)
        # Removed: Agent no longer sets retained flag, Nexus snapshot provides authoritative state
        self._cancel_pending_timer()
        self._pending_channels.clear()
        resubscribe_task = self._resubscribe_task
        if resubscribe_task is not None:
            resubscribe_task.cancel()
            self._resubscribe_task = None

    @property
    def topic(self) -> str:
        return self._channel_topics["sensors"]

    async def _dispose_worker(self, *, remove_callback: bool = False) -> None:
        worker = self._worker
        if worker is not None:
            if not worker.done():
                worker.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker
            self._worker = None

        if remove_callback and self._callback_registered:
            self._moonraker.remove_callback(self._handle_moonraker_update)
            self._callback_registered = False

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

        # NOTE: Moonraker callbacks execute on the asyncio event loop thread; stateful
        # collaborators rely on this single-threaded access. Revisit if we add
        # background workers that mutate shared state.
        if self._queue.full():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass

        await self._queue.put(payload)
        self._event.set()

    def _build_poll_groups(self) -> list[_PollGroup]:
        if not self._poll_specs:
            return []

        subscribed = set(self._subscription_objects.keys())
        groups: list[_PollGroup] = []

        for spec in self._poll_specs:
            manifest = build_subscription_manifest(spec.fields, ())
            
            # Check if this spec should force polling even for subscribed objects
            force_poll = getattr(spec, 'force_poll', False)
            
            if force_poll:
                # Include all objects from the manifest, even if already subscribed
                filtered = {
                    key: value
                    for key, value in manifest.items()
                    if key
                }
            else:
                # Filter out already-subscribed objects (original behavior)
                filtered = {
                    key: value
                    for key, value in manifest.items()
                    if key and key not in subscribed
                }

            if not filtered:
                continue

            interval = spec.interval_seconds if spec.interval_seconds > 0 else 60.0
            initial_delay = (
                spec.initial_delay_seconds if spec.initial_delay_seconds > 0 else 0.0
            )

            groups.append(
                _PollGroup(
                    name=spec.name,
                    objects=filtered,
                    interval=interval,
                    initial_delay=initial_delay,
                )
            )

        # Log poll groups for debugging
        for group in groups:
            LOGGER.debug(
                "Poll group '%s': interval=%.1fs objects=%s",
                group.name,
                group.interval,
                list(group.objects.keys()),
            )

        return groups

    def _start_pollers(self) -> None:
        if self._poll_tasks or not self._poll_groups:
            return

        for group in self._poll_groups:
            task = asyncio.create_task(self._poll_loop(group))
            self._poll_tasks.append(task)

    async def _cancel_pollers(self) -> None:
        if not self._poll_tasks:
            return

        for task in self._poll_tasks:
            task.cancel()

        for task in self._poll_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._poll_tasks.clear()

    async def _poll_loop(self, group: _PollGroup) -> None:
        if not group.objects:
            return

        delay = max(group.initial_delay, 0.0)
        if delay:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass

        while not self._stop_event.is_set():
            try:
                payload = await self._moonraker.fetch_printer_state(group.objects)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                pass  # Polling failure handled by circuit breaker
            else:
                await self._enqueue(payload)

            interval = max(group.interval, 0.1)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                continue

    async def _run(self) -> None:
        loop = self._loop or asyncio.get_running_loop()
        last_sent = 0.0

        pending: Optional[Dict[str, Any]] = None
        pending_overrides: Dict[str, _PendingChannel] = {}

        while not self._stop_event.is_set():
            if pending is None:
                if self._pending_payload is not None:
                    pending = copy.deepcopy(self._pending_payload)
                    pending_overrides = {
                        channel: entry.clone()
                        for channel, entry in self._pending_channels.items()
                    }
                    self._pending_payload = None
                    self._pending_channels.clear()
                    self._cancel_pending_timer()
                else:
                    heartbeat_payload = self._prepare_heartbeat_payload()
                    if heartbeat_payload is not None:
                        pending = heartbeat_payload
                        pending_overrides = {
                            "status": _PendingChannel(forced=True)
                        }
                    else:
                        await self._event.wait()
                        self._event.clear()
                        pending = self._gather_payloads()
                        pending_overrides = {}
                        if pending is None:
                            continue
            else:
                heartbeat_payload = self._prepare_heartbeat_payload()
                if heartbeat_payload is not None:
                    pending = heartbeat_payload
                    pending_overrides = {
                        "status": _PendingChannel(forced=True)
                    }

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
                    pending_overrides = {}
                    break

            if pending is None:
                continue

            pending = self._gather_payloads(pending)
            if pending is None:
                pending_overrides = {}
                continue

            try:
                self._process_payload(
                    pending,
                    overrides=pending_overrides,
                )
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("Telemetry pipeline failed")

            last_sent = loop.time()
            pending = self._gather_payloads()
            pending_overrides = {}

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

    def _process_payload(
        self,
        payload: Dict[str, Any],
        *,
        overrides: Optional[Dict[str, _PendingChannel]] = None,
    ) -> None:
        self._maybe_expire_watch_window()
        self._orchestrator.ingest(payload)
        listener_snapshot = copy.deepcopy(payload)
        aggregated_status = self._orchestrator.store.as_dict()
        if aggregated_status:
            listener_snapshot.setdefault("result", {})["status"] = copy.deepcopy(
                aggregated_status
            )
        self._notify_status_listeners(listener_snapshot)

        self._handle_klippy_state(payload, aggregated_status)

        has_status = bool(aggregated_status)
        if not self._bootstrapped:
            if has_status:
                self._bootstrapped = True
                LOGGER.info(
                    "Telemetry pipeline bootstrapped after receiving initial Moonraker status"
                )

        if self._bootstrapped and has_status:
            self._last_payload_snapshot = copy.deepcopy(payload)
        elif not self._bootstrapped:
            self._last_payload_snapshot = None

        overrides = overrides or {}
        forced_channels_set = {
            channel
            for channel, entry in overrides.items()
            if entry.forced
        }
        forced_channels_set.update(self._force_full_channels_after_reset)

        frames = self._orchestrator.build_payloads(forced_channels=forced_channels_set)
        if not frames:
            return

        include_raw = bool(self._config.telemetry.include_raw_payload)
        raw_json: Optional[str] = None
        if include_raw:
            raw_json = json.dumps(payload, default=_json_default)

        now_monotonic = time.monotonic()

        deferred = False
        min_delay: Optional[float] = None

        skipped_pre_bootstrap = False

        for channel, frame in frames.items():
            topic = self._channel_topics.get(channel)
            if topic is None:
                continue

            if not self._bootstrapped and channel != "events":
                skipped_pre_bootstrap = True
                continue

            if channel in self._force_full_channels_after_reset:
                frame.forced = True

            # Events channel bypasses cadence controller entirely.
            # It is purely event-driven with rate limiting handled by EventCollector.
            if channel == "events":
                # Always publish events immediately when present
                pass
            else:
                # Status and sensors channels use cadence controller
                override = overrides.get(channel)
                override_forced = override.forced if override else False
                override_respect_cadence = override.respect_cadence if override else False
                force_due_to_reset = channel in self._force_full_channels_after_reset

                decision = self._cadence_controller.evaluate(
                    channel,
                    frame.payload,
                    explicit_force=frame.forced or override_forced or force_due_to_reset,
                    respect_cadence=override_respect_cadence,
                    allow_force_publish=(channel == "sensors"),
                )

                if not decision.should_publish:
                    if decision.delay_seconds is not None:
                        deferred = True
                        if min_delay is None or decision.delay_seconds < min_delay:
                            min_delay = decision.delay_seconds
                        pending_entry = self._pending_channels.get(channel)
                        if pending_entry is None:
                            pending_entry = _PendingChannel()
                            self._pending_channels[channel] = pending_entry
                        enforce_cadence = decision.reason in {"cadence", "forced-rate"}
                        pending_entry.merge(
                            forced=enforce_cadence,
                            respect_cadence=enforce_cadence,
                        )
                    else:
                        self._pending_channels.pop(channel, None)
                    continue

                self._pending_channels.pop(channel, None)

            envelope = self._wrap_envelope(
                channel,
                frame,
                include_raw=include_raw,
                raw_json=raw_json,
            )

            if channel == "status":
                status_body = envelope.get("status")
                if isinstance(status_body, dict):
                    state_value = status_body.get("printerStatus")
                    if isinstance(state_value, str):
                        normalized_state = state_value.strip()
                        if normalized_state:
                            self._last_status_status = state_value
                            self._last_status_publish_time = now_monotonic
                            state_lower = normalized_state.lower()
                            if state_lower == "error":
                                self._status_error_snapshot_active = True
                            elif self._status_error_snapshot_active:
                                self._status_error_snapshot_active = False

            # Agent never uses retained messages - Nexus snapshot provides authority
            self._publish(channel, topic, envelope, retain=False)
            self._force_full_channels_after_reset.discard(channel)

        if deferred:
            self._store_pending_payload(payload)
            if min_delay is not None:
                self._schedule_cadence_check(min_delay)

        if not self._bootstrapped and skipped_pre_bootstrap:
            self._orchestrator.status_selector.reset()
            self._orchestrator.sensors_selector.reset()

    def _prepare_heartbeat_payload(self) -> Optional[Dict[str, Any]]:
        if not self._should_emit_status_heartbeat():
            return None

        if self._last_payload_snapshot is None:
            return None

        return copy.deepcopy(self._last_payload_snapshot)

    def _should_emit_status_heartbeat(self) -> bool:
        if self._last_status_publish_time == 0.0:
            return False

        interval = (
            self._status_active_interval
            if self._last_status_status in {"Printing", "Paused"}
            else self._status_idle_interval
        )

        return time.monotonic() - self._last_status_publish_time >= interval

    def _maybe_expire_watch_window(self, *, now: Optional[datetime] = None) -> None:
        if self._current_mode == "idle":
            return

        expires_at = self._watch_window_expires
        if expires_at is None:
            return

        current_time = now or datetime.now(timezone.utc)
        if current_time < expires_at:
            return

        LOGGER.debug("Watch window expired, reverting to idle")
        self.apply_sensors_rate(
            mode="idle",
            max_hz=self._idle_hz,
            duration_seconds=None,
            requested_at=current_time,
        )

    def get_current_sensors_state(
        self,
    ) -> tuple[str, float, Optional[datetime]]:
        """Return the current sensors cadence state.

        Returns:
            A tuple of (mode, interval, watch_window_expires).
        """
        return (self._current_mode, self._sensors_interval, self._watch_window_expires)

    def get_current_print_filename(self) -> Optional[str]:
        """Return the current print filename from the orchestrator's state store.

        Returns:
            The filename of the current/last print, or None if not available.
        """
        return self._orchestrator.store.print_filename

    def extend_watch_window(
        self,
        *,
        duration_seconds: int,
        requested_at: Optional[datetime] = None,
    ) -> Optional[datetime]:
        """Extend the current watch window without reconfiguring cadence.

        This is an optimization for repeated sensors:set-rate commands that
        request the same mode and interval. Instead of reconfiguring the
        entire cadence pipeline, we simply extend the expiration time.

        Args:
            duration_seconds: New duration from requested_at.
            requested_at: Base timestamp for computing expiration.

        Returns:
            The new expiration datetime, or None if not in watch mode.
        """
        if self._current_mode != "watch":
            return None

        requested_at = requested_at or datetime.now(timezone.utc)
        new_expires = requested_at + timedelta(seconds=duration_seconds)
        old_expires = self._watch_window_expires

        self._watch_window_expires = new_expires
        self._orchestrator.set_sensors_mode(
            mode=self._current_mode,
            max_hz=1.0 / self._sensors_interval if self._sensors_interval > 0 else 0.0,
            watch_window_expires=new_expires,
        )

        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug(
                "Extended watch window: %s -> %s (duration=%ds)",
                old_expires.isoformat() if old_expires else "<none>",
                new_expires.isoformat(),
                duration_seconds,
            )

        return new_expires

    def apply_sensors_rate(
        self,
        *,
        mode: str,
        max_hz: float,
        duration_seconds: Optional[int],
        requested_at: Optional[datetime] = None,
    ) -> Optional[datetime]:
        mode = (mode or "idle").strip().lower() or "idle"
        max_hz = max(0.0, max_hz)
        requested_at = requested_at or datetime.now(timezone.utc)
        normalized_duration = (
            duration_seconds if duration_seconds and duration_seconds > 0 else None
        )
        expires_at: Optional[datetime] = None
        if normalized_duration is not None:
            expires_at = requested_at + timedelta(seconds=normalized_duration)

        interval = _hz_to_interval(max_hz)
        if mode == "watch":
            interval = min(interval or self._watch_interval, self._watch_interval)
        else:
            interval = interval or self._idle_interval

        interval = interval or self._idle_interval
        effective_hz = 0.0 if interval <= 0 else 1.0 / interval

        previous_mode = self._current_mode
        previous_interval = self._sensors_interval

        self._sensors_interval = interval
        self._watch_window_expires = expires_at
        if interval > 0:
            self._sensors_force_publish_seconds = max(300.0, interval * 5)
        self._current_mode = mode
        self._refresh_channel_schedules()
        self._orchestrator.set_sensors_mode(
            mode=mode,
            max_hz=effective_hz,
            watch_window_expires=expires_at,
        )

        if mode != previous_mode:
            LOGGER.info(
                "Telemetry mode: %s -> %s (interval=%.1fs)",
                previous_mode,
                mode,
                interval,
            )

        for channel in ("sensors", "status"):
            self._cadence_controller.reset_channel(channel)

        self._event.set()
        self._active_rate_request = _RateRequest(
            mode=mode,
            max_hz=max_hz,
            requested_at=requested_at,
            duration_seconds=normalized_duration,
            expires_at=expires_at,
        )
        return expires_at

    def _refresh_channel_schedules(self) -> None:
        status_interval: Optional[float] = None
        sensors_interval = (
            self._sensors_interval if self._sensors_interval and self._sensors_interval > 0 else None
        )
        # Force cadence only tails the watch profile; once we drop back to idle we allow
        # forced publishes to follow the broader idle cadence rather than remaining at 1 Hz.
        if self._current_mode == "watch":
            sensors_forced_interval = (
                self._watch_interval if self._watch_interval and self._watch_interval > 0 else None
            )
        else:
            sensors_forced_interval = (
                sensors_interval if sensors_interval and sensors_interval > 0 else None
            )
        sensors_force_publish = (
            self._sensors_force_publish_seconds
            if self._sensors_force_publish_seconds and self._sensors_force_publish_seconds > 0
            else None
        )

        if "status" in self._channel_topics:
            self._cadence_controller.configure(
                "status",
                interval=status_interval,
                forced_interval=None,
                force_publish_seconds=None,
            )

        if "sensors" in self._channel_topics:
            self._cadence_controller.configure(
                "sensors",
                interval=sensors_interval,
                forced_interval=sensors_forced_interval,
                force_publish_seconds=sensors_force_publish,
            )

        # Note: events channel is NOT configured in cadence controller.
        # It is purely event-driven with rate limiting handled by EventCollector.

    def record_command_state(
        self,
        *,
        command_id: str,
        command_type: str,
        state: str,
        session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._orchestrator.events.record_command_state(
            command_id=command_id,
            command_type=command_type,
            state=state,
            session_id=session_id,
            details=details,
        )
        self._event.set()

    def set_print_state_callback(
        self, callback: Optional[Callable[[str], None]]
    ) -> None:
        """Set callback for print state changes.

        This allows the CommandProcessor to be notified when print state
        changes, enabling state-based command completion ACKs.

        Args:
            callback: Function accepting the new print state string,
                     or None to remove the callback.
        """
        self._orchestrator.set_print_state_callback(callback)

    def _wrap_envelope(
        self,
        channel: str,
        frame: ChannelPayload,
        *,
        include_raw: bool,
        raw_json: Optional[str],
    ) -> Dict[str, Any]:
        state = self._channel_state[channel]
        state.sequence += 1

        document: Dict[str, Any] = {
            "_schema": 1,
            "kind": "full",
            "_ts": frame.observed_at.replace(microsecond=0).isoformat(),
            "_origin": self._orchestrator.origin,
            "_seq": state.sequence,
            "sessionId": frame.session_id,
            channel: frame.payload,
            "deviceId": self._device_id,
        }

        if self._tenant_id:
            document["tenantId"] = self._tenant_id
        if self._printer_id:
            document["printerId"] = self._printer_id
        if include_raw and raw_json is not None:
            document["raw"] = raw_json

        return document

    def _publish(
        self,
        channel: str,
        topic: str,
        document: Dict[str, Any],
        *,
        retain: bool = False,
    ) -> None:
        payload_bytes = json.dumps(document, default=_json_default).encode("utf-8")
        properties = Properties(PacketTypes.PUBLISH)
        # Note: Device authentication now handled via JWT (MQTT password)
        # No need to attach device_token as MQTT user property

        try:
            self._mqtt.publish(
                topic,
                payload_bytes,
                qos=self._channel_qos.get(channel, 1),
                retain=retain,
                properties=properties,
            )
        except MQTTConnectionError as exc:
            LOGGER.warning("Telemetry publish failed: %s", exc)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception(
                "Unexpected error publishing telemetry for channel %s", channel
            )

    def _store_pending_payload(self, payload: Dict[str, Any]) -> None:
        if self._pending_payload is None:
            self._pending_payload = copy.deepcopy(payload)
        else:
            _merge_payload_dicts(self._pending_payload, payload)

    def _schedule_cadence_check(self, delay: float) -> None:
        if self._loop is None:
            return
        if delay <= 0:
            self._event.set()
            return

        handle = self._pending_timer_handle
        if handle is not None and not handle.cancelled():
            current_remaining = handle.when() - self._loop.time()
            if current_remaining <= delay + 1e-6:
                return
            handle.cancel()

        def _trigger() -> None:
            self._pending_timer_handle = None
            self._event.set()

        self._pending_timer_handle = self._loop.call_later(delay, _trigger)

    def _cancel_pending_timer(self) -> None:
        if self._pending_timer_handle is not None:
            self._pending_timer_handle.cancel()
            self._pending_timer_handle = None

    def _reset_runtime_state(self) -> None:
        previous_snapshot = copy.deepcopy(self._last_payload_snapshot)

        resubscribe_task = self._resubscribe_task
        if resubscribe_task is not None:
            resubscribe_task.cancel()
            self._resubscribe_task = None

        self._orchestrator.reset()
        self._orchestrator.set_sensors_mode(
            mode="idle",
            max_hz=1.0 / self._idle_interval if self._idle_interval > 0 else 0.0,
            watch_window_expires=None,
        )
        self._current_mode = "idle"
        self._sensors_interval = self._idle_interval
        self._sensors_force_publish_seconds = self._cadence.sensors_force_publish_seconds
        self._watch_window_expires = None
        self._bootstrapped = False
        self._cadence_controller.reset_all()
        self._refresh_channel_schedules()
        self._pending_payload = None
        self._pending_channels.clear()
        self._last_payload_snapshot = None
        self._last_status_publish_time = 0.0
        self._last_status_status = "Idle"
        self._status_error_snapshot_active = False
        self._last_printer_state = None
        # Removed: Agent no longer sets retained flag on reset
        if self._queue is not None:
            while True:
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        self._reapply_rate_request()
        self._force_full_channels_after_reset.clear()
        self._force_full_channels_after_reset.update({"status", "sensors"})

        if previous_snapshot is not None:
            self._pending_payload = copy.deepcopy(previous_snapshot)
            self._pending_channels["status"] = _PendingChannel(
                forced=True,
                respect_cadence=False,
            )
            self._pending_channels["sensors"] = _PendingChannel(
                forced=True,
                respect_cadence=False,
            )
            self._last_payload_snapshot = copy.deepcopy(previous_snapshot)
            self._event.set()
        self._klippy_ready_applied = False

    def _reapply_rate_request(self, *, now: Optional[datetime] = None) -> None:
        request = self._active_rate_request
        if request is None:
            return

        current_time = now or datetime.now(timezone.utc)

        remaining_seconds: Optional[int]
        if request.expires_at is None:
            remaining_seconds = None
        else:
            remaining = (request.expires_at - current_time).total_seconds()
            if remaining <= 0:
                self._active_rate_request = None
                return
            remaining_seconds = math.ceil(remaining)

        LOGGER.debug(
            "Reapplying telemetry cadence: mode=%s remaining=%ss",
            request.mode,
            remaining_seconds if remaining_seconds is not None else "indefinite",
        )

        self.apply_sensors_rate(
            mode=request.mode,
            max_hz=request.max_hz,
            duration_seconds=remaining_seconds,
            requested_at=current_time,
        )

    async def publish_system_status(
        self,
        *,
        printer_state: str,
        message: Optional[str] = None,
    ) -> None:
        """Publish a system-level status update (e.g., error state).

        This is used to communicate system-level errors like Moonraker unavailability.
        Agent never uses retained messages - Nexus snapshot provides authority.
        """
        if self._stop_event.is_set():
            return

        normalized_state = (printer_state or "").strip().lower() or "error"
        detail = (message or "Moonraker unavailable").strip() or "Moonraker unavailable"

        status_payload: Dict[str, Any] = {
            "result": {
                "status": {
                    "print_stats": {
                        "state": normalized_state,
                        "message": detail,
                    },
                    "display_status": {
                        "message": detail,
                    },
                    "webhooks": {
                        "state": normalized_state,
                    },
                }
            }
        }

        snapshot = self._orchestrator.store.export_state()
        try:
            self._process_payload(
                status_payload,
                overrides={
                    "status": _PendingChannel(
                        forced=True,
                        respect_cadence=False,
                    )
                },
            )
        finally:
            self._orchestrator.store.restore_state(snapshot)

    def register_status_listener(
        self,
        listener: Callable[[Dict[str, Any]], Any],
    ) -> None:
        if listener in self._status_listeners:
            return
        self._status_listeners.append(listener)

    def unregister_status_listener(
        self,
        listener: Callable[[Dict[str, Any]], Any],
    ) -> None:
        with contextlib.suppress(ValueError):
            self._status_listeners.remove(listener)

    def _notify_status_listeners(self, payload: Dict[str, Any]) -> None:
        if not self._status_listeners:
            return

        if LOGGER.isEnabledFor(logging.DEBUG):
            status = payload.get("result", {}).get("status", {})

            def _read_state(section: Any) -> Optional[str]:
                if isinstance(section, Mapping):
                    value = section.get("state")
                    if isinstance(value, str):
                        return value
                return None

            webhooks_state = _read_state(status.get("webhooks"))
            printer_state = _read_state(status.get("printer"))
            print_state = _read_state(status.get("print_stats"))
            listener_state = (webhooks_state, printer_state, print_state)
            if listener_state != self._last_listener_state:
                # Only surface changes so steady-state polling does not spam the log stream.
                LOGGER.debug(
                    "Dispatching status listeners: webhooks=%s printer=%s print_stats=%s",
                    webhooks_state,
                    printer_state,
                    print_state,
                )
                self._last_listener_state = listener_state
        else:
            self._last_listener_state = None

        listeners = tuple(self._status_listeners)
        for listener in listeners:
            try:
                outcome = listener(payload)
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.warning("Status listener raised unexpected exception", exc_info=True)
                continue

            if inspect.isawaitable(outcome):
                self._schedule_listener_awaitable(outcome)

    def _handle_klippy_state(
        self,
        payload: Dict[str, Any],
        aggregated_status: Dict[str, Any],
    ) -> None:
        previous_state = self._last_printer_state
        new_state = previous_state
        ready_signal = False
        reset_ready_flag = False

        printer_section = aggregated_status.get("printer")
        if isinstance(printer_section, Mapping):
            state_value = printer_section.get("state")
            if isinstance(state_value, str):
                normalized = state_value.strip().lower()
                if normalized:
                    new_state = normalized

        method = payload.get("method")
        if isinstance(method, str):
            normalized_method = method.strip().lower()
            if normalized_method == "notify_klippy_ready":
                new_state = "ready"
                ready_signal = True
            elif normalized_method == "notify_klippy_state":
                candidate = _extract_state_from_params(payload.get("params"))
                if candidate:
                    new_state = candidate
                    if candidate == "ready":
                        ready_signal = True
                    elif candidate in {"startup", "shutdown", "error"}:
                        reset_ready_flag = True
            elif normalized_method in {
                "notify_klippy_shutdown",
                "notify_klippy_disconnected",
            }:
                reset_ready_flag = True

        if new_state == previous_state and not ready_signal:
            return

        self._last_printer_state = new_state
        if reset_ready_flag and new_state != "ready":
            self._klippy_ready_applied = False

        if new_state == "ready":
            if not ready_signal and self._klippy_ready_applied:
                return
            if self._klippy_ready_applied and ready_signal:
                return
            # On a Klippy reboot the websocket keeps running but loses subscriptions; refresh them lazily.
            LOGGER.debug("Klippy ready, refreshing subscriptions")
            self._klippy_ready_applied = True
            self._schedule_resubscribe("klippy-ready")
        elif ready_signal:
            self._klippy_ready_applied = False

    def _schedule_resubscribe(self, reason: str) -> None:
        if not hasattr(self._moonraker, "resubscribe"):
            return

        loop = self._loop
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return

        existing_task = self._resubscribe_task
        if existing_task is not None and not existing_task.done():
            return

        async def _runner() -> None:
            try:
                await self._moonraker.resubscribe()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception(
                    "Failed to resend Moonraker subscription after %s", reason
                )
            try:
                await self._prime_initial_state()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.warning(
                    "Failed to refresh Moonraker snapshot after %s: %s",
                    reason,
                    exc,
                )
            finally:
                self._resubscribe_task = None

        # Resubscribe asynchronously so we do not block the main callback pipeline.
        try:
            self._resubscribe_task = loop.create_task(_runner())
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.warning(
                "Unable to schedule Moonraker resubscribe task (%s)", reason,
                exc_info=True,
            )
            self._resubscribe_task = None

    def _schedule_listener_awaitable(self, awaitable: Awaitable[Any]) -> None:
        loop = self._loop
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
        if loop is None:
            return

        async def _runner() -> None:
            try:
                await asyncio.shield(awaitable)
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.warning("Status listener awaitable raised", exc_info=True)

        try:
            loop.create_task(_runner())
        except Exception:  # pragma: no cover - defensive logging
            pass  # Task scheduling failed - no recovery possible


def _extract_state_from_params(params: Any) -> Optional[str]:
    if isinstance(params, str):
        normalized = params.strip().lower()
        return normalized or None

    if isinstance(params, Mapping):
        value = params.get("state")
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized:
                return normalized
        candidates = params.values()
    elif isinstance(params, Sequence):
        candidates = params
    else:
        return None

    for entry in candidates:
        result = _extract_state_from_params(entry)
        if result:
            return result
    return None


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
            "Device ID is required for telemetry publishing. "
            "Please link your device: moonraker-owl link"
        )

    if not printer_id:
        LOGGER.warning(
            "Printer ID missing from configuration; telemetry payload will omit it"
        )

    return tenant_id, device_id, printer_id


def _hz_to_interval(rate_hz: float) -> Optional[float]:
    if rate_hz <= 0:
        return None
    return max(0.1, 1.0 / rate_hz)


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
            if heater_has_target(base):
                # Heaters with target: subscribe to temperature and target
                objects[base] = ["temperature", "target"]
            elif is_heater_object(base):
                # Temperature sensors: only temperature (no target)
                objects[base] = ["temperature"]
            else:
                objects[base] = None
        else:
            attrs = attribute_map.get(base, set())
            if heater_has_target(base):
                attrs = attrs | {"temperature", "target"}
            elif is_heater_object(base):
                attrs = attrs | {"temperature"}
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
