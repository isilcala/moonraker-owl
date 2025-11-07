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
from .orchestrator import ChannelPayload, TelemetryOrchestrator
from .state_store import MoonrakerStateStore
from ..telemetry_state import TelemetryHasher

LOGGER = logging.getLogger(__name__)


def is_heater_object(obj_name: str) -> bool:
    """Return True when the Moonraker object represents a heater."""

    return obj_name in ("extruder", "heater_bed") or obj_name.startswith(
        ("extruder", "heater_generic")
    )


class TelemetryConfigurationError(RuntimeError):
    """Raised when required telemetry configuration values are missing."""


@dataclass
class _ChannelPublishState:
    sequence: int = 0
    hash: Optional[str] = None
    last_publish: float = 0.0


@dataclass(frozen=True)
class PollSpec:
    """Declarative definition for a Moonraker polling schedule."""

    name: str
    fields: Sequence[str]
    interval_seconds: float
    initial_delay_seconds: float = 0.0


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


DEFAULT_POLL_SPECS: tuple[PollSpec, ...] = (
    PollSpec(
        name="environment-sensors",
        fields=(
            "temperature_sensor ambient",
            "temperature_sensor chamber",
        ),
        interval_seconds=30.0,
        initial_delay_seconds=5.0,
    ),
    PollSpec(
        name="diagnostics",
        fields=(
            "moonraker_stats",
            "system_cpu_usage",
            "system_memory",
            "network",
            "websocket_connections",
            "cpu_temp",
        ),
        interval_seconds=60.0,
        initial_delay_seconds=10.0,
    ),
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
            self._device_token,
        ) = _resolve_printer_identity(config)

        self._cadence = config.telemetry_cadence

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
        self._channel_state: Dict[str, _ChannelPublishState] = {
            name: _ChannelPublishState() for name in self._channel_topics
        }
        self._hasher = TelemetryHasher()
        self._retain_next_publish: Set[str] = {"overview", "telemetry"}
        self._force_error_retain: Optional[bool] = None
        self._overview_error_snapshot_active = False
        self._status_listeners: list[Callable[[Dict[str, Any]], Any]] = []

        self._min_interval = 0.05
        idle_hz = config.telemetry.rate_hz or 0.033
        self._idle_hz = idle_hz
        self._idle_interval = _hz_to_interval(idle_hz) or 30.0
        self._watch_interval = _hz_to_interval(1.0) or 1.0
        self._telemetry_interval = self._idle_interval
        events_interval = None
        if self._cadence.events_max_per_second > 0:
            events_interval = 1.0 / float(self._cadence.events_max_per_second)
        self._channel_caps = {
            "overview": None,
            "telemetry": self._telemetry_interval,
            "events": events_interval,
        }
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
        self._pending_forced_channels: Set[str] = set()
        self._pending_cadence_forced: Set[str] = set()
        self._force_full_channels_after_reset: Set[str] = set()
        self._active_rate_request: Optional[_RateRequest] = None
        self._last_printer_state: Optional[str] = None
        self._resubscribe_task: Optional[asyncio.Task[None]] = None
        # Cache the last listener state we logged so debug output only fires on transitions.
        self._last_listener_state: Optional[Tuple[Optional[str], Optional[str], Optional[str]]] = None
        self._klippy_ready_applied = False

        self._last_overview_publish_time = 0.0
        self._last_overview_status = "Idle"
        self._last_payload_snapshot: Optional[Dict[str, Any]] = None
        self._overview_idle_interval = self._cadence.overview_idle_interval_seconds
        self._overview_active_interval = (
            self._cadence.overview_active_interval_seconds
        )
        self._telemetry_watchdog_seconds = self._cadence.telemetry_watchdog_seconds
        self._watch_window_expires: Optional[datetime] = None
        self._current_mode = "idle"
        self._bootstrapped = False

        self._orchestrator = TelemetryOrchestrator(
            clock=lambda: datetime.now(timezone.utc),
            cadence=self._cadence,
        )
        self._orchestrator.set_telemetry_mode(
            mode="idle",
            max_hz=1.0 / self._idle_interval if self._idle_interval > 0 else 0.0,
            watch_window_expires=None,
        )

        # Emit an initial cadence log to capture baseline configuration.
        self.apply_telemetry_rate(
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

        await self._prime_initial_state()

        callback_registered = False
        try:
            await self._moonraker.start(self._handle_moonraker_update)
        except ValueError as exc:
            message = str(exc)
            if "callback already registered" not in message.lower():
                raise
            LOGGER.debug(
                "Moonraker callback already registered; reusing existing listener"
            )
            callback_registered = True
        else:
            callback_registered = True

        self._callback_registered = callback_registered

        self._worker = asyncio.create_task(self._run())
        self._start_pollers()

    async def stop(self) -> None:
        self._stop_event.set()
        self._event.set()

        await self._cancel_pollers()

        await self._dispose_worker(remove_callback=True)
        if self._last_payload_snapshot is not None:
            self._retain_next_publish.update({"overview", "telemetry"})
        self._cancel_pending_timer()
        self._pending_forced_channels.clear()
        self._pending_cadence_forced.clear()
        resubscribe_task = self._resubscribe_task
        if resubscribe_task is not None:
            resubscribe_task.cancel()
            self._resubscribe_task = None

    @property
    def topic(self) -> str:
        return self._channel_topics["telemetry"]

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
                LOGGER.debug("Polling '%s' failed: %s", group.name, exc)
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
        pending_forced: Set[str] = set()
        pending_cadence_forced: Set[str] = set()

        while not self._stop_event.is_set():
            if pending is None:
                if self._pending_payload is not None:
                    pending = copy.deepcopy(self._pending_payload)
                    pending_forced = set(self._pending_forced_channels)
                    pending_cadence_forced = set(self._pending_cadence_forced)
                    self._pending_payload = None
                    self._pending_forced_channels.clear()
                    self._pending_cadence_forced.clear()
                    self._cancel_pending_timer()
                else:
                    heartbeat_payload = self._prepare_heartbeat_payload()
                    if heartbeat_payload is not None:
                        pending = heartbeat_payload
                        pending_forced = {"overview"}
                        pending_cadence_forced = set()
                    else:
                        await self._event.wait()
                        self._event.clear()
                        pending = self._gather_payloads()
                        pending_forced = set()
                        pending_cadence_forced = set()
                        if pending is None:
                            continue
            else:
                heartbeat_payload = self._prepare_heartbeat_payload()
                if heartbeat_payload is not None:
                    pending = heartbeat_payload
                    pending_forced = {"overview"}
                    pending_cadence_forced = set()

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
                    pending_forced = set()
                    break

            if pending is None:
                continue

            pending = self._gather_payloads(pending)
            if pending is None:
                pending_forced = set()
                pending_cadence_forced = set()
                continue

            try:
                self._process_payload(
                    pending,
                    forced_channels=pending_forced,
                    cadence_forced=pending_cadence_forced,
                )
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("Telemetry pipeline failed")

            last_sent = loop.time()
            pending = self._gather_payloads()
            pending_forced = set()
            pending_cadence_forced = set()

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

    def _process_payload(
        self,
        payload: Dict[str, Any],
        *,
        forced_channels: Optional[Iterable[str]] = None,
        cadence_forced: Optional[Iterable[str]] = None,
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
            else:
                LOGGER.debug(
                    "Telemetry bootstrap pending; awaiting Moonraker status (available=%s)",
                    list(aggregated_status.keys()),
                )

        if self._bootstrapped and has_status:
            self._last_payload_snapshot = copy.deepcopy(payload)
        elif not self._bootstrapped:
            self._last_payload_snapshot = None

        forced_channels_set = set(forced_channels or ())
        cadence_forced_set = set(cadence_forced or ())

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
                LOGGER.debug("Skipping %s publish until initial Moonraker status arrives", channel)
                skipped_pre_bootstrap = True
                continue

            if channel in self._force_full_channels_after_reset:
                frame.forced = True

            should_publish, delay = self._should_publish_channel(
                channel,
                frame.payload,
                now_monotonic,
                forced=frame.forced,
                force_respects_cadence=channel in cadence_forced_set,
            )
            if not should_publish:
                if delay is not None:
                    deferred = True
                    if min_delay is None or delay < min_delay:
                        min_delay = delay
                    self._pending_forced_channels.add(channel)
                    self._pending_cadence_forced.add(channel)
                continue

            envelope = self._wrap_envelope(
                channel,
                frame,
                include_raw=include_raw,
                raw_json=raw_json,
            )

            if channel == "overview":
                overview_body = envelope.get("overview")
                if isinstance(overview_body, dict):
                    status_value = overview_body.get("printerStatus")
                    if isinstance(status_value, str):
                        normalized_status = status_value.strip()
                        if normalized_status:
                            previous_status_value = self._last_overview_status
                            self._last_overview_status = status_value
                            self._last_overview_publish_time = now_monotonic
                            status_lower = normalized_status.lower()
                            if status_lower == "error":
                                if not self._overview_error_snapshot_active:
                                    self._clear_retained_channels(("overview", "telemetry"))
                                    self._overview_error_snapshot_active = True
                                retain_error = self._force_error_retain
                                if retain_error is None:
                                    previous_lower = (
                                        (previous_status_value or "").strip().lower()
                                    )
                                    retain_error = previous_lower in {"printing", "paused"}
                                if retain_error:
                                    self._retain_next_publish.add("overview")
                                else:
                                    self._retain_next_publish.discard("overview")
                            elif self._overview_error_snapshot_active:
                                self._retain_next_publish.add("overview")
                                self._overview_error_snapshot_active = False

            retain = channel in self._retain_next_publish
            if retain:
                self._retain_next_publish.discard(channel)
            self._publish(channel, topic, envelope, retain=retain)
            self._pending_forced_channels.discard(channel)
            self._pending_cadence_forced.discard(channel)
            self._force_full_channels_after_reset.discard(channel)

        if deferred:
            self._store_pending_payload(payload)
            if min_delay is not None:
                self._schedule_cadence_check(min_delay)

        if not self._bootstrapped and skipped_pre_bootstrap:
            self._orchestrator.overview_selector.reset()
            self._orchestrator.telemetry_selector.reset()

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

    def _maybe_expire_watch_window(self, *, now: Optional[datetime] = None) -> None:
        if self._current_mode == "idle":
            return

        expires_at = self._watch_window_expires
        if expires_at is None:
            return

        current_time = now or datetime.now(timezone.utc)
        if current_time < expires_at:
            return

        LOGGER.info(
            "Telemetry watch window expired at %s; reverting to idle cadence",
            expires_at.isoformat(),
        )
        self.apply_telemetry_rate(
            mode="idle",
            max_hz=self._idle_hz,
            duration_seconds=None,
            requested_at=current_time,
        )

    def apply_telemetry_rate(
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
        previous_interval = self._telemetry_interval

        self._telemetry_interval = interval
        self._channel_caps["telemetry"] = interval
        self._watch_window_expires = expires_at
        if interval > 0:
            self._telemetry_watchdog_seconds = max(300.0, interval * 5)
        self._orchestrator.set_telemetry_mode(
            mode=mode,
            max_hz=effective_hz,
            watch_window_expires=expires_at,
        )
        self._current_mode = mode

        if LOGGER.isEnabledFor(logging.INFO):
            LOGGER.info(
                "Telemetry cadence change: mode=%s (prev=%s) max_hz=%.3f interval=%.2fs (prev=%.2fs) expires=%s",
                mode,
                previous_mode,
                max_hz,
                interval,
                previous_interval,
                expires_at.isoformat() if expires_at else "<none>",
            )

        for channel in ("telemetry", "overview"):
            state = self._channel_state.get(channel)
            if state:
                state.hash = None
                state.last_publish = 0.0

        self._event.set()
        self._active_rate_request = _RateRequest(
            mode=mode,
            max_hz=max_hz,
            requested_at=requested_at,
            duration_seconds=normalized_duration,
            expires_at=expires_at,
        )
        return expires_at

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

    def _should_publish_channel(
        self,
        channel: str,
        payload: Dict[str, Any],
        now_monotonic: float,
        *,
        forced: bool,
        force_respects_cadence: bool,
    ) -> tuple[bool, Optional[float]]:
        state = self._channel_state[channel]

        effective_forced = forced

        if channel == "telemetry":
            elapsed_since_publish = now_monotonic - state.last_publish
            if state.last_publish == 0.0 or elapsed_since_publish >= self._telemetry_watchdog_seconds:
                effective_forced = True

        if (
            channel == "telemetry"
            and forced
            and force_respects_cadence
            and state.last_publish != 0.0
        ):
            force_cap_interval = self._watch_interval or 1.0
            if force_cap_interval <= 0:
                force_cap_interval = 1.0
            elapsed = now_monotonic - state.last_publish
            if elapsed < force_cap_interval:
                return False, force_cap_interval - elapsed

        cadence_interval = self._channel_caps.get(channel)
        if cadence_interval:
            elapsed = now_monotonic - state.last_publish
            if (
                channel == "telemetry"
                and forced
                and force_respects_cadence
                and state.last_publish != 0.0
                and elapsed < cadence_interval
            ):
                return False, cadence_interval - elapsed
            if not effective_forced and elapsed < cadence_interval:
                return False, cadence_interval - elapsed

        payload_hash = self._hasher.hash_payload(payload)
        if not effective_forced and payload_hash == state.hash:
            return False, None

        state.hash = payload_hash
        state.last_publish = now_monotonic
        return True, None

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
        properties.UserProperty = [
            (constants.DEVICE_TOKEN_MQTT_PROPERTY_NAME, self._device_token)
        ]

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

    def _clear_retained_channels(self, channels: Iterable[str]) -> None:
        if not channels:
            return

        properties = Properties(PacketTypes.PUBLISH)
        properties.UserProperty = [
            (constants.DEVICE_TOKEN_MQTT_PROPERTY_NAME, self._device_token)
        ]

        for channel in channels:
            topic = self._channel_topics.get(channel)
            if not topic:
                continue

            try:
                self._mqtt.publish(
                    topic,
                    b"",
                    qos=self._channel_qos.get(channel, 1),
                    retain=True,
                    properties=properties,
                )
            except MQTTConnectionError as exc:
                LOGGER.warning("Retained payload clear failed for %s: %s", channel, exc)
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception(
                    "Unexpected error clearing retained payload for channel %s",
                    channel,
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
        self._orchestrator.set_telemetry_mode(
            mode="idle",
            max_hz=1.0 / self._idle_interval if self._idle_interval > 0 else 0.0,
            watch_window_expires=None,
        )
        self._current_mode = "idle"
        self._telemetry_interval = self._idle_interval
        self._channel_caps["telemetry"] = self._telemetry_interval
        self._telemetry_watchdog_seconds = self._cadence.telemetry_watchdog_seconds
        self._watch_window_expires = None
        self._bootstrapped = False
        for state in self._channel_state.values():
            state.hash = None
            state.last_publish = 0.0
        self._pending_payload = None
        self._pending_forced_channels.clear()
        self._pending_cadence_forced.clear()
        self._last_payload_snapshot = None
        self._last_overview_publish_time = 0.0
        self._last_overview_status = "Idle"
        self._overview_error_snapshot_active = False
        self._last_printer_state = None
        self._retain_next_publish.update({"overview", "telemetry"})
        if self._queue is not None:
            while True:
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        self._reapply_rate_request()
        self._force_full_channels_after_reset.clear()
        self._force_full_channels_after_reset.update({"overview", "telemetry"})

        if previous_snapshot is not None:
            self._pending_payload = copy.deepcopy(previous_snapshot)
            self._pending_forced_channels.update({"overview", "telemetry"})
            self._pending_cadence_forced.difference_update({"overview", "telemetry"})
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
            "Reapplying telemetry cadence request after restart: mode=%s max_hz=%.3f remaining=%s",
            request.mode,
            request.max_hz,
            f"{remaining_seconds}s" if remaining_seconds is not None else "indefinite",
        )

        self.apply_telemetry_rate(
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
        previous_status = (self._last_overview_status or "").strip().lower()
        session = self._orchestrator.session_tracker.compute(self._orchestrator.store)
        retain_error = previous_status in {"printing", "paused"}
        if not retain_error:
            retain_error = session.has_active_job or (
                session.progress_percent is not None and session.progress_percent > 0
            )

        self._force_error_retain = retain_error
        if not retain_error:
            self._retain_next_publish.discard("overview")

        snapshot = self._orchestrator.store.export_state()
        try:
            self._process_payload(
                status_payload,
                forced_channels={"overview"},
                cadence_forced=(),
            )
        finally:
            self._orchestrator.store.restore_state(snapshot)
            self._force_error_retain = None

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
                LOGGER.debug("Status listener raised", exc_info=True)
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
                LOGGER.debug("Skipping Moonraker resubscribe; ready already handled")
                return
            # On a Klippy reboot the websocket keeps running but loses subscriptions; refresh them lazily.
            LOGGER.info("Detected Klippy ready; refreshing Moonraker subscriptions")
            self._klippy_ready_applied = True
            self._schedule_resubscribe("klippy-ready")
        elif ready_signal:
            self._klippy_ready_applied = False

    def _schedule_resubscribe(self, reason: str) -> None:
        if not hasattr(self._moonraker, "resubscribe"):
            LOGGER.debug(
                "Printer adapter does not support resubscribe; skipping (%s)",
                reason,
            )
            return

        loop = self._loop
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                LOGGER.debug(
                    "Unable to schedule Moonraker resubscribe (%s); no active loop",
                    reason,
                )
                return

        existing_task = self._resubscribe_task
        if existing_task is not None and not existing_task.done():
            LOGGER.debug(
                "Moonraker resubscribe already pending; skipping (%s)", reason
            )
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
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.debug(
                    "Failed to refresh Moonraker snapshot after %s: %s",
                    reason,
                    exc,
                )
            finally:
                self._resubscribe_task = None

        # Resubscribe asynchronously so we do not block the main callback pipeline.
        LOGGER.debug("Scheduling Moonraker resubscribe (%s)", reason)
        try:
            self._resubscribe_task = loop.create_task(_runner())
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.debug(
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
            LOGGER.debug(
                "Status listener produced awaitable but no loop is available"
            )
            return

        async def _runner() -> None:
            try:
                await asyncio.shield(awaitable)
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.debug("Status listener awaitable raised", exc_info=True)

        try:
            loop.create_task(_runner())
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.debug("Failed to schedule status listener", exc_info=True)


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
