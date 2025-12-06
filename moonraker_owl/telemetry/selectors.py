"""Selectors that compose channel payloads for the telemetry pipeline."""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .state_engine import PrinterContext, resolve_printer_state
from .state_store import MoonrakerStateStore, SectionSnapshot
from .trackers import HeaterMonitor, SessionInfo


LOGGER = logging.getLogger(__name__)


class StatusSelector:
    def __init__(self, *, heartbeat_seconds: int = 60) -> None:
        self._heartbeat_seconds = heartbeat_seconds
        self._last_contract_hash: Optional[str] = None
        self._last_updated: Optional[datetime] = None
        self._last_emitted_at: Optional[datetime] = None

    def reset(self) -> None:
        self._last_contract_hash = None
        self._last_updated = None
        self._last_emitted_at = None

    def build(
        self,
        store: MoonrakerStateStore,
        session: SessionInfo,
        heater_monitor: HeaterMonitor,
        observed_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        print_stats_snapshot = store.get("print_stats")
        display_status_snapshot = store.get("display_status")
        printer_snapshot = store.get("printer")
        state = session.raw_state
        if state is None and print_stats_snapshot is not None:
            state = print_stats_snapshot.data.get("state")

        # Check if klippy is in shutdown state
        is_shutdown = False
        shutdown_message: Optional[str] = None
        if printer_snapshot is not None:
            is_shutdown = printer_snapshot.data.get("is_shutdown", False)
            if is_shutdown:
                shutdown_message = printer_snapshot.data.get("state_message")

        context = PrinterContext(
            observed_at=observed_at,
            idle_state=session.idle_timeout_state,
            timelapse_paused=session.timelapse_paused,
            # has_active_job is only used for UI display, not state determination
            has_active_job=session.has_active_job,
        )

        phase = resolve_printer_state(state, context)

        # Override phase to Error when klippy is shutdown.
        # This ensures UI displays error state regardless of what print_stats.state
        # says, since Moonraker may send stale print_stats updates after shutdown.
        if is_shutdown:
            phase = "Error"

        # Override phase when job has terminated but print_stats.state lags behind.
        # This handles the timing gap between notify_history_changed (which updates
        # job_status) and print_stats.state updates from Moonraker.
        elif not session.has_active_job and phase == "Printing":
            state_lower = state.lower() if isinstance(state, str) else ""
            job_status_lower = (
                session.job_status.lower()
                if isinstance(session.job_status, str)
                else ""
            )

            # Determine terminal phase from job_status or raw_state
            completed_states = {"complete", "completed"}
            cancelled_states = {"cancelled", "canceled"}
            error_states = {"error"}

            if state_lower in completed_states or job_status_lower in completed_states:
                phase = "Completed"
            elif state_lower in cancelled_states or job_status_lower in cancelled_states:
                phase = "Cancelled"
            elif state_lower in error_states or job_status_lower in error_states:
                phase = "Error"

        lifecycle: Dict[str, Any] = {
            "phase": phase,
            "statusLabel": phase,
            "isHeating": heater_monitor.is_actively_heating(),
            "hasActiveJob": session.has_active_job,
        }

        # Priority: shutdown message > session message
        if is_shutdown and shutdown_message:
            lifecycle["reason"] = shutdown_message
        elif session.message:
            lifecycle["reason"] = session.message

        status: Dict[str, Any] = {
            "printerStatus": phase,
            "lifecycle": lifecycle,
            "flags": {
                "watchWindowActive": False,
                "hasActiveJob": session.has_active_job,
                "isShutdown": is_shutdown,
            },
        }

        # subStatus priority: shutdown message > session message > display message
        if is_shutdown and shutdown_message:
            status["subStatus"] = shutdown_message
        elif session.message:
            status["subStatus"] = session.message
        elif display_status_snapshot is not None:
            candidate = display_status_snapshot.data.get("message")
            if isinstance(candidate, str):
                cleaned = candidate.strip()
                if cleaned and _should_use_display_message(cleaned, session):
                    status["subStatus"] = cleaned

        if session.progress_percent is not None:
            quantized_progress = max(
                0,
                min(100, int(math.floor(session.progress_percent))),
            )
            status["progressPercent"] = quantized_progress
        if session.elapsed_seconds is not None:
            status["elapsedSeconds"] = session.elapsed_seconds
        estimated_remaining = session.remaining_seconds
        if estimated_remaining is None and print_stats_snapshot is not None:
            total_duration = _to_float(print_stats_snapshot.data.get("total_duration"))
            elapsed_duration = _to_float(
                print_stats_snapshot.data.get("print_duration")
            )
            if (
                total_duration is not None
                and elapsed_duration is not None
                and total_duration >= elapsed_duration
            ):
                estimated_remaining = max(int(total_duration - elapsed_duration), 0)

        if estimated_remaining is not None:
            status["estimatedTimeRemainingSeconds"] = estimated_remaining

        job_payload = _build_job_payload(session)
        if job_payload:
            status["job"] = job_payload
            if estimated_remaining is not None:
                progress_section = job_payload.setdefault("progress", {})
                progress_section["estimatedTimeRemainingSeconds"] = estimated_remaining

        status["cadence"] = {
            "heartbeatSeconds": self._heartbeat_seconds,
            "watchWindowActive": False,
        }

        contract_hash = self._build_contract_hash(status)
        heartbeat_due = self._last_emitted_at is None or (
            observed_at - self._last_emitted_at
        ) >= timedelta(seconds=self._heartbeat_seconds)
        has_meaningful_change = contract_hash != self._last_contract_hash

        if not has_meaningful_change and not heartbeat_due:
            return None

        if has_meaningful_change:
            self._last_contract_hash = contract_hash
            self._last_updated = observed_at

        last_updated = self._last_updated or observed_at

        status["lastUpdatedUtc"] = last_updated.replace(microsecond=0).isoformat()
        self._last_emitted_at = observed_at

        return status

    def _build_contract_hash(self, payload: Dict[str, Any]) -> str:
        time_keys = {
            "elapsedSeconds",
            "estimatedTimeRemainingSeconds",
            "lastUpdatedUtc",
        }

        def sanitize(value: Any) -> Any:
            if isinstance(value, dict):
                result: Dict[str, Any] = {}
                for key, inner in value.items():
                    if key in time_keys:
                        continue
                    result[key] = sanitize(inner)
                return result
            if isinstance(value, list):
                return [sanitize(item) for item in value]
            return value

        sanitized = sanitize(payload)
        return json.dumps(sanitized, sort_keys=True, default=str)


@dataclass
class _SensorState:
    value: Optional[float]
    target: Optional[float]
    status: str
    last_updated: datetime


class SensorsSelector:
    def __init__(self) -> None:
        self._sensor_state: Dict[str, _SensorState] = {}
        self._last_contract_hash: Optional[str] = None

    def reset(self) -> None:
        self._sensor_state.clear()
        self._last_contract_hash = None

    def build(
        self,
        store: MoonrakerStateStore,
        *,
        mode: str,
        max_hz: float,
        watch_window_expires: Optional[datetime],
        observed_at: datetime,
        force_emit: bool = False,
    ) -> Optional[Dict[str, Any]]:
        sensors = self._collect_sensors(store)
        cadence: Dict[str, Any] = {
            "mode": mode,
            "maxHz": max_hz,
        }
        if watch_window_expires is not None:
            cadence["watchWindowExpiresUtc"] = watch_window_expires.replace(
                microsecond=0
            ).isoformat()

        payload = {
            "cadence": cadence,
            "sensors": sensors,
        }

        # Only hash the sensors data for deduplication.
        # Cadence metadata (mode, maxHz, watchWindowExpiresUtc) changes frequently
        # and should not trigger re-emission if sensor values haven't changed.
        contract_hash = json.dumps(sensors, sort_keys=True, default=str)
        if not force_emit and contract_hash == self._last_contract_hash:
            return None

        self._last_contract_hash = contract_hash
        return payload

    def _collect_sensors(self, store: MoonrakerStateStore) -> List[Dict[str, Any]]:
        sensors: List[Dict[str, Any]] = []
        seen_channels: set[str] = set()

        for section in store.iter_sections():
            channel = _get_channel_name(section.name)
            if channel is None:
                continue

            payload, state = self._build_sensor_payload(channel, section)
            sensors.append(payload)
            self._sensor_state[channel] = state
            seen_channels.add(channel)

        for channel in list(self._sensor_state.keys()):
            if channel not in seen_channels:
                self._sensor_state.pop(channel)

        sensors.sort(key=lambda entry: entry["channel"])
        return sensors

    def _build_sensor_payload(
        self,
        channel: str,
        section: SectionSnapshot,
    ) -> tuple[Dict[str, Any], _SensorState]:
        sensor_type = _infer_sensor_type(section.name)
        unit = _infer_unit(sensor_type)
        is_fan = _is_fan_type(sensor_type)

        # For fans, use 'speed' field; for temperature sensors, use 'temperature'
        if is_fan:
            raw_value = section.data.get("speed")
            # Fan speed is 0.0-1.0, convert to percent (0-100)
            value = _round_fan_speed(raw_value)
            target = None  # Regular fans don't have target
        else:
            raw_value = section.data.get("temperature")
            if raw_value is None:
                raw_value = section.data.get("value")
            value = _round_sensor_value(sensor_type, raw_value)
            target = _round_sensor_value(sensor_type, section.data.get("target"))

        status = _normalise_sensor_state(section.data.get("state"))

        previous = self._sensor_state.get(channel)
        signature = (value, target, status)
        if previous and (previous.value, previous.target, previous.status) == signature:
            last_updated = previous.last_updated
        else:
            last_updated = section.observed_at
            previous = _SensorState(
                value=value,
                target=target,
                status=status,
                last_updated=last_updated,
            )

        payload = {
            "channel": channel,
            "type": sensor_type,
            "unit": unit,
            "value": value,
            "sourceObject": section.name,
            **({"target": target} if target is not None else {}),
            "status": status,
            "lastUpdatedUtc": last_updated.replace(microsecond=0).isoformat(),
        }

        return payload, previous


class EventsSelector:
    """Selector for the events channel payload (command ack events).

    This selector builds the events channel payload from command state events
    (drained from EventCollector._pending). These are system events for UI
    feedback, not user-facing business events.

    Note: The cadence values (max_per_second, max_per_minute) are metadata
    included in the payload for consumer information only.

    Rate limiting strategy for events channel:
    - Command ack events: No rate limiting (frequency limited by user interaction)
    - P0/P1 business events: Token bucket in EventCollector.harvest()
    - Events channel bypasses cadence controller entirely

    Both event types are merged in TelemetryOrchestrator.build_payloads()
    and share the same MQTT topic, distinguished by eventName field.
    """

    def __init__(self, *, max_per_second: int = 1, max_per_minute: int = 20) -> None:
        self._max_per_second = max(0, max_per_second)
        self._max_per_minute = max(0, max_per_minute)

    def build(
        self,
        *,
        events: List[Dict[str, Any]],
        observed_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        if not events:
            return None

        return {
            "cadence": {
                "maxPerSecond": self._max_per_second,
                "maxPerMinute": self._max_per_minute,
            },
            "items": events,
        }


def _build_job_payload(session: SessionInfo) -> Optional[Dict[str, Any]]:
    """Build job payload for status channel.

    Returns None when session_id is None (no active job).
    This ensures UI clears job info when print ends.
    """
    if not session.session_id:
        return None

    payload: Dict[str, Any] = {
        "sessionId": session.session_id,
        "jobId": session.session_id,  # Use session_id as jobId
    }

    if session.job_name:
        payload["name"] = session.job_name
        payload["sourcePath"] = session.job_name

    if session.progress_percent is not None:
        payload["progressPercent"] = max(
            0,
            min(100, int(math.floor(session.progress_percent))),
        )

    layers: Dict[str, int] = {}
    if session.layer_current is not None:
        layers["current"] = session.layer_current
    if session.layer_total is not None:
        layers["total"] = session.layer_total
    if layers:
        payload["layers"] = layers

    payload["progress"] = {
        "elapsedSeconds": session.elapsed_seconds or 0,
        "estimatedTimeRemainingSeconds": session.remaining_seconds or 0,
    }

    if session.message:
        payload["message"] = session.message

    # Include thumbnail URL if set by server via sync:job-thumbnail command
    if session.thumbnail_url:
        payload["thumbnailUrl"] = session.thumbnail_url

    return payload


def _should_use_display_message(message: str, session: SessionInfo) -> bool:
    normalized = message.lower()
    redundant = {"printing", "resuming", "pausing", "paused", "idle", "ready", "busy"}
    terminal_states = {
        "cancelled",
        "canceled",
        "completed",
        "complete",
        "error",
        "failed",
        "aborted",
    }

    if normalized.startswith("klippy ready"):
        return False

    if not session.has_active_job and normalized in redundant:
        return False

    if session.raw_state in terminal_states and normalized in redundant:
        return False

    if session.job_status in terminal_states and normalized in redundant:
        return False

    if session.idle_timeout_state in {"idle", "ready"} and normalized in redundant:
        return False

    return True


def _get_channel_name(name: str) -> Optional[str]:
    """Get the channel name from a Moonraker object name.
    
    Returns the original Moonraker object name as the channel identifier,
    or None if this object type should be excluded from sensors.
    
    The UI layer is responsible for formatting these names for display.
    """
    # Exclude non-sensor objects
    if name in {"temperatures", "toolhead"}:
        return None
    
    # Include all recognized sensor types
    if name.startswith("extruder"):
        return name
    if name == "heater_bed":
        return name
    if name.startswith("heater_generic "):
        return name
    if name.startswith("temperature_sensor "):
        return name
    if name.startswith("temperature_fan "):
        return name
    if name == "fan":
        return name
    if name.startswith("fan_generic "):
        return name
    if name.startswith("heater_fan "):
        return name
    if name.startswith("controller_fan "):
        return name
    
    return None


def _infer_sensor_type(name: str) -> str:
    """Infer sensor type from Moonraker object name.
    
    Types:
    - 'heater': extruder, heater_bed, heater_generic (can set target)
    - 'temperatureFan': temperature_fan (can set target temperature)
    - 'sensor': temperature_sensor (read-only temperature)
    - 'fan': regular fan (speed only, no temperature)
    """
    if name.startswith("extruder") or name == "heater_bed":
        return "heater"
    if name.startswith("heater_generic"):
        return "heater"
    if name.startswith("temperature_fan"):
        return "temperatureFan"
    if name.startswith("temperature_sensor"):
        return "sensor"
    if name == "fan":
        return "fan"
    return "sensor"


def _is_fan_type(sensor_type: str) -> bool:
    """Check if sensor type is a fan (speed-based, not temperature-based)."""
    return sensor_type == "fan"


def _infer_unit(sensor_type: str) -> str:
    """Determine the unit based on sensor type."""
    if sensor_type == "fan":
        return "percent"
    return "celsius"


def _normalise_sensor_state(status: Any) -> str:
    if isinstance(status, str) and status:
        return status
    return "ok"


def _round_sensor_value(sensor_type: str, value: Any) -> Optional[float]:
    numeric = _to_float(value)
    if numeric is None:
        return None
    # All temperature sensors floor to whole numbers (204.9 -> 204)
    return float(math.floor(numeric))


def _round_fan_speed(value: Any) -> Optional[float]:
    """Round fan speed to integer percentage (0-100)."""
    numeric = _to_float(value)
    if numeric is None:
        return None
    # Convert 0.0-1.0 to 0-100 and round to nearest integer (0.505 -> 51%)
    return float(round(numeric * 100))


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# =============================================================================
# ObjectsSelector - Exclude Object Channel (ADR-0016)
# =============================================================================


@dataclass
class ObjectDefinition:
    """A single object definition from Klipper's exclude_object module."""

    name: str
    center: Optional[List[float]] = None
    polygon: Optional[List[List[float]]] = None


class ObjectsSelector:
    """Selector for the objects channel payload (ADR-0016: Exclude Object).

    Builds the objects channel payload from Moonraker's exclude_object state.
    This channel provides object definitions, exclusion status, and current
    object for the exclude object UI feature.

    Only produces output when:
    1. exclude_object is configured in Klipper
    2. Objects are defined (during active print with labeled objects)

    The channel uses kind="full" for updates and kind="reset" on print end.
    """

    def __init__(self) -> None:
        self._previous_payload: Optional[Dict[str, Any]] = None
        self._has_published: bool = False

    def reset(self) -> None:
        """Reset selector state (e.g., on print end or reconnection)."""
        self._previous_payload = None
        self._has_published = False

    def build(
        self,
        store: MoonrakerStateStore,
        *,
        observed_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Build objects channel payload from exclude_object state.

        Args:
            store: Moonraker state store with current printer state.
            observed_at: Observation timestamp.

        Returns:
            Objects payload dict or None if no objects defined or no change.
        """
        exclude_obj_snapshot = store.get("exclude_object")

        # No exclude_object data - feature not configured or no objects
        if exclude_obj_snapshot is None:
            return None

        data = exclude_obj_snapshot.data
        objects = data.get("objects", [])

        # No objects defined - don't publish
        if not objects:
            return None

        # Build definitions list
        definitions: List[Dict[str, Any]] = []
        for obj in objects:
            if not isinstance(obj, dict):
                continue
            name = obj.get("name")
            if not name:
                continue

            definition: Dict[str, Any] = {"name": name}
            center = obj.get("center")
            if center and isinstance(center, (list, tuple)) and len(center) >= 2:
                definition["center"] = [float(center[0]), float(center[1])]
            polygon = obj.get("polygon")
            if polygon and isinstance(polygon, list):
                definition["polygon"] = [
                    [float(p[0]), float(p[1])]
                    for p in polygon
                    if isinstance(p, (list, tuple)) and len(p) >= 2
                ]
            definitions.append(definition)

        # Get excluded objects and current object
        excluded = data.get("excluded_objects", [])
        if not isinstance(excluded, list):
            excluded = []
        current = data.get("current_object")
        if current is not None and not isinstance(current, str):
            current = str(current) if current else None

        payload = {
            "definitions": definitions,
            "excluded": excluded,
            "current": current,
        }

        # Check if we should publish (change detection)
        if not self.should_publish(self._previous_payload, payload):
            return None

        # Update previous payload for next comparison
        self._previous_payload = payload
        self._has_published = True

        return payload

    def should_publish(
        self,
        previous: Optional[Dict[str, Any]],
        current: Optional[Dict[str, Any]],
    ) -> bool:
        """Determine if objects channel should be published.

        Publishes when:
        - Objects appear for the first time (print start with labeled objects)
        - excluded list changes (user excluded an object)
        - current object changes (printing moved to different object)

        Args:
            previous: Previous payload (None if never published).
            current: Current payload (None if no objects).

        Returns:
            True if channel should be published.
        """
        # First appearance of objects
        if previous is None and current is not None:
            return True

        # Objects cleared (will be handled by reset, but detect anyway)
        if previous is not None and current is None:
            return True

        # No objects on either side
        if previous is None and current is None:
            return False

        # Check for meaningful changes
        # Note: definitions rarely change, but excluded/current change during print
        return (
            previous.get("excluded") != current.get("excluded")
            or previous.get("current") != current.get("current")
            or previous.get("definitions") != current.get("definitions")
        )

    def needs_reset(self, had_objects: bool, has_objects: bool) -> bool:
        """Determine if a reset should be published.

        Args:
            had_objects: Whether objects were previously published.
            has_objects: Whether objects are currently available.

        Returns:
            True if reset should be published.
        """
        return had_objects and not has_objects
