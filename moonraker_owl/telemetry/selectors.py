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
        self._last_debug_signature: Optional[tuple[Any, ...]] = None

    def reset(self) -> None:
        self._last_contract_hash = None
        self._last_updated = None
        self._last_emitted_at = None
        self._last_debug_signature = None

    def build(
        self,
        store: MoonrakerStateStore,
        session: SessionInfo,
        heater_monitor: HeaterMonitor,
        observed_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        print_stats_snapshot = store.get("print_stats")
        display_status_snapshot = store.get("display_status")
        state = session.raw_state
        if state is None and print_stats_snapshot is not None:
            state = print_stats_snapshot.data.get("state")

        context = PrinterContext(
            observed_at=observed_at,
            idle_state=session.idle_timeout_state,
            timelapse_paused=session.timelapse_paused,
            # has_active_job is only used for UI display, not state determination
            has_active_job=session.has_active_job,
        )

        phase = resolve_printer_state(state, context)

        # Override phase when job has terminated but print_stats.state lags behind.
        # This handles the timing gap between notify_history_changed (which updates
        # job_status) and print_stats.state updates from Moonraker.
        if not session.has_active_job and phase == "Printing":
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
                LOGGER.debug(
                    "Overriding phase to Completed (raw_state=%s, job_status=%s)",
                    state,
                    session.job_status,
                )
                phase = "Completed"
            elif state_lower in cancelled_states or job_status_lower in cancelled_states:
                LOGGER.debug(
                    "Overriding phase to Cancelled (raw_state=%s, job_status=%s)",
                    state,
                    session.job_status,
                )
                phase = "Cancelled"
            elif state_lower in error_states or job_status_lower in error_states:
                LOGGER.debug(
                    "Overriding phase to Error (raw_state=%s, job_status=%s)",
                    state,
                    session.job_status,
                )
                phase = "Error"

        lifecycle: Dict[str, Any] = {
            "phase": phase,
            "statusLabel": phase,
            "isHeating": heater_monitor.is_actively_heating(),
            "hasActiveJob": session.has_active_job,
        }

        if session.message:
            lifecycle["reason"] = session.message

        status: Dict[str, Any] = {
            "printerStatus": phase,
            "lifecycle": lifecycle,
            "flags": {
                "watchWindowActive": False,
                "hasActiveJob": session.has_active_job,
            },
        }

        if session.message:
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

        if LOGGER.isEnabledFor(logging.DEBUG):
            signature = (
                phase,
                lifecycle.get("reason"),
                session.session_id,
                session.has_active_job,
                session.progress_percent,
                session.elapsed_seconds,
                session.remaining_seconds,
                session.layer_current,
                session.layer_total,
                heater_monitor.is_actively_heating(),
                state,
                session.idle_timeout_state,
                session.timelapse_paused,
                session.job_status,
            )
            if signature != self._last_debug_signature:
                LOGGER.debug(
                    "Overview lifecycle resolved: raw_state=%s phase=%s reason=%s session=%s has_active_job=%s is_heating=%s progress=%s%% elapsed=%s remaining=%s layers=%s/%s idle_timeout_state=%s timelapse_paused=%s job_status=%s",
                    state,
                    phase,
                    lifecycle.get("reason"),
                    session.session_id,
                    session.has_active_job,
                    heater_monitor.is_actively_heating(),
                    session.progress_percent,
                    session.elapsed_seconds,
                    session.remaining_seconds,
                    session.layer_current,
                    session.layer_total,
                    session.idle_timeout_state,
                    session.timelapse_paused,
                    session.job_status,
                )
                self._last_debug_signature = signature

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

        contract_hash = json.dumps(payload, sort_keys=True, default=str)
        if not force_emit and contract_hash == self._last_contract_hash:
            return None

        self._last_contract_hash = contract_hash
        return payload

    def _collect_sensors(self, store: MoonrakerStateStore) -> List[Dict[str, Any]]:
        sensors: List[Dict[str, Any]] = []
        seen_channels: set[str] = set()

        for section in store.iter_sections():
            channel = _normalise_channel_name(section.name)
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
        unit = _infer_unit(section.name)

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
            **({"target": target} if target is not None else {}),
            "status": status,
            "lastUpdatedUtc": last_updated.replace(microsecond=0).isoformat(),
        }

        return payload, previous


class EventsSelector:
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


def _normalise_channel_name(name: str) -> Optional[str]:
    if name.startswith("extruder"):
        suffix = name[len("extruder") :]
        return "extruder" + suffix.capitalize() if suffix else "extruder"
    if name == "heater_bed":
        return "heaterBed"
    if name.startswith("heater_generic"):
        return name.replace("heater_generic ", "heaterGeneric-")
    if name in {"temperatures", "toolhead"}:
        return None
    if name in {"temperature_fan", "temperature_fans", "fan", "part_fan"}:
        return "partCoolingFan"
    return None


def _infer_sensor_type(name: str) -> str:
    if name.startswith("extruder") or name.startswith("heater"):
        return "heater"
    return "fan"


def _infer_unit(name: str) -> str:
    if name.startswith("extruder") or name.startswith("heater"):
        return "celsius"
    return "percent"


def _normalise_sensor_state(status: Any) -> str:
    if isinstance(status, str) and status:
        return status
    return "ok"


def _round_sensor_value(sensor_type: str, value: Any) -> Optional[float]:
    numeric = _to_float(value)
    if numeric is None:
        return None
    if sensor_type == "heater":
        return float(round(numeric))
    return round(numeric, 1)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
