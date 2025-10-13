"""Normalization helpers for Moonraker telemetry payloads."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import (
    Decimal,
    InvalidOperation,
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_HALF_UP,
    ROUND_HALF_DOWN,
)
from typing import Any, Dict, Iterable, List, Optional

from .core import deep_merge

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class NormalizedChannels:
    """Structured payloads ready for publishing to Nexus."""

    status: Optional[Dict[str, Any]] = None
    progress: Optional[Dict[str, Any]] = None
    telemetry: Optional[Dict[str, Any]] = None
    events: List[Dict[str, Any]] = field(default_factory=list)


class TelemetryNormalizer:
    """Maintains Moonraker state and projects it into the Owl contract."""

    def __init__(self) -> None:
        self._status_state: Dict[str, Any] = {}
        self._proc_state: Dict[str, Any] = {}
        self._file_metadata: Dict[str, Any] = {}
        self._pending_events: List[Dict[str, Any]] = []
        self._temperature_state: Dict[str, Dict[str, Optional[float]]] = {}
        self._last_progress_emit: Optional[Dict[str, Optional[int]]] = None
        self._gcode_temp_pattern = re.compile(
            r"(?P<label>[TB]\d?):\s*(?P<actual>-?\d+(?:\.\d+)?)\s*/\s*(?P<target>-?\d+(?:\.\d+)?)",
            re.IGNORECASE,
        )

    def ingest(self, payload: Dict[str, Any]) -> NormalizedChannels:
        self._apply_payload(payload)

        status_payload, timing_state, emitted_timings = self._build_status_payload()
        progress_payload = self._build_progress_payload()
        telemetry_payload = self._build_telemetry_payload()
        events_payload = self._drain_events()

        if emitted_timings and timing_state is not None:
            self._last_progress_emit = timing_state

        return NormalizedChannels(
            status=status_payload,
            progress=progress_payload,
            telemetry=telemetry_payload,
            events=events_payload,
        )

    # ------------------------------------------------------------------
    # payload ingestion
    # ------------------------------------------------------------------
    def _apply_payload(self, payload: Dict[str, Any]) -> None:
        result = payload.get("result")
        if isinstance(result, dict):
            status = result.get("status")
            if isinstance(status, dict):
                _deep_merge(self._status_state, status)

        method = payload.get("method")
        params = payload.get("params")
        if isinstance(params, list):
            for entry in params:
                if not isinstance(entry, dict):
                    continue

                status = entry.get("status")
                if isinstance(status, dict):
                    _deep_merge(self._status_state, status)
                elif method == "notify_status_update" and entry:
                    # Some Moonraker builds emit status fields directly without a wrapper.
                    _deep_merge(self._status_state, entry)

                self._capture_proc_stats(entry)
                self._capture_file_metadata(entry)

        self._capture_events(method, params)

    def _capture_proc_stats(self, entry: Dict[str, Any]) -> None:
        for key in (
            "moonraker_stats",
            "system_cpu_usage",
            "system_memory",
            "network",
            "cpu_temp",
            "websocket_connections",
        ):
            value = entry.get(key)
            if value is None:
                continue

            if isinstance(value, dict):
                target = self._proc_state.setdefault(key, {})
                _deep_merge(target, value)
            else:
                self._proc_state[key] = value

    def _capture_file_metadata(self, entry: Dict[str, Any]) -> None:
        item = entry.get("item")
        if isinstance(item, dict):
            path = item.get("path")
            size = item.get("size")
            if path:
                self._file_metadata[path] = {
                    "size": size,
                    "modified": item.get("modified"),
                }

        job = entry.get("job")
        if isinstance(job, dict):
            filename = job.get("filename")
            metadata = job.get("metadata")
            if isinstance(filename, str) and isinstance(metadata, dict):
                size = metadata.get("size")
                if size is not None:
                    self._file_metadata[filename] = {
                        "size": size,
                        "modified": metadata.get("modified"),
                    }

    def _capture_events(
        self, method: Optional[str], params: Optional[Iterable[Any]]
    ) -> None:
        if method == "notify_gcode_response":
            if isinstance(params, list):
                for entry in params:
                    if isinstance(entry, str):
                        self._update_temperatures_from_gcode(entry)
                        self._pending_events.append(
                            {
                                "type": "gcode_response",
                                "message": entry.strip(),
                                "severity": "info",
                            }
                        )
            return

        if method == "notify_history_changed":
            if isinstance(params, list):
                for entry in params:
                    if not isinstance(entry, dict):
                        continue
                    job = entry.get("job")
                    if not isinstance(job, dict):
                        continue
                    state = job.get("status")
                    filename = job.get("filename")
                    self._pending_events.append(
                        {
                            "type": "history",
                            "status": state,
                            "file": filename,
                            "severity": "info",
                        }
                    )
            return

        if method == "notify_timelapse_event":
            if isinstance(params, list):
                for entry in params:
                    if isinstance(entry, dict):
                        self._pending_events.append(
                            {
                                "type": "timelapse",
                                "payload": entry,
                                "severity": "info",
                            }
                        )
            return

    def _drain_events(self) -> List[Dict[str, Any]]:
        if not self._pending_events:
            return []

        events = [
            {
                "code": event.get("type", "event"),
                "severity": event.get("severity", "info"),
                "message": event.get("message"),
                "data": {
                    key: value
                    for key, value in event.items()
                    if key not in {"type", "severity", "message"}
                },
            }
            for event in self._pending_events
        ]
        self._pending_events.clear()
        return events

    def _update_temperatures_from_gcode(self, line: str) -> None:
        for match in self._gcode_temp_pattern.finditer(line):
            label = match.group("label")
            actual = _round_temperature(match.group("actual"))
            target = _round_temperature(match.group("target"))

            channel = self._map_gcode_channel(label)
            if channel is None:
                continue

            channel_state = self._status_state.setdefault(channel, {})
            if actual is not None:
                channel_state["temperature"] = actual
            if target is not None:
                channel_state["target"] = target

            stored = self._temperature_state.setdefault(channel, {})
            if actual is not None:
                stored["actual"] = actual
            if target is not None:
                stored["target"] = target

    @staticmethod
    def _map_gcode_channel(label: str) -> Optional[str]:
        if not label:
            return None

        label = label.upper()

        if label.startswith("B"):
            return "heater_bed"

        if label.startswith("T"):
            suffix = label[1:]
            if not suffix or suffix == "0":
                return "extruder"
            return f"extruder{suffix}"

        return None

    # ------------------------------------------------------------------
    # builders
    # ------------------------------------------------------------------
    def _build_status_payload(
        self,
    ) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Optional[int]]], bool]:
        job = _build_job_section(self._status_state, self._file_metadata)
        alerts = _build_alerts_section(self._status_state)

        payload: Dict[str, Any] = {}
        if job:
            payload["job"] = job
        if alerts:
            payload["alerts"] = alerts

        timing_state: Optional[Dict[str, Optional[int]]] = None
        emit_timings = False
        if job:
            progress_dict = job.get("progress")
            if isinstance(progress_dict, dict):
                elapsed = progress_dict.get("elapsedSeconds")
                remaining = progress_dict.get("remainingSeconds")
                percent = progress_dict.get("percent")

                has_elapsed = isinstance(elapsed, int)
                has_remaining = isinstance(remaining, int)

                if has_elapsed or has_remaining:
                    timing_state = {
                        "elapsed": elapsed if has_elapsed else None,
                        "remaining": remaining if has_remaining else None,
                        "percent": percent if isinstance(percent, int) else None,
                    }

                emit_timings = self._should_emit_timings(timing_state)
                if not emit_timings:
                    progress_dict.pop("elapsedSeconds", None)
                    progress_dict.pop("remainingSeconds", None)
                    timing_state = self._last_progress_emit

        return payload or None, timing_state, emit_timings

    def _should_emit_timings(self, current: Optional[Dict[str, Optional[int]]]) -> bool:
        if current is None:
            return True

        previous = self._last_progress_emit
        if previous is None:
            return True

        current_percent = current.get("percent")
        previous_percent = previous.get("percent") if previous else None
        if isinstance(current_percent, int) and current_percent != previous_percent:
            return True

        if self._has_significant_delta(
            current.get("elapsed"), previous.get("elapsed") if previous else None
        ):
            return True

        if self._has_significant_delta(
            current.get("remaining"), previous.get("remaining") if previous else None
        ):
            return True

        return False

    @staticmethod
    def _has_significant_delta(
        current: Optional[int], previous: Optional[int], threshold: int = 15
    ) -> bool:
        if not isinstance(current, int) or not isinstance(previous, int):
            return False
        return abs(current - previous) >= threshold

    def _build_progress_payload(self) -> Optional[Dict[str, Any]]:
        progress = _build_progress_section(self._status_state)
        if not progress:
            return None
        return {"job": {"progress": progress}}

    def _build_telemetry_payload(self) -> Optional[Dict[str, Any]]:
        toolhead = _build_toolhead_section(self._status_state)
        temperatures = _build_temperature_section(
            self._status_state, self._temperature_state
        )
        fans = _build_fan_section(self._status_state)

        payload: Dict[str, Any] = {}
        if toolhead:
            payload["toolhead"] = toolhead
        if temperatures:
            payload["temperatures"] = temperatures
        if fans:
            payload["fans"] = fans

        return payload or None


# ----------------------------------------------------------------------
# section builders
# ----------------------------------------------------------------------


def _build_job_section(
    status_state: Dict[str, Any],
    file_metadata: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    print_stats = status_state.get("print_stats")
    if not isinstance(print_stats, dict):
        return None

    job: Dict[str, Any] = {}

    state = print_stats.get("state")
    if isinstance(state, str) and state:
        job["status"] = state

    filename = print_stats.get("filename")
    if isinstance(filename, str) and filename:
        file_info: Dict[str, Any] = {"name": filename}
        meta = file_metadata.get(filename) or {}
        size = meta.get("size")
        if size is not None:
            try:
                file_info["sizeBytes"] = int(size)
            except (TypeError, ValueError):
                pass
        job["file"] = file_info

    job_id = _derive_job_id(filename)
    if job_id:
        job["id"] = job_id

    message = print_stats.get("message")
    if isinstance(message, str) and message:
        job["message"] = message

    progress = _build_progress_section(status_state)
    if progress:
        job["progress"] = progress

    print_duration = _safe_float(print_stats.get("print_duration"))
    total_duration = _safe_float(print_stats.get("total_duration"))

    if print_duration is not None:
        job.setdefault("progress", {})["elapsed"] = _format_duration(print_duration)
        job["progress"]["elapsedSeconds"] = max(int(print_duration), 0)

    if (
        total_duration is not None
        and print_duration is not None
        and total_duration >= print_duration
    ):
        remaining_seconds = total_duration - print_duration
        job.setdefault("progress", {})["remaining"] = _format_duration(
            remaining_seconds
        )
        job["progress"]["remainingSeconds"] = max(int(remaining_seconds), 0)

    return job or None


def _derive_job_id(filename: Optional[str]) -> Optional[str]:
    if not isinstance(filename, str):
        return None

    candidate = filename.strip()
    if not candidate:
        return None

    candidate = candidate.replace("\\", "/")
    name = candidate.split("/")[-1]
    stem = name.rsplit(".", 1)[0] if "." in name else name
    return stem or None


def _build_progress_section(status_state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    display_status = status_state.get("display_status")
    virtual_sdcard = status_state.get("virtual_sdcard")
    print_stats = status_state.get("print_stats")

    percent: Optional[float] = None

    for candidate in (
        _safe_float_from_fraction(display_status, "progress"),
        _safe_float_from_fraction(virtual_sdcard, "progress"),
        _safe_float(print_stats.get("progress"))
        if isinstance(print_stats, dict)
        else None,
    ):
        if candidate is not None:
            percent = max(
                0.0, min(candidate * 100.0 if candidate <= 1.0 else candidate, 100.0)
            )
            break

    if percent is None:
        return None

    progress: Dict[str, Any] = {"percent": int(round(percent))}
    return progress


def _build_toolhead_section(status_state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    toolhead = status_state.get("toolhead")
    if not isinstance(toolhead, dict):
        return None

    position = toolhead.get("position")
    if not isinstance(position, Iterable):
        return None

    try:
        x, y, z = list(position)[:3]
    except (ValueError, TypeError):
        return None

    return {
        "position": {
            "x": _safe_round(x),
            "y": _safe_round(y),
            "z": _safe_round(z),
        }
    }


def _build_temperature_section(
    status_state: Dict[str, Any],
    temperature_state: Dict[str, Dict[str, Optional[float]]],
) -> List[Dict[str, Any]]:
    temperatures: List[Dict[str, Any]] = []

    extruder = status_state.get("extruder")
    if isinstance(extruder, dict):
        _merge_temperature_entry(temperatures, "extruder", extruder, temperature_state)

    heater_bed = status_state.get("heater_bed")
    if isinstance(heater_bed, dict):
        _merge_temperature_entry(
            temperatures, "heater_bed", heater_bed, temperature_state
        )

    for key, value in status_state.items():
        if not isinstance(value, dict):
            continue
        if not key.startswith("temperature_sensor"):
            continue

        _merge_temperature_entry(temperatures, key.strip(), value, temperature_state)

    return [
        temperature
        for temperature in temperatures
        if temperature.get("actual") is not None
        or temperature.get("target") is not None
    ]


def _merge_temperature_entry(
    collection: List[Dict[str, Any]],
    channel: str,
    data: Dict[str, Any],
    temperature_state: Dict[str, Dict[str, Optional[float]]],
) -> None:
    previous = temperature_state.get(channel, {})

    # Extract current values from Moonraker data
    actual_candidate = _round_temperature(data.get("temperature"))
    target_candidate = _round_temperature(data.get("target"))

    # Use new value if present, otherwise fall back to preserved state
    # This handles cases where Moonraker omits target in rapid temperature updates
    actual = (
        actual_candidate if actual_candidate is not None else previous.get("actual")
    )
    target = (
        target_candidate if target_candidate is not None else previous.get("target")
    )

    # Skip sensors with no data at all
    if actual is None and target is None:
        return

    # Log when we're preserving a target value that wasn't in the current update
    if target_candidate is None and target is not None:
        LOGGER.debug(
            "Preserving target for %s: actual=%s target=%s (from previous=%s)",
            channel,
            actual,
            target,
            previous.get("target"),
        )

    entry = {
        "channel": channel,
        "actual": actual,
        "target": target,
    }

    collection.append(entry)

    # Persist both values for future reference
    # This ensures target persists across updates where it may be omitted
    temperature_state[channel] = {
        "actual": actual,
        "target": target,
    }


def _build_fan_section(status_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    fan = status_state.get("fan")
    if not isinstance(fan, dict):
        return []

    speed = fan.get("speed")
    speed_value = _safe_float(speed)
    if speed_value is None:
        return []

    percent = speed_value * 100.0 if speed_value <= 1.0 else speed_value
    return [
        {
            "name": "part",
            "percent": round(max(0.0, min(percent, 100.0)), 1),
        }
    ]


def _build_alerts_section(status_state: Dict[str, Any]) -> List[Dict[str, Any]]:
    display_status = status_state.get("display_status")
    if not isinstance(display_status, dict):
        return []

    message = display_status.get("message")
    if not isinstance(message, str) or not message.strip():
        return []

    return [
        {
            "code": "display_status",
            "severity": "info",
            "message": message.strip(),
        }
    ]


# ----------------------------------------------------------------------
# utilities
# ----------------------------------------------------------------------


def _format_duration(seconds: float) -> str:
    seconds = max(0.0, seconds)
    duration = timedelta(seconds=float(seconds))
    total_seconds = int(duration.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_round(value: Any, digits: int = 2) -> Optional[float]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return round(numeric, digits)


def _round_temperature(value: Any) -> Optional[float]:
    numeric = _safe_float(value)
    if numeric is None:
        return None

    decimal_value = Decimal(str(numeric))

    try:
        quantized = decimal_value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
        return float(quantized)
    except (InvalidOperation, ValueError):
        scaled = int(numeric * 10)
        return scaled / 10.0


def _safe_float_from_fraction(
    container: Optional[Dict[str, Any]], key: str
) -> Optional[float]:
    if not isinstance(container, dict):
        return None
    value = container.get(key)
    return _safe_float(value)


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _deep_merge(target: Dict[str, Any], updates: Dict[str, Any]) -> None:
    """Legacy wrapper for shared deep_merge utility."""
    deep_merge(target, updates)
