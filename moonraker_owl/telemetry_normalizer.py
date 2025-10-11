"""Normalization helpers for Moonraker telemetry payloads."""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Optional


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
        self._gcode_temp_pattern = re.compile(
            r"(?P<label>[TB]\d?):\s*(?P<actual>-?\d+(?:\.\d+)?)\s*/\s*(?P<target>-?\d+(?:\.\d+)?)",
            re.IGNORECASE,
        )

    def ingest(self, payload: Dict[str, Any]) -> NormalizedChannels:
        self._apply_payload(payload)

        status_payload = self._build_status_payload()
        progress_payload = self._build_progress_payload()
        telemetry_payload = self._build_telemetry_payload()
        events_payload = self._drain_events()

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
            actual = _safe_round(match.group("actual"))
            target = _safe_round(match.group("target"))

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
    def _build_status_payload(self) -> Optional[Dict[str, Any]]:
        job = _build_job_section(self._status_state, self._file_metadata)
        system = _build_system_section(self._proc_state)
        alerts = _build_alerts_section(self._status_state)

        payload: Dict[str, Any] = {}
        if job:
            payload["job"] = job
        if system:
            payload["system"] = system
        if alerts:
            payload["alerts"] = alerts

        return payload or None

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

    progress = _build_progress_section(status_state)
    if progress:
        job["progress"] = progress

    print_duration = _safe_float(print_stats.get("print_duration"))
    total_duration = _safe_float(print_stats.get("total_duration"))

    if print_duration is not None:
        job.setdefault("progress", {})["elapsed"] = _format_duration(print_duration)

    if (
        total_duration is not None
        and print_duration is not None
        and total_duration >= print_duration
    ):
        remaining_seconds = total_duration - print_duration
        job.setdefault("progress", {})["remaining"] = _format_duration(
            remaining_seconds
        )

    return job or None


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

    progress: Dict[str, Any] = {"percent": round(percent, 2)}
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

    actual_candidate = _safe_round(data.get("temperature"))
    actual = (
        actual_candidate if actual_candidate is not None else previous.get("actual")
    )

    target_candidate = _safe_round(data.get("target"))
    target = (
        target_candidate if target_candidate is not None else previous.get("target")
    )

    if actual is None and target is None:
        return

    entry = {
        "channel": channel,
        "actual": actual,
        "target": target,
    }

    collection.append(entry)
    temperature_state[channel] = {
        "actual": entry.get("actual"),
        "target": entry.get("target"),
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


def _build_system_section(proc_state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    payload: Dict[str, Any] = {}

    moonraker_stats = proc_state.get("moonraker_stats")
    if isinstance(moonraker_stats, dict):
        uptime = moonraker_stats.get("time")
        if uptime is not None:
            try:
                payload["uptimeSeconds"] = int(uptime)
            except (TypeError, ValueError):
                pass

    cpu_usage = proc_state.get("system_cpu_usage")
    if isinstance(cpu_usage, dict):
        value = cpu_usage.get("cpu")
        if value is not None:
            payload["cpu"] = {"usagePercent": _safe_round(value)}

    memory = proc_state.get("system_memory")
    if isinstance(memory, dict):
        used = memory.get("used") or memory.get("usedBytes") or memory.get("used")
        total = memory.get("total") or memory.get("totalBytes") or memory.get("total")
        data: Dict[str, Any] = {}
        if used is not None:
            data["usedBytes"] = _coerce_int(used)
        if total is not None:
            data["totalBytes"] = _coerce_int(total)
        if data:
            payload["memory"] = data

    network = proc_state.get("network")
    if isinstance(network, dict):
        interfaces = []
        for name, metrics in network.items():
            if not isinstance(metrics, dict):
                continue
            interfaces.append(
                {
                    "name": name,
                    "rxBytes": _coerce_int(metrics.get("rx_bytes")),
                    "txBytes": _coerce_int(metrics.get("tx_bytes")),
                }
            )
        if interfaces:
            payload["network"] = {"interfaces": interfaces}

    return payload or None


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
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = copy.deepcopy(value)
