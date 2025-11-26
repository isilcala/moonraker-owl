"""Normalization helpers for Moonraker telemetry payloads."""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from typing import Any, Dict, Iterable, List, Optional

from .core.utils import deep_merge
from .printer_state import PrinterContext, PrinterStateResolver


@dataclass(slots=True)
class NormalizedPayloads:
    status: Optional[Dict[str, Any]] = None
    sensors: Optional[Dict[str, Any]] = None
    events: List[Dict[str, Any]] = field(default_factory=list)


class TelemetryNormalizer:
    """Maintain Moonraker state and project it into the Owl contract."""

    TERMINAL_STATE_TTL = timedelta(seconds=10)
    JOB_ACTIVITY_TTL = timedelta(seconds=180)

    def __init__(self) -> None:
        self._initialized = False
        self._print_stats: Dict[str, Any] = {}
        self._status_sections: Dict[str, Any] = {}
        # Backwards compatibility: legacy tests and helpers reference
        # _status_state, keep it as an alias of the canonical map.
        self._status_state = self._status_sections
        self._proc_state: Dict[str, Any] = {}
        self._file_metadata: Dict[str, Any] = {}
        self._pending_events: List[Dict[str, Any]] = []
        self._temperature_state: Dict[str, Dict[str, Optional[float]]] = {}
        self._last_status_timestamp: Optional[str] = None
        self._last_print_stats_at: Optional[datetime] = None
        self._active_job_hint: Optional[str] = None
        self._active_job_expires_at: Optional[datetime] = None
        self._gcode_temp_pattern = re.compile(
            r"(?P<label>[TB]\d?):\s*(?P<actual>-?\d+(?:\.\d+)?)\s*/\s*(?P<target>-?\d+(?:\.\d+)?)",
            re.IGNORECASE,
        )
        self._state_resolver = PrinterStateResolver(
            terminal_ttl=self.TERMINAL_STATE_TTL
        )

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def ingest(self, payload: Dict[str, Any]) -> NormalizedPayloads:
        observed_at = datetime.now(timezone.utc)
        self._process_payload(payload, observed_at)

        status_payload = self._build_status_payload(observed_at)
        sensors_payload = self._build_sensors_payload()
        events_payload = self._drain_events()

        self._initialized = True

        return NormalizedPayloads(
            status=status_payload,
            sensors=sensors_payload,
            events=events_payload,
        )

    # ------------------------------------------------------------------
    # payload ingestion
    # ------------------------------------------------------------------
    def _process_payload(self, payload: Dict[str, Any], observed_at: datetime) -> None:
        result = payload.get("result")
        if isinstance(result, dict):
            status = result.get("status")
            if isinstance(status, dict):
                allow_print_stats = not self._initialized
                self._apply_status(status, observed_at, allow_print_stats)
            self._capture_side_effects("snapshot", result)

        method = payload.get("method")
        params = payload.get("params")

        if method == "notify_status_update":
            for entry in _iter_entries(params):
                if isinstance(entry, dict) and "status" in entry:
                    status = entry.get("status")
                    if isinstance(status, dict):
                        self._apply_status(status, observed_at, allow_print_stats=True)
                        entry = {
                            key: value
                            for key, value in entry.items()
                            if key != "status"
                        }
                if isinstance(entry, dict):
                    self._apply_status(entry, observed_at, allow_print_stats=True)
                    self._capture_side_effects(method, entry)
                else:
                    self._capture_side_effects(method, entry)

        elif method == "notify_print_stats_update":
            for entry in _iter_entries(params):
                self._apply_print_stats_entry(entry, observed_at)

        else:
            self._capture_side_effects(method, params)

    def _apply_status(
        self,
        status: Dict[str, Any],
        observed_at: datetime,
        allow_print_stats: bool,
    ) -> None:
        for key, value in status.items():
            if key == "print_stats":
                if allow_print_stats:
                    self._apply_print_stats(value, observed_at)
                continue

            existing_section = self._status_sections.get(key)
            if isinstance(existing_section, dict) and isinstance(value, dict):
                deep_merge(existing_section, _clone(value))
            else:
                self._status_sections[key] = _clone(value)

            if key in {
                "moonraker_stats",
                "system_cpu_usage",
                "system_memory",
                "network",
                "cpu_temp",
                "websocket_connections",
            }:
                self._proc_state[key] = _clone(value)

        self._capture_side_effects("status", status)

    def _apply_print_stats_entry(self, entry: Any, observed_at: datetime) -> None:
        if isinstance(entry, dict):
            candidate = entry.get("print_stats")
            if isinstance(candidate, dict):
                self._apply_print_stats(candidate, observed_at)
            else:
                self._apply_print_stats(entry, observed_at)
            return

        if isinstance(entry, (list, tuple)) and len(entry) == 2:
            key, value = entry
            if key == "print_stats" and isinstance(value, dict):
                self._apply_print_stats(value, observed_at)
            else:
                self._apply_print_stats({key: _clone(value)}, observed_at)
            return

    def _apply_print_stats(self, updates: Any, observed_at: datetime) -> None:
        if not isinstance(updates, dict):
            return

        self._print_stats = {**self._print_stats, **_clone(updates)}
        self._status_sections["print_stats"] = _clone(self._print_stats)
        self._last_print_stats_at = observed_at

        filename = updates.get("filename")
        if isinstance(filename, str) and filename:
            self._file_metadata.setdefault(filename, {}).setdefault(
                "relativePath", filename
            )

    def _refresh_job_activity(
        self, updates: Dict[str, Any], observed_at: datetime
    ) -> None:
        if not updates:
            return

        filename = updates.get("filename")
        if isinstance(filename, str):
            sanitized = filename.strip()
            if sanitized:
                self._active_job_hint = sanitized
                self._active_job_expires_at = observed_at + self.JOB_ACTIVITY_TTL

        state_value = _extract_state_value(updates)
        normalized_state = (
            state_value.strip().lower() if isinstance(state_value, str) else ""
        )

        active_states = {"printing", "resuming", "paused", "cancelling", "canceling"}
        terminal_states = {
            "complete",
            "completed",
            "cancelled",
            "canceled",
            "error",
            "shutdown",
        }

        if normalized_state in terminal_states:
            self._active_job_expires_at = None
            return

        if normalized_state in active_states:
            self._active_job_expires_at = observed_at + self.JOB_ACTIVITY_TTL
            return

        if self._active_job_hint:
            progress_value = _safe_float(updates.get("progress"))
            duration_value = _safe_float(updates.get("print_duration"))
            total_duration_value = _safe_float(updates.get("total_duration"))
            if any(
                value is not None
                for value in (progress_value, duration_value, total_duration_value)
            ):
                self._active_job_expires_at = observed_at + self.JOB_ACTIVITY_TTL

    def _active_job_recent(self, observed_at: datetime) -> bool:
        return (
            self._active_job_expires_at is not None
            and observed_at <= self._active_job_expires_at
        )

    def _compose_job_payload(
        self, observed_at: datetime
    ) -> tuple[Dict[str, Any], Optional[Dict[str, Any]], Optional[str]]:
        overview_updates: Dict[str, Any] = {}
        job_payload: Dict[str, Any] = {}
        sub_status: Optional[str] = None

        raw_filename = self._print_stats.get("filename")
        effective_filename: Optional[str] = None
        if isinstance(raw_filename, str) and raw_filename.strip():
            effective_filename = raw_filename.strip()
        elif self._active_job_hint and self._active_job_recent(observed_at):
            effective_filename = self._active_job_hint

        metadata: Optional[Dict[str, Any]] = None
        if effective_filename:
            metadata = self._file_metadata.get(effective_filename)
            overview_updates["jobName"] = effective_filename

            file_info: Dict[str, Any] = {
                "name": effective_filename,
                "path": effective_filename,
            }

            if isinstance(metadata, dict):
                size = _coerce_int(metadata.get("size"))
                if size is not None:
                    file_info["sizeBytes"] = size

                relative_path = metadata.get("relativePath")
                if isinstance(relative_path, str) and relative_path.strip():
                    file_info["relativePath"] = relative_path.strip()

                thumbnails = metadata.get("thumbnails")
                if thumbnails:
                    job_payload["thumbnails"] = thumbnails
                    if not job_payload.get("thumbnail"):
                        first_thumb = (
                            thumbnails[0] if isinstance(thumbnails, list) else None
                        )
                        if isinstance(first_thumb, dict):
                            job_payload["thumbnail"] = first_thumb

            file_info.setdefault("relativePath", effective_filename)

            job_payload["name"] = effective_filename
            job_payload["sourcePath"] = file_info.get(
                "relativePath", effective_filename
            )
            job_payload["file"] = file_info

        job_id = _derive_job_id(effective_filename)
        if job_id:
            job_payload["id"] = job_id

        message = self._print_stats.get("message")
        if isinstance(message, str) and message.strip():
            job_payload["message"] = message.strip()
            sub_status = message.strip()

        progress = _build_progress_section(
            self._status_sections.get("display_status"),
            self._status_sections.get("virtual_sdcard"),
            self._print_stats,
        )
        if progress:
            job_payload["progressPercent"] = progress.get("percent")
            if progress.get("elapsedSeconds") is not None:
                job_payload.setdefault("progress", {})["elapsed"] = _format_duration(
                    progress["elapsedSeconds"]
                )
                job_payload["progress"]["elapsedSeconds"] = progress["elapsedSeconds"]
            if progress.get("remainingSeconds") is not None:
                job_payload.setdefault("progress", {})["remaining"] = _format_duration(
                    progress["remainingSeconds"]
                )
                job_payload["progress"]["remainingSeconds"] = progress[
                    "remainingSeconds"
                ]

        info = (
            self._print_stats.get("info")
            if isinstance(self._print_stats, dict)
            else None
        )
        current_layer = None
        total_layer = None
        if isinstance(info, dict):
            current_layer = _coerce_int(
                info.get("current_layer") or info.get("currentLayer")
            )
            total_layer = _coerce_int(info.get("total_layer") or info.get("totalLayer"))

        if total_layer is not None:
            job_payload.setdefault("layers", {})["total"] = total_layer
        if current_layer is not None:
            job_payload.setdefault("layers", {})["current"] = current_layer

        return overview_updates, job_payload or None, sub_status

    def _has_active_job(
        self, job_payload: Optional[Dict[str, Any]], observed_at: datetime
    ) -> bool:
        if isinstance(job_payload, dict):
            source_path = job_payload.get("sourcePath") or job_payload.get("name")
            if isinstance(source_path, str) and source_path.strip():
                return True
        return self._active_job_recent(observed_at)

    def _capture_side_effects(self, method: Optional[str], payload: Any) -> None:
        if method in {"notify_status_update", "status", "snapshot"}:
            if isinstance(payload, dict):
                self._capture_proc_stats(payload)
                self._capture_file_metadata(payload)
        elif method == "notify_proc_stat_update":
            if isinstance(payload, list):
                for entry in payload:
                    if isinstance(entry, dict):
                        self._capture_proc_stats(entry)
        elif method == "notify_filelist_changed":
            if isinstance(payload, list):
                for entry in payload:
                    if isinstance(entry, dict):
                        self._capture_file_metadata(entry)
        self._capture_events(method, payload)

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
                deep_merge(target, value)
            else:
                self._proc_state[key] = value

    def _capture_file_metadata(self, entry: Dict[str, Any]) -> None:
        item = entry.get("item")
        if isinstance(item, dict):
            path = item.get("path")
            if isinstance(path, str) and path:
                record = self._file_metadata.setdefault(path, {})
                size = _coerce_int(item.get("size"))
                if size is not None:
                    record["size"] = size
                modified = item.get("modified")
                if modified is not None:
                    record["modified"] = modified
                record.setdefault("relativePath", path)

        job = entry.get("job")
        if isinstance(job, dict):
            filename = job.get("filename")
            metadata = job.get("metadata")
            if isinstance(filename, str) and filename:
                record = self._file_metadata.setdefault(filename, {})
                record.setdefault("relativePath", filename)

                if isinstance(metadata, dict):
                    size = _coerce_int(metadata.get("size"))
                    if size is not None:
                        record["size"] = size

                    modified = metadata.get("modified")
                    if modified is not None:
                        record["modified"] = modified

                    layer_count = metadata.get("layer_count") or metadata.get(
                        "layerCount"
                    )
                    layer_count_value = _coerce_int(layer_count)
                    if layer_count_value is not None:
                        record["layerCount"] = layer_count_value

                    thumbnails = _sanitize_thumbnails(metadata.get("thumbnails"))
                    if thumbnails:
                        record["thumbnails"] = thumbnails

                    relative_path = metadata.get("relative_path") or metadata.get(
                        "relativePath"
                    )
                    if isinstance(relative_path, str) and relative_path.strip():
                        record["relativePath"] = relative_path.strip()

    def _capture_events(self, method: Optional[str], params: Any) -> None:
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

            channel = _map_gcode_channel(label)
            if channel is None:
                continue

            channel_state = self._status_sections.setdefault(channel, {})
            if actual is not None:
                channel_state["temperature"] = actual
            if target is not None:
                channel_state["target"] = target

            stored = self._temperature_state.setdefault(channel, {})
            if actual is not None:
                stored["actual"] = actual
            if target is not None:
                stored["target"] = target

    # ------------------------------------------------------------------
    # builders
    # ------------------------------------------------------------------
    def _build_status_payload(
        self, observed_at: datetime
    ) -> Optional[Dict[str, Any]]:
        status_updates, job_payload, sub_status = self._compose_job_payload(
            observed_at
        )

        printer_status = self._state_resolver.resolve(
            _extract_state_value(self._print_stats),
            PrinterContext(
                observed_at=observed_at,
                has_active_job=self._has_active_job(job_payload, observed_at),
                is_heating=self._is_heating_for_print(),
            ),
        )

        status: Dict[str, Any] = {"printerStatus": printer_status}

        if status_updates:
            status.update(status_updates)

        progress = _build_progress_section(
            self._status_sections.get("display_status"),
            self._status_sections.get("virtual_sdcard"),
            self._print_stats,
        )

        if progress:
            percent_value = progress.get("percent")
            if percent_value is not None:
                status["progressPercent"] = percent_value
            if (elapsed := progress.get("elapsedSeconds")) is not None:
                status["elapsedSeconds"] = elapsed
            if (remaining := progress.get("remainingSeconds")) is not None:
                status["estimatedTimeRemainingSeconds"] = remaining

        if job_payload:
            status["job"] = job_payload

        if sub_status:
            status.setdefault("subStatus", sub_status)
        else:
            display_status = self._status_sections.get("display_status")
            if isinstance(display_status, dict):
                candidate = display_status.get("message")
                if isinstance(candidate, str) and candidate.strip():
                    status.setdefault("subStatus", candidate.strip())

        self._last_status_timestamp = observed_at.isoformat(timespec="seconds")
        status["lastUpdatedUtc"] = self._last_status_timestamp

        return status

    def _build_sensors_payload(self) -> Optional[Dict[str, Any]]:
        toolhead = _build_toolhead_section(self._status_sections)
        temperatures = _build_temperature_section(
            self._status_sections, self._temperature_state
        )
        fans = _build_fan_section(self._status_sections)

        payload: Dict[str, Any] = {}
        if toolhead:
            payload["toolhead"] = toolhead
        if temperatures:
            payload["temperatures"] = temperatures
        if fans:
            payload["fans"] = fans

        return payload or None

    def _is_heating_for_print(self) -> bool:
        HEATING_THRESHOLD = 40.0
        HEATING_DELTA = 5.0

        for key in ("extruder", "heater_bed"):
            channel = self._status_sections.get(key)
            if not isinstance(channel, dict):
                continue
            target = _safe_float(channel.get("target"))
            actual = _safe_float(channel.get("temperature"))
            if target is None or target <= HEATING_THRESHOLD:
                continue
            if actual is None or (target - actual) > HEATING_DELTA:
                return True

        for key, value in self._status_sections.items():
            if not isinstance(value, dict) or not key.startswith("heater_generic"):
                continue
            target = _safe_float(value.get("target"))
            actual = _safe_float(value.get("temperature"))
            if target is None or target <= HEATING_THRESHOLD:
                continue
            if actual is None or (target - actual) > HEATING_DELTA:
                return True

        return False


# ----------------------------------------------------------------------
# helper functions
# ----------------------------------------------------------------------


def _iter_entries(params: Any) -> List[Any]:
    if params is None:
        return []
    if isinstance(params, list):
        return params
    if isinstance(params, dict):
        return [params]
    return [params]


def _clone(value: Any) -> Any:
    return copy.deepcopy(value)


def _extract_state_value(candidate: Any) -> Optional[str]:
    if isinstance(candidate, dict):
        state = candidate.get("state")
        if isinstance(state, str):
            return state
    if isinstance(candidate, str):
        return candidate
    return None


def _build_progress_section(
    display_status: Any,
    virtual_sdcard: Any,
    print_stats: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
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

    # Use floor (truncation) for progress percentage to avoid showing 100% prematurely
    progress: Dict[str, Any] = {"percent": int(percent)}

    elapsed = _safe_float(print_stats.get("print_duration"))
    if elapsed is not None:
        progress["elapsedSeconds"] = max(int(elapsed), 0)

    total_duration = _safe_float(print_stats.get("total_duration"))
    if total_duration is not None and elapsed is not None and total_duration >= elapsed:
        progress["remainingSeconds"] = max(int(total_duration - elapsed), 0)

    return progress


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


def _build_toolhead_section(
    status_sections: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    toolhead = status_sections.get("toolhead")
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
    status_sections: Dict[str, Any],
    temperature_state: Dict[str, Dict[str, Optional[float]]],
) -> List[Dict[str, Any]]:
    temperatures: List[Dict[str, Any]] = []

    for key, value in status_sections.items():
        if key not in {"extruder", "heater_bed"} and not key.startswith(
            "temperature_sensor"
        ):
            continue
        if not isinstance(value, dict):
            continue

        channel = key.strip()
        _merge_temperature_entry(temperatures, channel, value, temperature_state)

    return [
        temp
        for temp in temperatures
        if temp.get("actual") is not None or temp.get("target") is not None
    ]


def _merge_temperature_entry(
    collection: List[Dict[str, Any]],
    channel: str,
    data: Dict[str, Any],
    temperature_state: Dict[str, Dict[str, Optional[float]]],
) -> None:
    previous = temperature_state.get(channel, {})

    actual_candidate = _round_temperature(data.get("temperature"))
    target_candidate = _round_temperature(data.get("target"))

    actual = (
        actual_candidate if actual_candidate is not None else previous.get("actual")
    )
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
        "actual": actual,
        "target": target,
    }


def _build_fan_section(status_sections: Dict[str, Any]) -> List[Dict[str, Any]]:
    fan = status_sections.get("fan")
    if not isinstance(fan, dict):
        return []

    speed = _safe_float(fan.get("speed"))
    if speed is None:
        return []

    percent = speed * 100.0 if speed <= 1.0 else speed
    return [
        {
            "name": "part",
            "percent": round(max(0.0, min(percent, 100.0)), 1),
        }
    ]


def _sanitize_thumbnails(entries: Any) -> List[Dict[str, Any]]:
    sanitized: List[Dict[str, Any]] = []
    if not isinstance(entries, Iterable):
        return sanitized

    for raw in entries:
        if not isinstance(raw, dict):
            continue

        path = raw.get("relative_path") or raw.get("relativePath")
        if not isinstance(path, str) or not path.strip():
            continue

        entry: Dict[str, Any] = {"relativePath": path.strip()}

        width = _coerce_int(raw.get("width"))
        if width is not None:
            entry["width"] = width

        height = _coerce_int(raw.get("height"))
        if height is not None:
            entry["height"] = height

        size_bytes = _coerce_int(raw.get("size") or raw.get("sizeBytes"))
        if size_bytes is not None:
            entry["sizeBytes"] = size_bytes

        sanitized.append(entry)

    return sanitized


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
        quantized = decimal_value.quantize(Decimal("0.1"), rounding=ROUND_FLOOR)
        return float(quantized)
    except (InvalidOperation, ValueError):
        scaled = int(numeric * 10)
        return scaled / 10.0


def _safe_float_from_fraction(
    container: Optional[Dict[str, Any]], key: str
) -> Optional[float]:
    if not isinstance(container, dict):
        return None
    return _safe_float(container.get(key))


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _format_duration(seconds: float) -> str:
    seconds = max(0.0, seconds)
    duration = timedelta(seconds=float(seconds))
    total_seconds = int(duration.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


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
