"""Normalization helpers for Moonraker telemetry payloads."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from typing import Any, Dict, Iterable, List, Optional

from .core import deep_merge


@dataclass(slots=True)
class NormalizedPayloads:
    """Structured payloads ready for publishing to Nexus."""

    overview: Optional[Dict[str, Any]] = None
    sensors: Optional[Dict[str, Any]] = None
    events: List[Dict[str, Any]] = field(default_factory=list)


class TelemetryNormalizer:
    """Maintains Moonraker state and projects it into the Owl contract."""

    def __init__(self) -> None:
        self._status_state: Dict[str, Any] = {}
        self._proc_state: Dict[str, Any] = {}
        self._file_metadata: Dict[str, Any] = {}
        self._pending_events: List[Dict[str, Any]] = []
        self._temperature_state: Dict[str, Dict[str, Optional[float]]] = {}
        self._last_overview_timestamp: Optional[str] = None
        self._gcode_temp_pattern = re.compile(
            r"(?P<label>[TB]\d?):\s*(?P<actual>-?\d+(?:\.\d+)?)\s*/\s*(?P<target>-?\d+(?:\.\d+)?)",
            re.IGNORECASE,
        )
        self._last_terminal_state: Optional[tuple[str, datetime]] = None
        self._terminal_state_ttl = timedelta(seconds=10)
        self._current_print_state: Optional[str] = None
        self._previous_print_state: Optional[str] = None

    def ingest(self, payload: Dict[str, Any]) -> NormalizedPayloads:
        self._apply_payload(payload)

        overview_payload = self._build_overview_payload()
        sensors_payload = self._build_sensors_payload()
        events_payload = self._drain_events()

        return NormalizedPayloads(
            overview=overview_payload,
            sensors=sensors_payload,
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
                self._update_terminal_state(
                    _extract_state_value(status.get("print_stats"))
                )

        method = payload.get("method")
        params = payload.get("params")
        entries: List[Any] = []
        if isinstance(params, list):
            entries = params
        elif isinstance(params, dict):
            entries = [params]

        for entry in entries:
            self._apply_param_entry(method, entry)

        self._capture_events(method, params)

    def _apply_param_entry(self, method: Optional[str], entry: Any) -> None:
        if isinstance(entry, dict):
            status = entry.get("status")
            if isinstance(status, dict):
                _deep_merge(self._status_state, status)
                self._update_terminal_state(
                    _extract_state_value(status.get("print_stats"))
                )
            elif method == "notify_status_update":
                # Some Moonraker builds emit status fields directly without a wrapper.
                _deep_merge(self._status_state, entry)
                self._update_terminal_state(_extract_state_value(entry))

            print_stats = entry.get("print_stats")
            if isinstance(print_stats, dict):
                # Moonraker often sends print_stats updates outside the status wrapper.
                target = self._status_state.setdefault("print_stats", {})
                _deep_merge(target, print_stats)
                self._update_terminal_state(_extract_state_value(print_stats))
            elif method == "notify_print_stats_update":
                target = self._status_state.setdefault("print_stats", {})
                _deep_merge(target, entry)
                self._update_terminal_state(_extract_state_value(entry))

            self._capture_proc_stats(entry)
            self._capture_file_metadata(entry)
            return

        if method == "notify_print_stats_update" and isinstance(entry, (list, tuple)):
            if (
                len(entry) == 2
                and entry[0] == "print_stats"
                and isinstance(entry[1], dict)
            ):
                target = self._status_state.setdefault("print_stats", {})
                _deep_merge(target, entry[1])
                self._update_terminal_state(_extract_state_value(entry[1]))

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
    def _build_overview_payload(self) -> Optional[Dict[str, Any]]:
        job = _build_job_section(self._status_state, self._file_metadata) or {}
        printer_status = self._resolve_printer_status(job)
        # timelapse_paused = self._resolve_timelapse_paused()
        # idle_timeout_state = self._resolve_idle_timeout_state()

        # if timelapse_paused and printer_status == "Paused":
        #     printer_status = "Printing"

        overview: Dict[str, Any] = {"printerStatus": printer_status}

        # if timelapse_paused is not None:
        #     overview["timelapsePaused"] = timelapse_paused

        # if idle_timeout_state:
        #     overview["idleTimeoutState"] = idle_timeout_state

        progress = job.get("progress") if isinstance(job, dict) else None
        percent: Optional[int] = None
        elapsed: Optional[int] = None
        remaining: Optional[int] = None
        if isinstance(progress, dict):
            percent_value = _coerce_int(progress.get("percent"))
            if percent_value is not None:
                percent = percent_value
                overview["progressPercent"] = percent_value

            elapsed_value = _coerce_int(progress.get("elapsedSeconds"))
            if elapsed_value is not None:
                elapsed = elapsed_value
                overview["elapsedSeconds"] = elapsed_value

            remaining_value = _coerce_int(progress.get("remainingSeconds"))
            if remaining_value is not None:
                remaining = remaining_value
                overview["estimatedTimeRemainingSeconds"] = remaining_value

        job_payload: Dict[str, Any] = {}
        file_info = job.get("file") if isinstance(job, dict) else None
        if isinstance(file_info, dict):
            name = file_info.get("name")
            if isinstance(name, str) and name.strip():
                normalized_name = name.strip()
                overview["jobName"] = normalized_name
                job_payload["name"] = normalized_name

            path = file_info.get("path") or file_info.get("relativePath")
            if isinstance(path, str) and path.strip():
                job_payload["sourcePath"] = path.strip()

            size_bytes = _coerce_int(file_info.get("sizeBytes"))
            if size_bytes is not None:
                job_payload["sizeBytes"] = size_bytes

        job_id = job.get("id")
        if isinstance(job_id, str) and job_id.strip():
            job_payload["id"] = job_id.strip()

        job_message = job.get("message")
        if isinstance(job_message, str) and job_message.strip():
            job_payload["message"] = job_message.strip()

        if percent is not None:
            job_payload["progressPercent"] = percent
        if elapsed is not None:
            job_payload["elapsedSeconds"] = elapsed
        if remaining is not None:
            job_payload["estimatedTimeRemainingSeconds"] = remaining

        layers = job.get("layers")
        layer_status: Optional[str] = None
        if isinstance(layers, dict):
            sanitized_layers: Dict[str, int] = {}
            current_layer = _coerce_int(layers.get("current"))
            total_layer = _coerce_int(layers.get("total"))

            if current_layer is not None:
                sanitized_layers["current"] = current_layer
            if total_layer is not None:
                sanitized_layers["total"] = total_layer

            if sanitized_layers:
                job_payload["layers"] = sanitized_layers

            layer_status = _format_layer_status(current_layer, total_layer)

        thumbnails = job.get("thumbnails")
        selected_thumbnail = _select_thumbnail_entry(thumbnails) if thumbnails else None

        if job_payload:
            thumbnail_payload: Dict[str, Any] = {
                "cloudUrl": None,
                "sourcePath": selected_thumbnail.get("relativePath")
                if selected_thumbnail
                else None,
            }

            if selected_thumbnail:
                if (width := selected_thumbnail.get("width")) is not None:
                    thumbnail_payload["width"] = width
                if (height := selected_thumbnail.get("height")) is not None:
                    thumbnail_payload["height"] = height
                if (size := selected_thumbnail.get("sizeBytes")) is not None:
                    thumbnail_payload["sizeBytes"] = size

            job_payload["thumbnail"] = thumbnail_payload
            overview["job"] = job_payload

        sub_status = layer_status
        if not sub_status and isinstance(job_payload.get("message"), str):
            sub_status = job_payload["message"]

        if not sub_status:
            display_status = self._status_state.get("display_status")
            if isinstance(display_status, dict):
                candidate = display_status.get("message")
                if isinstance(candidate, str) and candidate.strip():
                    sub_status = candidate.strip()

        if sub_status:
            overview["subStatus"] = sub_status

        # Refresh heartbeat on every publish so connectivity checks remain accurate.
        self._last_overview_timestamp = datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        )
        overview["lastUpdatedUtc"] = self._last_overview_timestamp

        return overview

    def _resolve_idle_timeout_state(self) -> Optional[str]:
        idle_timeout = self._status_state.get("idle_timeout")
        if not isinstance(idle_timeout, dict):
            return None

        state = idle_timeout.get("state")
        if isinstance(state, str) and state.strip():
            return state.strip()

        return None

    # def _resolve_timelapse_paused(self) -> Optional[bool]:
    #     macro = self._status_state.get("gcode_macro TIMELAPSE_TAKE_FRAME")
    #     if not isinstance(macro, dict):
    #         return None

    #     candidate = macro.get("is_paused")
    #     if isinstance(candidate, bool):
    #         return candidate
    #     if isinstance(candidate, (int, float)):
    #         return bool(candidate)
    #     if isinstance(candidate, str):
    #         normalized = candidate.strip().lower()
    #         if normalized in {"true", "1", "yes", "on"}:
    #             return True
    #         if normalized in {"false", "0", "no", "off"}:
    #             return False

    #     return None

    def _resolve_printer_status(self, job: Dict[str, Any]) -> str:
        """
        Resolve printer status using Mainsail/Obico-style context-aware detection.

        Priority order:
        1. Explicit print states (printing, paused, cancelled, etc.)
        2. Error/shutdown states
        3. Heating detection (ready/standby + heater targets)
        4. Job context (ready/standby + active job file)
        5. Idle timeout override (Klipper macro support)
        6. Default idle state

        Matches Mainsail behavior for heating phase detection and
        Obico behavior for job-loaded detection.
        """
        raw_state: Optional[str] = None

        if isinstance(self._current_print_state, str):
            raw_state = self._current_print_state

        if raw_state is None and isinstance(job, dict):
            candidate = job.get("status")
            if isinstance(candidate, str):
                raw_state = candidate

        # Normalize status for downstream mapping
        if raw_state:
            raw_label = raw_state.strip()
            normalized = raw_label.lower()
        else:
            raw_label = None
            normalized = ""

        # ═══════════════════════════════════════════════════════════
        # Priority 1: Explicit Print States
        # ═══════════════════════════════════════════════════════════
        if normalized in {"printing", "resuming"}:
            return "Printing"

        if normalized == "paused":
            # # Check timelapse pause override (Mainsail behavior)
            # timelapse_paused = self._resolve_timelapse_paused()
            # if timelapse_paused:
            #     return "Printing"  # Timelapse pause is still printing
            return "Paused"

        if normalized in {"cancelling", "canceling"}:
            return "Cancelling"

        # Map cancelled spelling to Cancelled (explicit, not Completed)
        if normalized in {"cancelled", "canceled"}:
            return "Cancelled"

        if normalized in {"complete", "completed"}:
            return "Completed"

        # ═══════════════════════════════════════════════════════════
        # Priority 2: Error States
        # ═══════════════════════════════════════════════════════════
        if normalized in {"error", "shutdown"}:
            return "Error"

        if normalized == "offline":
            return "Offline"

        # ═══════════════════════════════════════════════════════════
        # Priority 3: Ambiguous States (ready/standby/idle)
        # Apply Mainsail/Obico heuristics
        # ═══════════════════════════════════════════════════════════
        if normalized in {"standby", "ready", "idle"}:
            if self._has_active_job(job):
                self._last_terminal_state = None
                return "Printing"

            last_terminal = self._last_terminal_state
            if last_terminal is not None:
                state_value, observed_at = last_terminal
                if datetime.now(timezone.utc) - observed_at <= self._terminal_state_ttl:
                    mapped = self._map_terminal_status(state_value)
                    if mapped is not None:
                        return mapped
                else:
                    self._last_terminal_state = None

            # # Check 1: Heating for print (Mainsail behavior)
            # if self._is_heating_for_print():
            #     return "Printing"
            # # Check 3: Idle timeout state override (Klipper macro support)
            # idle_timeout_state = self._resolve_idle_timeout_state()
            # if idle_timeout_state and idle_timeout_state.lower() == "printing":
            #     return "Printing"

            # Default: Actually idle
            return "Idle"

        # ═══════════════════════════════════════════════════════════
        # Fallback: Unknown state
        # ═══════════════════════════════════════════════════════════
        return "Unknown"

    def _update_terminal_state(self, state: Optional[str]) -> None:
        if not isinstance(state, str):
            return

        normalized = state.strip().lower()
        if normalized != self._current_print_state:
            self._previous_print_state = self._current_print_state
            self._current_print_state = normalized
        if not normalized:
            return

        terminal_states = {
            "cancelled",
            "canceled",
            "complete",
            "completed",
            "error",
            "shutdown",
        }
        active_states = {
            "printing",
            "resuming",
            "paused",
            "cancelling",
            "canceling",
        }

        if normalized in terminal_states:
            self._last_terminal_state = (normalized, datetime.now(timezone.utc))
        elif normalized in active_states:
            self._last_terminal_state = None

    @staticmethod
    def _map_terminal_status(state: str) -> Optional[str]:
        normalized = state.strip().lower()
        mapping = {
            "cancelled": "Cancelled",
            "canceled": "Cancelled",
            "completed": "Completed",
            "complete": "Completed",
            "error": "Error",
            "shutdown": "Error",
        }
        return mapping.get(normalized)

    def _is_heating_for_print(self) -> bool:
        """
        Detect if printer is heating bed/extruder in preparation for a print.

        Matches Mainsail behavior: Any heater with target > threshold is "heating".

        Returns:
            True if extruder target > 40°C OR bed target > 40°C
            AND current temp is significantly below target (>5°C delta)
        """
        HEATING_THRESHOLD = 40.0  # °C - minimum target to consider "heating"
        HEATING_DELTA = 5.0  # °C - minimum delta to consider "in progress"

        # Check extruder
        extruder = self._status_state.get("extruder")
        if isinstance(extruder, dict):
            target = _safe_float(extruder.get("target"))
            actual = _safe_float(extruder.get("temperature"))

            if target is not None and target > HEATING_THRESHOLD:
                if actual is None or (target - actual) > HEATING_DELTA:
                    return True  # Heating extruder for print

        # Check heated bed
        heater_bed = self._status_state.get("heater_bed")
        if isinstance(heater_bed, dict):
            target = _safe_float(heater_bed.get("target"))
            actual = _safe_float(heater_bed.get("temperature"))

            if target is not None and target > HEATING_THRESHOLD:
                if actual is None or (target - actual) > HEATING_DELTA:
                    return True  # Heating bed for print

        # Check generic heaters (e.g., chamber)
        for key, value in self._status_state.items():
            if not isinstance(value, dict):
                continue
            if not key.startswith("heater_generic"):
                continue

            target = _safe_float(value.get("target"))
            actual = _safe_float(value.get("temperature"))

            if target is not None and target > HEATING_THRESHOLD:
                if actual is None or (target - actual) > HEATING_DELTA:
                    return True

        return False

    def _has_active_job(self, job: Dict[str, Any]) -> bool:
        """Check if a print job file is loaded (business logic: job loaded = printing workflow)."""
        if not isinstance(job, dict):
            return False

        file_info = job.get("file")
        if not isinstance(file_info, dict):
            return False

        filename = file_info.get("name") or file_info.get("path")
        return isinstance(filename, str) and bool(filename.strip())

    def _build_sensors_payload(self) -> Optional[Dict[str, Any]]:
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

    meta: Dict[str, Any] = {}
    filename = print_stats.get("filename")
    if isinstance(filename, str) and filename:
        sanitized_name = filename.strip()
        meta = file_metadata.get(filename) or {}
        file_info: Dict[str, Any] = {
            "name": sanitized_name,
            "path": sanitized_name,
        }

        size = _coerce_int(meta.get("size"))
        if size is not None:
            file_info["sizeBytes"] = size

        relative_path = meta.get("relativePath")
        if isinstance(relative_path, str) and relative_path.strip():
            file_info["relativePath"] = relative_path.strip()

        thumbnails = meta.get("thumbnails")
        if thumbnails:
            job["thumbnails"] = thumbnails

        layer_count_meta = _coerce_int(meta.get("layerCount"))
        if layer_count_meta is not None:
            job["layerCount"] = layer_count_meta

        job["file"] = file_info

    job_id = _derive_job_id(filename)
    if job_id:
        job["id"] = job_id

    message = print_stats.get("message")
    if isinstance(message, str) and message:
        job["message"] = message

    progress = _build_progress_section(status_state)
    percent_value: Optional[float] = None
    if progress:
        job["progress"] = progress
        percent_candidate = progress.get("percent")
        if percent_candidate is not None:
            try:
                percent_value = float(percent_candidate)
            except (TypeError, ValueError):
                percent_value = None

    info = print_stats.get("info")
    current_layer = None
    total_layer = None
    if isinstance(info, dict):
        current_layer = _coerce_int(
            info.get("current_layer") or info.get("currentLayer")
        )
        total_layer = _coerce_int(info.get("total_layer") or info.get("totalLayer"))

    if total_layer is None:
        total_layer = _coerce_int(meta.get("layerCount")) if meta else None

    if (
        current_layer is None
        and total_layer is not None
        and total_layer > 0
        and percent_value is not None
    ):
        estimated = int(round((percent_value / 100.0) * total_layer))
        current_layer = max(0, min(estimated, total_layer))
        if percent_value > 0.0 and current_layer == 0:
            current_layer = 1

    layers_payload: Dict[str, Any] = {}
    if current_layer is not None:
        layers_payload["current"] = current_layer
    if total_layer is not None:
        layers_payload["total"] = total_layer
    if layers_payload:
        job["layers"] = layers_payload

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

    # Preserve prior target when the current update omits it
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


def _select_thumbnail_entry(
    entries: Optional[Iterable[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    if not entries:
        return None

    best_entry: Optional[Dict[str, Any]] = None
    best_score = -1

    for candidate in entries:
        if not isinstance(candidate, dict):
            continue

        path = candidate.get("relativePath") or candidate.get("relative_path")
        if not isinstance(path, str) or not path.strip():
            continue

        sanitized: Dict[str, Any] = {"relativePath": path.strip()}

        width = _coerce_int(candidate.get("width"))
        if width is not None:
            sanitized["width"] = width

        height = _coerce_int(candidate.get("height"))
        if height is not None:
            sanitized["height"] = height

        size_bytes = _coerce_int(candidate.get("sizeBytes") or candidate.get("size"))
        if size_bytes is not None:
            sanitized["sizeBytes"] = size_bytes

        score = (sanitized.get("width") or 0) * (sanitized.get("height") or 0)
        if score > best_score:
            best_entry = sanitized
            best_score = score

    return best_entry


def _format_layer_status(current: Optional[int], total: Optional[int]) -> Optional[str]:
    if total is None or total <= 0:
        return None

    if current is None:
        return None

    current_value = max(0, current)
    current_value = min(current_value, total)

    return f"Layer {current_value}/{total}"


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


def _extract_state_value(candidate: Any) -> Optional[str]:
    if isinstance(candidate, dict):
        state = candidate.get("state")
        if isinstance(state, str):
            return state
    if isinstance(candidate, str):
        return candidate
    return None
