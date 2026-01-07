"""Domain trackers for the telemetry pipeline."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Optional

from .state_store import MoonrakerStateStore, SectionSnapshot

LOGGER = logging.getLogger(__name__)


@dataclass
class SessionInfo:
    """Represents the current print session state.

    Design follows Obico's PrinterState pattern:
    - session_id is None when no active job exists
    - Progress fields only meaningful when has_active_job is True
    - State determination follows Mainsail: print_stats.state -> idle_timeout.state -> Idle
    """

    # Core identification - None when no active job
    session_id: Optional[str]
    job_name: Optional[str]
    moonraker_job_id: Optional[str]  # Raw job_id from print_stats/history_event

    # State signals
    raw_state: Optional[str]  # print_stats.state
    idle_timeout_state: Optional[str]
    job_status: Optional[str]  # from history_event (takes precedence for terminal states)
    history_action: Optional[str]  # "added" or "finished" from notify_history_changed
    has_active_job: bool

    # Progress (only meaningful when has_active_job=True)
    progress_percent: Optional[float]
    elapsed_seconds: Optional[int]
    remaining_seconds: Optional[int]
    layer_current: Optional[int]
    layer_total: Optional[int]

    # Filament data (for Mainsail-compatible ETA calculation)
    filament_used: Optional[float]  # mm, from print_stats.filament_used
    filament_total: Optional[float]  # mm, from gcode metadata

    # Slicer estimated time (for Mainsail-compatible ETA calculation)
    slicer_estimated_time: Optional[int]  # seconds, from gcode metadata

    # Special states
    timelapse_paused: bool
    message: Optional[str]

    # Note: thumbnail_url has been removed from session telemetry.
    # Thumbnail URLs are now pushed via SignalR after upload ACK processing.


class PrintSessionTracker:
    """Derives stable session identifiers and job metadata.

    Design principles (aligned with Obico/Mainsail):
    - Session ID is cleared when job ends (not latched)
    - Active job detection is simple: print_stats.state in {printing, paused}
    - Terminal states from history_event take precedence
    - Progress calculation uses file-relative method (like Mainsail's default)
    - ETA calculation uses Mainsail algorithm: avg(file-based, filament-based)
    """

    def __init__(self) -> None:
        self._current_session_id: Optional[str] = None
        # File metadata for accurate progress calculation (Mainsail file-relative method)
        self._gcode_start_byte: Optional[int] = None
        self._gcode_end_byte: Optional[int] = None
        self._current_filename: Optional[str] = None
        # Slicer metadata for Mainsail-compatible ETA calculation
        self._filament_total: Optional[float] = None  # mm
        self._slicer_estimated_time: Optional[int] = None  # seconds

    def set_file_metadata(
        self,
        filename: str,
        gcode_start_byte: Optional[int],
        gcode_end_byte: Optional[int],
        filament_total: Optional[float] = None,
        slicer_estimated_time: Optional[int] = None,
    ) -> None:
        """Set file metadata for accurate progress and ETA calculation.

        Called when a new print job starts. These values are used to calculate:
        - Progress using the file-relative method (excluding slicer headers)
        - ETA using Mainsail algorithm: avg(file-based, filament-based)

        Args:
            filename: The gcode filename
            gcode_start_byte: Start byte of actual gcode (after slicer header)
            gcode_end_byte: End byte of gcode content
            filament_total: Total filament in mm from slicer metadata
            slicer_estimated_time: Slicer's estimated print time in seconds
        """
        self._current_filename = filename
        self._gcode_start_byte = gcode_start_byte
        self._gcode_end_byte = gcode_end_byte
        self._filament_total = filament_total
        self._slicer_estimated_time = slicer_estimated_time

        metadata_parts = []
        if gcode_start_byte is not None and gcode_end_byte is not None:
            metadata_parts.append(f"gcode: {gcode_start_byte}-{gcode_end_byte} bytes")
        if filament_total is not None:
            metadata_parts.append(f"filament: {filament_total:.1f}mm")
        if slicer_estimated_time is not None:
            metadata_parts.append(f"eta: {slicer_estimated_time}s")

        if metadata_parts:
            LOGGER.debug("File metadata set: %s (%s)", filename, ", ".join(metadata_parts))
        else:
            LOGGER.debug("File metadata set: %s (no metadata)", filename)

    def clear_file_metadata(self) -> None:
        """Clear file metadata when job ends."""
        self._current_filename = None
        self._gcode_start_byte = None
        self._gcode_end_byte = None
        self._filament_total = None
        self._slicer_estimated_time = None

    def compute(self, store: MoonrakerStateStore) -> SessionInfo:
        print_stats = _coerce_section(store.get("print_stats"))
        virtual_sdcard = _coerce_section(store.get("virtual_sdcard"))
        display_status = _coerce_section(store.get("display_status"))
        idle_timeout = _coerce_section(store.get("idle_timeout"))
        timelapse_macro = _coerce_section(store.get("gcode_macro TIMELAPSE_TAKE_FRAME"))
        history_event = _coerce_section(store.get("history_event"))

        observed_at = store.latest_observed_at()

        history_id = _extract_history_id(print_stats) or _extract_history_id(
            history_event
        )
        filename = (
            _extract_filename(print_stats)
            or _extract_filename(virtual_sdcard)
            or _extract_filename(history_event)
        )
        job_id = _extract_job_id(print_stats) or _extract_job_id(history_event)

        raw_state = _normalise_state(print_stats.get("state"))
        idle_state = _normalise_state(idle_timeout.get("state"))
        job_status = _normalise_state(_extract_job_status(history_event))
        history_action = history_event.get("action") if history_event else None
        timelapse_paused = bool(_as_bool(timelapse_macro.get("is_paused")))

        sd_active = bool(_as_bool(virtual_sdcard.get("is_active")))
        sd_printing = bool(_as_bool(virtual_sdcard.get("is_printing")))
        sd_paused = bool(_as_bool(virtual_sdcard.get("is_paused")))

        # Determine active job first - this affects session_id lifecycle
        has_active_job = self._has_active_job(
            raw_state=raw_state,
            job_status=job_status,
        )

        session_id = self._resolve_session_id(
            history_id=history_id,
            filename=filename,
            job_id=job_id,
            has_active_job=has_active_job,
        )

        # Use file-relative progress if metadata available, otherwise fallback
        # Check if filename matches current metadata (new job may not have metadata yet)
        use_file_metadata = (
            self._current_filename is not None
            and filename is not None
            and self._current_filename == filename
        )
        progress_percent = _extract_progress_percent(
            virtual_sdcard,
            filename=filename if use_file_metadata else None,
            gcode_start_byte=self._gcode_start_byte if use_file_metadata else None,
            gcode_end_byte=self._gcode_end_byte if use_file_metadata else None,
        )
        elapsed_seconds = _as_int(print_stats.get("print_duration"))

        # Extract filament used from print_stats
        filament_used = _to_float(print_stats.get("filament_used"))
        filament_total = self._filament_total if use_file_metadata else None
        slicer_estimated_time = self._slicer_estimated_time if use_file_metadata else None

        # Calculate remaining time using Mainsail algorithm
        remaining_seconds = _calculate_remaining_seconds_mainsail(
            elapsed_seconds=elapsed_seconds,
            progress_percent=progress_percent,
            filament_used=filament_used,
            filament_total=filament_total,
            slicer_estimated_time=slicer_estimated_time,
        )

        layer_current, layer_total = _extract_layers(print_stats)

        message = _extract_message(display_status, print_stats)
        message = _normalise_message(
            message,
            raw_state=raw_state,
            idle_state=idle_state,
            job_status=job_status,
            has_active_job=has_active_job,
        )

        # Clear file metadata when job ends
        if not has_active_job:
            if self._current_filename:
                self.clear_file_metadata()
                LOGGER.debug("Cleared file metadata (job ended)")

        return SessionInfo(
            session_id=session_id,
            job_name=filename,
            moonraker_job_id=job_id,
            raw_state=raw_state,
            idle_timeout_state=idle_state,
            job_status=job_status,
            history_action=history_action,
            has_active_job=has_active_job,
            progress_percent=progress_percent,
            elapsed_seconds=elapsed_seconds,
            remaining_seconds=remaining_seconds,
            layer_current=layer_current,
            layer_total=layer_total,
            filament_used=filament_used,
            filament_total=filament_total,
            slicer_estimated_time=slicer_estimated_time,
            timelapse_paused=timelapse_paused,
            message=message,
        )

    def _resolve_session_id(
        self,
        *,
        history_id: Optional[str],
        filename: Optional[str],
        job_id: Optional[str],
        has_active_job: bool,
    ) -> Optional[str]:
        """Resolve session ID based on active job state.

        Key behavior: session_id is None when no active job exists.
        This ensures UI clears job info when print ends (fixes stale job display).
        """
        # Clear session when job ends
        if not has_active_job:
            self._current_session_id = None
            return None

        # Generate new session ID if needed
        if history_id:
            new_id = f"history-{history_id}"
        elif job_id:
            new_id = f"job-{job_id}"
        elif filename:
            digest = hashlib.sha1(filename.encode("utf-8"), usedforsecurity=False)
            new_id = digest.hexdigest()
        else:
            # Active job but no identifiers - keep existing or generate placeholder
            return self._current_session_id

        self._current_session_id = new_id
        return self._current_session_id

    def _has_active_job(
        self,
        *,
        raw_state: Optional[str],
        job_status: Optional[str],
    ) -> bool:
        """Determine if there's an active print job.

        Aligned with Mainsail/Obico pattern:
        - Use print_stats.state as the authoritative source
        - Active states: printing, paused
        - Terminal states: complete, cancelled, standby, error

        Note: We do NOT use job_status from history_event to override this.
        Instead, we query print_stats on history events (action:added/finished)
        to ensure raw_state is always up-to-date.
        """
        raw = (raw_state or "").lower()

        # Active states from print_stats (aligned with Mainsail/Obico)
        active_states = {"printing", "paused"}
        return raw in active_states


@dataclass
class HeaterSnapshot:
    name: str
    observed_at: datetime
    temperature: Optional[float]
    target: Optional[float]


@dataclass
class HeaterMonitor:
    """Tracks heater readiness and warming state."""

    heaters: dict[str, HeaterSnapshot] = field(default_factory=dict)

    def refresh(self, store: MoonrakerStateStore) -> None:
        for section in store.iter_sections():
            if section.name.startswith("extruder") or section.name.startswith("heater"):
                temperature = _as_float(section.data.get("temperature"))
                target = _as_float(section.data.get("target"))
                self.heaters[section.name] = HeaterSnapshot(
                    name=section.name,
                    observed_at=section.observed_at,
                    temperature=temperature,
                    target=target,
                )

    def is_actively_heating(self) -> bool:
        """Check if any heater is actively warming up.

        Used for UI display (lifecycle.isHeating) to indicate the printer
        is heating before/during a print. This is NOT used for state
        determination - printer state follows Mainsail's pattern:
        ``print_stats.state ?? idle_timeout.state ?? "Idle"``

        Returns:
            True if any heater has a target > 40°C and hasn't reached
            within 5°C of that target.
        """
        for snapshot in self.heaters.values():
            if snapshot.target is None or snapshot.temperature is None:
                continue
            if snapshot.target <= 40:
                continue
            if snapshot.target - snapshot.temperature >= 5:
                return True
        return False


def _coerce_section(snapshot: Optional[SectionSnapshot]) -> Mapping[str, Any]:
    if snapshot is None:
        return {}
    return snapshot.data


def _extract_history_id(section: Mapping[str, Any]) -> Optional[str]:
    for candidate in _iter_candidate_sections(section):
        for key in ("history_id", "historyId"):
            value = candidate.get(key)
            if value:
                return str(value)
    return None


def _extract_filename(section: Mapping[str, Any]) -> Optional[str]:
    filename_keys = ("filename", "file_path", "filePath", "job_path")
    for candidate in _iter_candidate_sections(section):
        for key in filename_keys:
            value = candidate.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def _extract_job_id(section: Mapping[str, Any]) -> Optional[str]:
    for candidate in _iter_candidate_sections(section):
        for key in ("job_id", "jobId"):
            value = candidate.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def _extract_job_status(section: Mapping[str, Any]) -> Optional[str]:
    for candidate in _iter_candidate_sections(section):
        value = candidate.get("status")
        if isinstance(value, str) and value.strip():
            return value
    return None


def _iter_candidate_sections(section: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    if not isinstance(section, Mapping):
        return []

    stack: list[Mapping[str, Any]] = [section]
    seen: set[int] = set()
    candidates: list[Mapping[str, Any]] = []

    while stack:
        current = stack.pop()
        identifier = id(current)
        if identifier in seen:
            continue
        seen.add(identifier)
        candidates.append(current)

        for value in current.values():
            if isinstance(value, Mapping):
                stack.append(value)
            elif isinstance(value, (list, tuple)):
                for entry in value:
                    if isinstance(entry, Mapping):
                        stack.append(entry)

    return candidates


def _extract_message(
    display_status: Mapping[str, Any],
    print_stats: Mapping[str, Any],
) -> Optional[str]:
    for source in (print_stats, display_status):
        message = source.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
    return None


def _normalise_message(
    message: Optional[str],
    *,
    raw_state: Optional[str],
    idle_state: Optional[str],
    job_status: Optional[str],
    has_active_job: bool,
) -> Optional[str]:
    if not isinstance(message, str):
        return None

    cleaned = message.strip()
    if not cleaned:
        return None

    normalized = cleaned.lower()
    redundant = {"printing", "resuming", "pausing", "paused", "idle", "ready", "busy"}

    if not has_active_job and normalized in redundant:
        return None

    terminal_states = {
        "cancelled",
        "canceled",
        "complete",
        "completed",
        "error",
        "failed",
        "aborted",
    }

    if raw_state in terminal_states and normalized in redundant:
        return None

    if job_status in terminal_states and normalized in redundant:
        return None

    if idle_state in {"idle", "ready"} and normalized in redundant:
        return None

    return cleaned


def _calculate_remaining_seconds_mainsail(
    *,
    elapsed_seconds: Optional[int],
    progress_percent: Optional[float],
    filament_used: Optional[float],
    filament_total: Optional[float],
    slicer_estimated_time: Optional[int],
) -> Optional[int]:
    """Calculate remaining print time using Mainsail's algorithm.

    Mainsail uses a combination of methods (configurable by user):
    - File-based: elapsed / (progress / 100) - elapsed
    - Filament-based: elapsed / (filament_used / filament_total) - elapsed
    - Slicer-based: slicer_estimated_time - elapsed

    Default config in Mainsail: calcEstimateTime: ['file', 'filament']
    We implement the same default: average of file-based and filament-based.

    Reference: Mainsail src/store/printer/getters.ts
    - getEstimatedTimeFile
    - getEstimatedTimeFilament
    - getEstimatedTimeSlicer
    - getEstimatedTimeAvg
    """
    if elapsed_seconds is None or elapsed_seconds <= 0:
        return None

    estimates: list[int] = []

    # File-based estimation (Mainsail getEstimatedTimeFile)
    if progress_percent is not None and progress_percent > 0:
        # progress_percent is 0-100 scale, convert to 0-1
        progress_ratio = progress_percent / 100.0
        if progress_ratio > 0:
            total_estimated = elapsed_seconds / progress_ratio
            file_remaining = int(total_estimated - elapsed_seconds)
            if file_remaining > 0:
                estimates.append(file_remaining)

    # Filament-based estimation (Mainsail getEstimatedTimeFilament)
    if (
        filament_used is not None
        and filament_total is not None
        and filament_used > 0
        and filament_total > 0
        and filament_total > filament_used
    ):
        filament_ratio = filament_used / filament_total
        if filament_ratio > 0:
            total_estimated = elapsed_seconds / filament_ratio
            filament_remaining = int(total_estimated - elapsed_seconds)
            if filament_remaining > 0:
                estimates.append(filament_remaining)

    # Slicer-based estimation (Mainsail getEstimatedTimeSlicer) - not used by default
    # but available if no other estimates work
    slicer_remaining: Optional[int] = None
    if slicer_estimated_time is not None and slicer_estimated_time > 0:
        slicer_remaining = max(0, slicer_estimated_time - elapsed_seconds)

    # Mainsail default: average of file + filament
    if estimates:
        return int(sum(estimates) / len(estimates))

    # Fallback to slicer estimate if no other method works
    if slicer_remaining is not None and slicer_remaining > 0:
        return slicer_remaining

    return None


def _to_float(value: Any) -> Optional[float]:
    """Convert value to float, alias for _as_float for compatibility."""
    return _as_float(value)


def _extract_progress_percent(
    virtual_sdcard: Mapping[str, Any],
    filename: Optional[str],
    gcode_start_byte: Optional[int],
    gcode_end_byte: Optional[int],
) -> Optional[float]:
    """Extract progress percentage from Moonraker state.

    Strategy aligned with Mainsail's 'file-relative' mode (default):
    - Primary: Calculate from file_position relative to gcode byte range
    - Fallback: virtual_sdcard.progress (when metadata unavailable)

    The file-relative method is more accurate because:
    - It excludes slicer headers/metadata from progress calculation
    - It matches what users see in Mainsail by default
    - virtual_sdcard.progress includes header bytes which can cause mismatch

    Reference: Mainsail src/store/printer/getters.ts getPrintPercentByFilepositionRelative
    """
    file_position = _as_int(virtual_sdcard.get("file_position"))

    # Try file-relative calculation if we have all required data
    if (
        file_position is not None
        and filename is not None
        and gcode_start_byte is not None
        and gcode_end_byte is not None
        and gcode_end_byte > gcode_start_byte
    ):
        # Before gcode starts
        if file_position <= gcode_start_byte:
            return 0.0
        # After gcode ends
        if file_position >= gcode_end_byte:
            return 100.0

        # Calculate relative progress within gcode range
        current_position = file_position - gcode_start_byte
        max_position = gcode_end_byte - gcode_start_byte
        if current_position > 0 and max_position > 0:
            progress = (current_position / max_position) * 100.0
            return max(0.0, min(progress, 100.0))

    # Fallback to virtual_sdcard.progress when file-relative not possible
    sd_progress = _as_float(virtual_sdcard.get("progress"))
    if sd_progress is not None:
        return max(0.0, min(sd_progress * 100.0, 100.0))

    return None


def _extract_layers(print_stats: Mapping[str, Any]) -> tuple[Optional[int], Optional[int]]:
    """Extract layer information from print_stats.

    Moonraker stores layer info in print_stats.info as:
    - current_layer: Current layer number (1-based)
    - total_layer: Total number of layers

    Falls back to legacy layer/layers structure for compatibility.
    """
    # Primary source: print_stats.info (Klipper native)
    info = print_stats.get("info")
    if isinstance(info, Mapping):
        current = _as_int(info.get("current_layer"))
        total = _as_int(info.get("total_layer"))
        if current is not None or total is not None:
            return current, total

    # Fallback: legacy layer/layers structure
    layers = print_stats.get("layer") or print_stats.get("layers")
    if isinstance(layers, Mapping):
        current = _as_int(layers.get("current"))
        total = _as_int(layers.get("total"))
        return current, total

    return None, None


def _normalise_state(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    return trimmed.lower() if trimmed else None


def _as_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    return None


def _as_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return None
    return None


def _as_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return None
    return None
