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

    # State signals
    raw_state: Optional[str]  # print_stats.state
    idle_timeout_state: Optional[str]
    job_status: Optional[str]  # from history_event (takes precedence for terminal states)
    has_active_job: bool

    # Progress (only meaningful when has_active_job=True)
    progress_percent: Optional[float]
    elapsed_seconds: Optional[int]
    remaining_seconds: Optional[int]
    layer_current: Optional[int]
    layer_total: Optional[int]

    # Special states
    timelapse_paused: bool
    message: Optional[str]

    # Cloud-assigned thumbnail URL (set via job:set-thumbnail-url command)
    thumbnail_url: Optional[str] = None


class PrintSessionTracker:
    """Derives stable session identifiers and job metadata.

    Design principles (aligned with Obico/Mainsail):
    - Session ID is cleared when job ends (not latched)
    - Active job detection is simple: print_stats.state in {printing, paused}
    - Terminal states from history_event take precedence
    - Progress calculation uses file-relative method (like Mainsail's default)
    """

    def __init__(self) -> None:
        self._current_session_id: Optional[str] = None
        # Cloud-assigned thumbnail URL for the current print job
        self._current_thumbnail_url: Optional[str] = None
        # File metadata for accurate progress calculation (Mainsail file-relative method)
        self._gcode_start_byte: Optional[int] = None
        self._gcode_end_byte: Optional[int] = None
        self._current_filename: Optional[str] = None

    def set_file_metadata(
        self,
        filename: str,
        gcode_start_byte: Optional[int],
        gcode_end_byte: Optional[int],
    ) -> None:
        """Set file metadata for accurate progress calculation.

        Called when a new print job starts. These values are used to calculate
        progress using the file-relative method (excluding slicer headers).
        """
        self._current_filename = filename
        self._gcode_start_byte = gcode_start_byte
        self._gcode_end_byte = gcode_end_byte
        if gcode_start_byte is not None and gcode_end_byte is not None:
            LOGGER.debug(
                "File metadata set: %s (gcode: %d-%d bytes)",
                filename,
                gcode_start_byte,
                gcode_end_byte,
            )
        else:
            LOGGER.debug("File metadata set: %s (no byte range)", filename)

    def clear_file_metadata(self) -> None:
        """Clear file metadata when job ends."""
        self._current_filename = None
        self._gcode_start_byte = None
        self._gcode_end_byte = None

    def set_thumbnail_url(self, url: Optional[str]) -> None:
        """Set the thumbnail URL for the current print job.

        Called by CommandProcessor when job:set-thumbnail-url command is received.
        The URL is cleared when the job ends (session_id becomes None).
        """
        self._current_thumbnail_url = url
        if url:
            LOGGER.debug("Thumbnail URL set: %s", url)

    def clear_thumbnail_url(self) -> None:
        """Clear the thumbnail URL (e.g., when job ends)."""
        self._current_thumbnail_url = None

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
        remaining_seconds = _as_int(print_stats.get("print_duration_remaining"))
        layer_current, layer_total = _extract_layers(print_stats)

        message = _extract_message(display_status, print_stats)
        message = _normalise_message(
            message,
            raw_state=raw_state,
            idle_state=idle_state,
            job_status=job_status,
            has_active_job=has_active_job,
        )

        # Clear thumbnail URL and file metadata when job ends
        thumbnail_url = self._current_thumbnail_url if has_active_job else None
        if not has_active_job:
            if self._current_thumbnail_url:
                self._current_thumbnail_url = None
                LOGGER.debug("Cleared thumbnail URL (job ended)")
            if self._current_filename:
                self.clear_file_metadata()
                LOGGER.debug("Cleared file metadata (job ended)")

        return SessionInfo(
            session_id=session_id,
            job_name=filename,
            raw_state=raw_state,
            idle_timeout_state=idle_state,
            job_status=job_status,
            has_active_job=has_active_job,
            progress_percent=progress_percent,
            elapsed_seconds=elapsed_seconds,
            remaining_seconds=remaining_seconds,
            layer_current=layer_current,
            layer_total=layer_total,
            timelapse_paused=timelapse_paused,
            message=message,
            thumbnail_url=thumbnail_url,
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

        Simplified logic aligned with Obico's has_active_job():
        - Terminal states from history_event (job_status) take precedence
        - Active states: printing, paused (from print_stats.state)

        This replaces the previous 7-signal approach with a clean 2-signal check.
        """
        raw = (raw_state or "").lower()
        job = (job_status or "").lower()

        # Terminal states from history event take precedence
        terminal_states = {"complete", "completed", "cancelled", "canceled", "error"}
        if job in terminal_states:
            return False

        # Active states from print_stats (aligned with Obico's ACTIVE_STATES)
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
