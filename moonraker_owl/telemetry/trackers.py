"""Domain trackers for the telemetry pipeline."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Optional, Tuple

from .state_store import MoonrakerStateStore, SectionSnapshot

LOGGER = logging.getLogger(__name__)

@dataclass
class SessionInfo:
    session_id: str
    job_id: Optional[str]
    job_name: Optional[str]
    source_path: Optional[str]
    message: Optional[str]
    progress_percent: Optional[float]
    elapsed_seconds: Optional[int]
    remaining_seconds: Optional[int]
    layer_current: Optional[int]
    layer_total: Optional[int]
    has_active_job: bool
    raw_state: Optional[str]
    idle_timeout_state: Optional[str]
    timelapse_paused: bool
    progress_trend: str
    job_status: Optional[str]


class PrintSessionTracker:
    """Derives stable session identifiers and job metadata."""

    def __init__(self) -> None:
        self._current_session_id: Optional[str] = None
        self._last_filename: Optional[str] = None
        self._last_history_id: Optional[str] = None
        self._last_job_id: Optional[str] = None
        self._last_debug_signature: Optional[tuple[Any, ...]] = None
        self._last_progress_sample: Optional[Tuple[float, datetime]] = None

    def compute(self, store: MoonrakerStateStore) -> SessionInfo:
        print_stats = _coerce_section(store.get("print_stats"))
        virtual_sdcard = _coerce_section(store.get("virtual_sdcard"))
        display_status = _coerce_section(store.get("display_status"))
        idle_timeout = _coerce_section(store.get("idle_timeout"))
        timelapse_macro = _coerce_section(
            store.get("gcode_macro TIMELAPSE_TAKE_FRAME")
        )
        history_event = _coerce_section(store.get("history_event"))

        observed_at = store.latest_observed_at()

        history_id = _extract_history_id(print_stats)
        filename = _extract_filename(print_stats) or _extract_filename(virtual_sdcard)
        job_id = _extract_job_id(print_stats)
        message = _extract_message(display_status, print_stats)

        raw_state = _normalise_state(print_stats.get("state"))
        idle_state = _normalise_state(idle_timeout.get("state"))
        job_status = _normalise_state(history_event.get("status"))
        timelapse_paused = bool(_as_bool(timelapse_macro.get("is_paused")))

        session_id = self._resolve_session_id(history_id, filename, job_id)
        progress_percent = _extract_progress_percent(virtual_sdcard)
        elapsed_seconds = _as_int(print_stats.get("print_duration"))
        remaining_seconds = _as_int(print_stats.get("print_duration_remaining"))
        layer_current, layer_total = _extract_layers(print_stats)

        progress_value = _as_float(progress_percent)
        progress_trend = self._derive_progress_trend(progress_value, observed_at)

        has_active_job = self._is_active_job(
            session_id=session_id,
            raw_state=raw_state,
            idle_state=idle_state,
            job_status=job_status,
            timelapse_paused=timelapse_paused,
            progress_value=progress_value,
            progress_trend=progress_trend,
            elapsed_seconds=elapsed_seconds,
        )

        if LOGGER.isEnabledFor(logging.DEBUG):
            signature = (
                session_id,
                raw_state,
                history_id,
                job_id,
                filename,
                progress_percent,
                has_active_job,
                message,
                idle_state,
                job_status,
                timelapse_paused,
                progress_trend,
            )
            if signature != self._last_debug_signature:
                LOGGER.debug(
                    "Session resolved: session=%s raw_state=%s history_id=%s job_id=%s filename=%s progress=%s%% has_active_job=%s message=%s idle_state=%s job_status=%s timelapse_paused=%s progress_trend=%s",
                    session_id,
                    raw_state,
                    history_id,
                    job_id,
                    filename,
                    progress_percent,
                    has_active_job,
                    message,
                    idle_state,
                    job_status,
                    timelapse_paused,
                    progress_trend,
                )
                if session_id == "offline":
                    LOGGER.debug(
                        "Session identifiers missing (offline fallback): history_id=%s job_id=%s filename=%s",
                        history_id,
                        job_id,
                        filename,
                    )
                self._last_debug_signature = signature

        return SessionInfo(
            session_id=session_id,
            job_id=job_id or history_id,
            job_name=filename,
            source_path=filename,
            message=message,
            progress_percent=progress_percent,
            elapsed_seconds=elapsed_seconds,
            remaining_seconds=remaining_seconds,
            layer_current=layer_current,
            layer_total=layer_total,
            has_active_job=has_active_job,
            raw_state=raw_state,
            idle_timeout_state=idle_state,
            timelapse_paused=timelapse_paused,
            progress_trend=progress_trend,
            job_status=job_status,
        )

    def _resolve_session_id(
        self,
        history_id: Optional[str],
        filename: Optional[str],
        job_id: Optional[str],
    ) -> str:
        if history_id:
            self._current_session_id = f"history-{history_id}"
        elif job_id:
            self._current_session_id = f"job-{job_id}"
        elif filename:
            digest = hashlib.sha1(filename.encode("utf-8"), usedforsecurity=False)
            self._current_session_id = digest.hexdigest()
        elif not self._current_session_id:
            self._current_session_id = "offline"

        self._last_history_id = history_id or self._last_history_id
        self._last_filename = filename or self._last_filename
        self._last_job_id = job_id or self._last_job_id

        return self._current_session_id or "offline"

    def _derive_progress_trend(
        self,
        progress_value: Optional[float],
        observed_at: Optional[datetime],
    ) -> str:
        if progress_value is None or observed_at is None:
            return "unknown"

        last_sample = self._last_progress_sample
        if last_sample is not None:
            last_value, last_time = last_sample
            delta_value = progress_value - last_value
            delta_seconds = max((observed_at - last_time).total_seconds(), 0)

            if delta_value > 0.05:
                trend = "increasing"
            elif delta_value < -0.05:
                trend = "decreasing"
            elif delta_seconds >= 5:
                trend = "steady"
            else:
                trend = "unknown"
        else:
            trend = "increasing" if progress_value > 0.0 else "steady"

        self._last_progress_sample = (progress_value, observed_at)
        return trend

    def _is_active_job(
        self,
        *,
        session_id: Optional[str],
        raw_state: Optional[str],
        idle_state: Optional[str],
        job_status: Optional[str],
        timelapse_paused: bool,
        progress_value: Optional[float],
        progress_trend: str,
        elapsed_seconds: Optional[int],
    ) -> bool:
        has_session = bool(session_id) and session_id != "offline"

        raw_state = raw_state or "unknown"
        idle_state = idle_state or "unknown"
        job_status = job_status or ""

        progress_value = progress_value or 0.0
        elapsed_seconds = elapsed_seconds or 0

        progress_active = progress_value > 0.0
        progress_increasing = progress_trend == "increasing"
        elapsed_active = elapsed_seconds > 0
        idle_printing = idle_state in {"printing", "busy"}

        job_terminal_states = {"completed", "complete", "cancelled", "canceled", "error", "failed", "aborted"}
        raw_terminal_states = {"complete", "completed", "cancelled", "canceled", "error"}

        active_signals = any(
            (
                raw_state in {"printing", "paused", "resuming", "cancelling", "pausing"},
                timelapse_paused,
                progress_increasing,
                (progress_active and job_status not in job_terminal_states),
                (elapsed_active and job_status not in job_terminal_states),
                idle_printing,
            )
        )

        if active_signals:
            return True

        if raw_state in raw_terminal_states:
            if job_status in job_terminal_states or (
                progress_value >= 99.9 and progress_trend != "increasing"
            ):
                return False
            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug(
                    "Active job gating: raw_state=%s job_status=%s progress=%s%% trend=%s timelapse_paused=%s",
                    raw_state,
                    job_status or "",
                    progress_value,
                    progress_trend,
                    timelapse_paused,
                )
            return True

        if raw_state in {"standby", "idle", "ready"}:
            return False

        if not has_session:
            return progress_increasing or (
                progress_active and job_status not in job_terminal_states
            ) or elapsed_active

        return (progress_active and job_status not in job_terminal_states) or progress_increasing


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

    def is_heating_for_print(self) -> bool:
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
    for key in ("history_id", "historyId"):
        value = section.get(key)
        if value:
            return str(value)
    return None


def _extract_filename(section: Mapping[str, Any]) -> Optional[str]:
    for key in ("filename", "file_path", "filePath"):
        value = section.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _extract_job_id(section: Mapping[str, Any]) -> Optional[str]:
    for key in ("job_id", "jobId"):
        value = section.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _extract_message(
    display_status: Mapping[str, Any],
    print_stats: Mapping[str, Any],
) -> Optional[str]:
    for source in (print_stats, display_status):
        message = source.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
    return None


def _extract_progress_percent(section: Mapping[str, Any]) -> Optional[float]:
    progress = section.get("progress")
    if progress is None:
        return None
    progress_value = _as_float(progress)
    if progress_value is None:
        return None
    return max(0.0, min(progress_value * 100.0, 100.0))


def _extract_layers(section: Mapping[str, Any]) -> tuple[Optional[int], Optional[int]]:
    layers = section.get("layer") or section.get("layers")
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
