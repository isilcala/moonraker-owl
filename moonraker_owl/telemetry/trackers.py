"""Domain trackers for the telemetry pipeline."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Optional

from .state_store import MoonrakerStateStore, SectionSnapshot


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


class PrintSessionTracker:
    """Derives stable session identifiers and job metadata."""

    def __init__(self) -> None:
        self._current_session_id: Optional[str] = None
        self._last_filename: Optional[str] = None
        self._last_history_id: Optional[str] = None
        self._last_job_id: Optional[str] = None

    def compute(self, store: MoonrakerStateStore) -> SessionInfo:
        print_stats = _coerce_section(store.get("print_stats"))
        virtual_sdcard = _coerce_section(store.get("virtual_sdcard"))
        display_status = _coerce_section(store.get("display_status"))

        history_id = _extract_history_id(print_stats)
        filename = _extract_filename(print_stats) or _extract_filename(virtual_sdcard)
        job_id = _extract_job_id(print_stats)
        message = _extract_message(display_status, print_stats)

        session_id = self._resolve_session_id(history_id, filename, job_id)
        progress_percent = _extract_progress_percent(virtual_sdcard)
        elapsed_seconds = _as_int(print_stats.get("print_duration"))
        remaining_seconds = _as_int(print_stats.get("print_duration_remaining"))
        layer_current, layer_total = _extract_layers(print_stats)

        progress_value = _as_float(progress_percent)
        has_active_job = bool(
            session_id
            and (
                print_stats.get("state") not in {"standby", "idle", "complete"}
                or (progress_value is not None and progress_value > 0.0)
            )
        )

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
