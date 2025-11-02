"""Printer state engine for deterministic lifecycle transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


_CANONICAL_STATES = {
    "printing": "Printing",
    "paused": "Paused",
    "pausing": "Pausing",
    "resuming": "Resuming",
    "cancelling": "Cancelling",
    "cancelled": "Cancelled",
    "canceled": "Cancelled",
    "complete": "Completed",
    "completed": "Completed",
    "error": "Error",
    "shutdown": "Offline",
    "startup": "Heating",
    "standby": "Idle",
    "ready": "Idle",
    "idle": "Idle",
    "offline": "Offline",
}


@dataclass
class PrinterContext:
    observed_at: datetime
    has_active_job: bool
    is_heating: bool
    idle_state: Optional[str] = None
    timelapse_paused: bool = False
    progress_percent: Optional[float] = None
    progress_trend: Optional[str] = None
    job_status: Optional[str] = None


class PrinterStateEngine:
    """Maps Moonraker print states onto the Owl canonical lifecycle."""

    def resolve(self, raw_state: Optional[str], context: PrinterContext) -> str:
        raw_lower = raw_state.lower() if isinstance(raw_state, str) else None
        normalized = _CANONICAL_STATES.get(raw_lower) if raw_lower else None

        idle_hint = None
        if context.idle_state:
            idle_hint = _CANONICAL_STATES.get(context.idle_state.lower())

        job_hint = None
        if context.job_status:
            job_hint = _CANONICAL_STATES.get(context.job_status.lower())

        if context.timelapse_paused and (context.has_active_job or idle_hint == "Printing"):
            return "Paused"

        if normalized is None or normalized == "Unknown":
            normalized = idle_hint or job_hint or self._resolve_from_context(context)

        if normalized in {"Idle", "Heating"}:
            if context.is_heating:
                return "Heating"
            if context.has_active_job or idle_hint == "Printing" or (context.progress_trend == "increasing"):
                return "Printing"
            if idle_hint in {"Idle", "Heating"}:
                return idle_hint
            return normalized

        inactive_states = {"Cancelled", "Completed", "Error"}
        if (
            normalized not in {"Printing", "Paused", "Resuming", "Pausing", "Cancelling"}
            and job_hint in inactive_states
            and not context.has_active_job
        ):
            return job_hint

        if normalized == "Cancelled":
            if context.is_heating:
                return "Heating"
            return "Cancelled"

        if normalized in {"Completed", "Error"}:
            if context.has_active_job or context.progress_trend == "increasing":
                if context.is_heating:
                    return "Heating"
                return "Printing"
            if job_hint in {"Completed", "Cancelled", "Error"}:
                return job_hint
            return normalized

        if normalized == "Resuming":
            return "Printing"

        if normalized == "Pausing" and context.timelapse_paused:
            return "Paused"

        return normalized

    @staticmethod
    def _resolve_from_context(context: PrinterContext) -> str:
        if context.has_active_job and context.is_heating:
            return "Heating"
        if context.has_active_job:
            return "Printing"
        if context.is_heating:
            return "Heating"
        return "Idle"
