"""Printer state engine for deterministic lifecycle transitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


_CANONICAL_STATES = {
    "printing": "Printing",
    "paused": "Paused",
    "cancelling": "Cancelling",
    "cancelled": "Cancelled",
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


class PrinterStateEngine:
    """Maps Moonraker print states onto the Owl canonical lifecycle."""

    def resolve(self, raw_state: Optional[str], context: PrinterContext) -> str:
        if not raw_state:
            return self._resolve_from_context(context)

        normalized = _CANONICAL_STATES.get(raw_state.lower())
        if normalized is None:
            return "Unknown"

        if normalized == "Idle" and context.is_heating:
            return "Heating"

        if normalized in {"Idle", "Heating"} and context.has_active_job:
            return "Heating" if context.is_heating else "Printing"

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
