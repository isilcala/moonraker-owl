"""Printer state resolution logic aligning with Mainsail/Obico semantics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

__all__ = [
    "PrinterContext",
    "PrinterStateResolver",
]


@dataclass(slots=True)
class PrinterContext:
    """Contextual signals used to resolve high level printer status."""

    observed_at: datetime
    has_active_job: bool
    is_heating: bool


class PrinterStateResolver:
    """Map raw Moonraker states to Owl printer status labels."""

    _TERMINAL_STATES = {
        "cancelled": "Cancelled",
        "canceled": "Cancelled",
        "completed": "Completed",
        "complete": "Completed",
        "error": "Error",
        "shutdown": "Error",
    }

    _ACTIVE_STATES = {
        "printing": "Printing",
        "resuming": "Printing",
        "paused": "Paused",
        "cancelling": "Cancelling",
        "canceling": "Cancelling",
    }

    _IDLE_STATES = {"standby", "ready", "idle", ""}

    def __init__(self, *, terminal_ttl: timedelta = timedelta(seconds=10)) -> None:
        self._terminal_ttl = terminal_ttl
        self._latched_state: Optional[tuple[str, datetime]] = None

    def resolve(self, raw_state: Optional[str], context: PrinterContext) -> str:
        normalized = (raw_state or "").strip().lower()

        if normalized in self._ACTIVE_STATES:
            self._clear_latch()
            status = self._ACTIVE_STATES[normalized]
            if status in {"Printing", "Cancelling"}:
                # Active extrusion clears any previous terminal latch.
                self._clear_latch()
            return status

        if normalized in self._TERMINAL_STATES:
            status = self._TERMINAL_STATES[normalized]
            self._latch(status, context.observed_at)
            return status

        if normalized == "offline":
            self._clear_latch()
            return "Offline"

        if normalized in self._IDLE_STATES:
            latched = self._current_latch(context.observed_at)
            if latched is not None:
                if context.has_active_job:
                    self._clear_latch()
                else:
                    return latched

            if context.is_heating:
                # Mainsail labels pre-print heating as printing to signal activity.
                return "Printing"

            if context.has_active_job:
                return "Printing"

            self._clear_latch()
            return "Idle"

        latched = self._current_latch(context.observed_at)
        if latched is not None:
            return latched

        if normalized:
            # Unknown but non-empty state -> surface raw value for diagnostics.
            if isinstance(raw_state, str):
                return raw_state.strip().title()
            return normalized.title()

        return "Unknown"

    # ------------------------------------------------------------------
    # latch management
    # ------------------------------------------------------------------
    def _latch(self, status: str, observed_at: datetime) -> None:
        expires_at = observed_at + self._terminal_ttl
        self._latched_state = (status, expires_at)

    def _clear_latch(self) -> None:
        self._latched_state = None

    def _current_latch(self, observed_at: datetime) -> Optional[str]:
        if self._latched_state is None:
            return None

        status, expires_at = self._latched_state
        if observed_at >= expires_at:
            self._latched_state = None
            return None
        return status


def utcnow() -> datetime:
    """Return timezone aware UTC timestamp."""

    return datetime.now(timezone.utc)
