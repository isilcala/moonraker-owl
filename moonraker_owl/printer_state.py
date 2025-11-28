"""Unified printer state resolution aligned with Mainsail/Obico semantics.

This module provides a single, stateless state resolver following the proven
patterns used by Mainsail and moonraker-obico. State determination is based on:

1. print_stats.state (primary source)
2. idle_timeout.state (fallback for idle detection)
3. timelapse pause detection (Mainsail behavior)

References:
- Mainsail: src/store/printer/getters.ts (printer_state getter)
- moonraker-obico: moonraker_obico/printer.py (PrinterState.get_state_from_status)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

__all__ = [
    "PrinterContext",
    "PrinterState",
    "PrinterStateResolver",
    "resolve_printer_state",
]


@dataclass(slots=True, frozen=True)
class PrinterContext:
    """Minimal context for state resolution - matches Mainsail inputs.

    Note: has_active_job is NOT used for state determination (Mainsail alignment).
    It exists only for backward compatibility with UI code that displays job status.
    """

    observed_at: datetime
    idle_state: Optional[str] = None
    timelapse_paused: bool = False
    # UI-only field, not used in state determination
    has_active_job: bool = False


class PrinterState:
    """Canonical printer states aligned with Mainsail display."""

    PRINTING = "Printing"
    PAUSED = "Paused"
    IDLE = "Idle"
    CANCELLED = "Cancelled"
    COMPLETED = "Completed"
    ERROR = "Error"
    OFFLINE = "Offline"
    HEATING = "Heating"


# State mappings following Mainsail/Obico patterns
_PRINT_STATS_STATE_MAP = {
    # Active states
    "printing": PrinterState.PRINTING,
    "resuming": PrinterState.PRINTING,
    "paused": PrinterState.PAUSED,
    "pausing": PrinterState.PAUSED,
    "cancelling": "Cancelling",  # Mainsail shows raw state
    "canceling": "Cancelling",
    # Terminal states
    "cancelled": PrinterState.CANCELLED,
    "canceled": PrinterState.CANCELLED,
    "complete": PrinterState.COMPLETED,
    "completed": PrinterState.COMPLETED,
    "error": PrinterState.ERROR,
    # Idle states
    "standby": PrinterState.IDLE,
    "ready": PrinterState.IDLE,
    "idle": PrinterState.IDLE,
    # Offline
    "shutdown": PrinterState.OFFLINE,
    "offline": PrinterState.OFFLINE,
}


def resolve_printer_state(
    raw_state: Optional[str],
    context: PrinterContext,
) -> str:
    """Resolve printer state from Moonraker data, aligned with Mainsail.

    This follows Mainsail's exact logic:
        print_stats.state ?? idle_timeout.state ?? "Idle"

    This is a stateless function - no latches, no TTLs, no side effects.
    The state is determined purely from the current data snapshot.

    Args:
        raw_state: The print_stats.state value from Moonraker
        context: Additional context for state resolution

    Returns:
        A canonical state string (e.g., "Printing", "Paused", "Idle")
    """
    normalized = (raw_state or "").strip().lower()

    # 1. Check print_stats.state first (primary source)
    if normalized in _PRINT_STATS_STATE_MAP:
        state = _PRINT_STATS_STATE_MAP[normalized]

        # Mainsail behavior: timelapse pause shows as "Printing" not "Paused"
        if state == PrinterState.PAUSED and context.timelapse_paused:
            return PrinterState.PRINTING

        return state

    # 2. Fallback to idle_timeout.state (like Mainsail's ?? operator)
    idle_normalized = (context.idle_state or "").strip().lower()
    if idle_normalized in _PRINT_STATS_STATE_MAP:
        return _PRINT_STATS_STATE_MAP[idle_normalized]

    # 3. Default to Idle (like Mainsail when no state available)
    return PrinterState.IDLE


class PrinterStateResolver:
    """Stateless wrapper for backward compatibility.

    New code should use resolve_printer_state() directly.
    """

    def __init__(self, *, terminal_ttl=None) -> None:
        # terminal_ttl is ignored - we no longer use latches
        pass

    def resolve(self, raw_state: Optional[str], context: PrinterContext) -> str:
        return resolve_printer_state(raw_state, context)
