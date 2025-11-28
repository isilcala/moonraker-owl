"""Printer state engine - re-exports unified state resolver.

This module maintains backward compatibility with existing imports while
delegating all state resolution to the unified printer_state module.
"""

from __future__ import annotations

# Re-export from unified module for backward compatibility
from ..printer_state import (
    PrinterContext,
    PrinterState,
    PrinterStateResolver,
    resolve_printer_state,
)

__all__ = [
    "PrinterContext",
    "PrinterState",
    "PrinterStateEngine",
    "PrinterStateResolver",
    "resolve_printer_state",
]


class PrinterStateEngine:
    """Backward-compatible wrapper around the unified state resolver.

    This class is deprecated - use resolve_printer_state() directly.
    """

    def resolve(self, raw_state, context: PrinterContext) -> str:
        return resolve_printer_state(raw_state, context)
