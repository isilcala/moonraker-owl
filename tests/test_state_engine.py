"""Tests for the unified printer state resolver aligned with Mainsail/Obico."""

from datetime import datetime, timezone

import pytest

from moonraker_owl.printer_state import (
    PrinterContext,
    PrinterState,
    resolve_printer_state,
)
from moonraker_owl.telemetry.state_engine import PrinterStateEngine


def _ctx(**overrides):
    """Create a PrinterContext with sensible defaults."""
    base = dict(
        observed_at=datetime.now(tz=timezone.utc),
        idle_state=None,
        timelapse_paused=False,
        has_active_job=False,  # UI-only field
    )
    base.update(overrides)
    return PrinterContext(**base)


# ============================================================================
# Mainsail-aligned state resolution tests (parametrized)
# ============================================================================


@pytest.mark.parametrize(
    "raw_state,expected",
    [
        # Active states
        ("printing", PrinterState.PRINTING),
        ("Printing", PrinterState.PRINTING),  # Case insensitive
        ("resuming", PrinterState.PRINTING),
        ("paused", PrinterState.PAUSED),
        ("pausing", PrinterState.PAUSED),
        ("cancelling", "Cancelling"),  # Mainsail shows raw state
        ("canceling", "Cancelling"),
        # Terminal states
        ("cancelled", PrinterState.CANCELLED),
        ("canceled", PrinterState.CANCELLED),
        ("complete", PrinterState.COMPLETED),
        ("completed", PrinterState.COMPLETED),
        ("error", PrinterState.ERROR),
        # Idle states
        ("standby", PrinterState.IDLE),
        ("ready", PrinterState.IDLE),
        ("idle", PrinterState.IDLE),
        # Offline
        ("shutdown", PrinterState.OFFLINE),
        ("offline", PrinterState.OFFLINE),
    ],
)
def test_print_stats_state_mapping(raw_state: str, expected: str) -> None:
    """Verify print_stats.state maps correctly to canonical states."""
    context = _ctx()
    result = resolve_printer_state(raw_state, context)
    assert result == expected


def test_timelapse_paused_shows_printing() -> None:
    """Mainsail behavior: timelapse pause shows as Printing, not Paused."""
    context = _ctx(has_active_job=True, timelapse_paused=True)
    result = resolve_printer_state("paused", context)
    assert result == PrinterState.PRINTING


def test_idle_timeout_fallback() -> None:
    """When print_stats.state is empty, use idle_timeout.state."""
    context = _ctx(idle_state="printing")
    result = resolve_printer_state("", context)
    assert result == PrinterState.PRINTING


def test_no_state_defaults_to_idle() -> None:
    """No state info defaults to Idle."""
    context = _ctx()
    result = resolve_printer_state("", context)
    assert result == PrinterState.IDLE


def test_none_state_defaults_to_idle() -> None:
    """None state defaults to Idle."""
    context = _ctx()
    result = resolve_printer_state(None, context)
    assert result == PrinterState.IDLE


# ============================================================================
# Legacy PrinterStateEngine compatibility tests
# ============================================================================


def test_engine_wrapper_cancelled() -> None:
    """PrinterStateEngine wrapper resolves cancelled correctly."""
    engine = PrinterStateEngine()
    context = _ctx()
    result = engine.resolve("cancelled", context)
    assert result == PrinterState.CANCELLED


def test_engine_wrapper_printing() -> None:
    """PrinterStateEngine wrapper resolves printing correctly."""
    engine = PrinterStateEngine()
    context = _ctx(has_active_job=True)
    result = engine.resolve("printing", context)
    assert result == PrinterState.PRINTING
