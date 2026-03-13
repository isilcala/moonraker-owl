"""Tests for MoonrakerOwlApp resilience helpers."""

from __future__ import annotations

import asyncio
import contextlib
import types
from typing import Any, Optional

import pytest

from moonraker_owl.app import AgentState, MoonrakerOwlApp, _ALLOWED_TRANSITIONS
from helpers import build_config


class _StubTelemetryPublisher:
    def __init__(self) -> None:
        self.stop_calls = 0
        self.system_status_calls: list[tuple[str, Optional[str]]] = []
        self.register_calls = 0
        self.unregister_calls = 0
        self.listeners: list[Any] = []

    async def stop(self) -> None:
        self.stop_calls += 1

    async def publish_system_status(
        self,
        *,
        printer_state: str,
        message: Optional[str] = None,
    ) -> None:
        self.system_status_calls.append((printer_state, message))

    def register_status_listener(self, listener: Any) -> None:
        self.register_calls += 1
        self.listeners.append(listener)

    def unregister_status_listener(self, listener: Any) -> None:
        self.unregister_calls += 1
        with contextlib.suppress(ValueError):
            self.listeners.remove(listener)


class _StubCommandProcessor:
    def __init__(self) -> None:
        self.stop_calls = 0
        self.abandon_reasons: list[str] = []
        self.pending_count = 0

    async def stop(self) -> None:
        self.stop_calls += 1

    async def abandon_inflight(self, reason: str) -> None:
        self.abandon_reasons.append(reason)


@pytest.mark.asyncio
async def test_moonraker_breaker_trips_after_failures() -> None:
    config = build_config(breaker_threshold=2)
    app = MoonrakerOwlApp(config)

    app._loop = asyncio.get_running_loop()
    app._state = AgentState.ACTIVE
    app._telemetry_ready = True
    app._commands_ready = True

    telemetry = _StubTelemetryPublisher()
    commands = _StubCommandProcessor()
    app._telemetry_publisher = telemetry
    app._command_processor = commands

    await app._register_moonraker_failure("rpc timeout")
    assert not app._moonraker_breaker_tripped
    assert commands.stop_calls == 0

    await app._register_moonraker_failure("rpc timeout")

    assert app._moonraker_breaker_tripped is True
    assert app._state == AgentState.DEGRADED
    assert commands.stop_calls == 1
    assert commands.abandon_reasons == ["moonraker unavailable"]
    assert telemetry.stop_calls == 0
    assert telemetry.system_status_calls == [("error", "rpc timeout")]
    assert app._telemetry_ready is True


@pytest.mark.asyncio
async def test_moonraker_recovery_restarts_components() -> None:
    config = build_config(breaker_threshold=1)
    app = MoonrakerOwlApp(config)

    app._loop = asyncio.get_running_loop()
    app._state = AgentState.DEGRADED
    app._telemetry_ready = False
    app._commands_ready = False
    app._moonraker_breaker_tripped = True
    app._moonraker_failures = 3

    telemetry = _StubTelemetryPublisher()
    commands = _StubCommandProcessor()
    app._telemetry_publisher = telemetry
    app._command_processor = commands

    async def _fake_restart(self: MoonrakerOwlApp) -> bool:
        self._telemetry_ready = True
        self._commands_ready = True
        return True

    app._restart_components = types.MethodType(_fake_restart, app)

    await app._register_moonraker_recovery()

    assert app._moonraker_failures == 0
    assert app._moonraker_breaker_tripped is False
    assert app._state == AgentState.ACTIVE
    assert commands.stop_calls == 0
    assert commands.abandon_reasons == []
    assert telemetry.stop_calls == 0


def _build_snapshot(
    *,
    webhooks_state: Optional[str] = None,
    printer_state: Optional[str] = None,
    printer_shutdown: Optional[bool] = None,
    print_state: Optional[str] = None,
    print_message: Optional[str] = None,
) -> dict:
    status: dict[str, dict[str, object]] = {}
    if webhooks_state is not None:
        status["webhooks"] = {"state": webhooks_state}
    if printer_state is not None or printer_shutdown is not None:
        node: dict[str, object] = {}
        if printer_state is not None:
            node["state"] = printer_state
        if printer_shutdown is not None:
            node["is_shutdown"] = printer_shutdown
        status["printer"] = node
    if print_state is not None or print_message is not None:
        node = {}
        if print_state is not None:
            node["state"] = print_state
        if print_message is not None:
            node["message"] = print_message
        status["print_stats"] = node

    return {"result": {"status": status}}


def test_moonraker_assessment_reports_healthy_on_klipper_shutdown() -> None:
    """Klipper shutdown is not a Moonraker connectivity failure."""
    app = MoonrakerOwlApp(build_config())
    snapshot = _build_snapshot(
        webhooks_state="shutdown",
        print_message="Emergency stop",
    )

    assessment = app._analyse_moonraker_snapshot(snapshot)

    assert assessment.healthy is True


@pytest.mark.asyncio
async def test_push_status_listener_keeps_commands_on_klipper_shutdown() -> None:
    """Klipper shutdown must NOT trip breaker — commands stay active for recovery."""
    config = build_config(breaker_threshold=1)
    app = MoonrakerOwlApp(config)

    app._loop = asyncio.get_running_loop()
    app._state = AgentState.ACTIVE
    app._telemetry_ready = True
    app._commands_ready = True

    telemetry = _StubTelemetryPublisher()
    commands = _StubCommandProcessor()
    app._telemetry_publisher = telemetry
    app._command_processor = commands

    snapshot = _build_snapshot(
        webhooks_state="shutdown",
        print_message="Emergency stop",
    )

    await app._handle_telemetry_status_update(snapshot)

    assert app._moonraker_breaker_tripped is False
    assert commands.stop_calls == 0
    assert commands.abandon_reasons == []
    assert app._commands_ready is True


def test_moonraker_assessment_reports_healthy_state() -> None:
    app = MoonrakerOwlApp(build_config())
    snapshot = _build_snapshot(
        webhooks_state="ready",
        printer_state="ready",
        print_state="standby",
    )

    assessment = app._analyse_moonraker_snapshot(snapshot)

    assert assessment.healthy is True
    assert assessment.force_trip is False
    assert assessment.detail is None


def test_moonraker_assessment_ignores_stale_webhooks_error() -> None:
    app = MoonrakerOwlApp(build_config())
    snapshot = _build_snapshot(
        webhooks_state="error",
        print_state="standby",
    )

    assessment = app._analyse_moonraker_snapshot(snapshot)

    assert assessment.healthy is True
    assert assessment.force_trip is False
    assert assessment.detail is None


def test_moonraker_assessment_reports_healthy_on_print_stats_error() -> None:
    """print_stats error is a Klipper state, not a Moonraker failure."""
    app = MoonrakerOwlApp(build_config())
    snapshot = _build_snapshot(
        print_state="error",
        print_message="Emergency stop",
    )

    assessment = app._analyse_moonraker_snapshot(snapshot)

    assert assessment.healthy is True


@pytest.mark.asyncio
async def test_moonraker_failure_force_trip_bypasses_threshold() -> None:
    config = build_config(breaker_threshold=5)
    app = MoonrakerOwlApp(config)
    app._loop = asyncio.get_running_loop()

    telemetry = _StubTelemetryPublisher()
    app._telemetry_publisher = telemetry
    app._telemetry_ready = True

    await app._register_moonraker_failure(
        "moonraker shutdown",
        force_trip=True,
    )

    assert app._moonraker_breaker_tripped is True
    assert telemetry.system_status_calls == [("error", "moonraker shutdown")]


@pytest.mark.asyncio
async def test_invalid_state_transition_is_rejected(caplog: pytest.LogCaptureFixture) -> None:
    """Test that invalid transitions are rejected and logged as errors."""
    config = build_config()
    app = MoonrakerOwlApp(config)
    app._loop = asyncio.get_running_loop()

    # Start in COLD_START (default)
    assert app._state == AgentState.COLD_START

    # COLD_START -> ACTIVE is not allowed (must go through AWAITING_*)
    await app._transition_state(AgentState.ACTIVE, detail="invalid")

    # State should remain COLD_START
    assert app._state == AgentState.COLD_START
    assert any(
        "Invalid state transition: cold_start -> active" in r.message
        for r in caplog.records
        if r.levelname == "ERROR"
    )


@pytest.mark.asyncio
async def test_stopping_is_terminal_state() -> None:
    """Test that no transitions are allowed from STOPPING."""
    config = build_config()
    app = MoonrakerOwlApp(config)
    app._loop = asyncio.get_running_loop()

    # Force into STOPPING
    app._state = AgentState.STOPPING

    # Try every state �?all should be rejected
    for target in AgentState:
        if target == AgentState.STOPPING:
            continue  # same-state is a no-op
        await app._transition_state(target)
        assert app._state == AgentState.STOPPING


def test_allowed_transitions_covers_all_states() -> None:
    """Every AgentState must appear as a key in _ALLOWED_TRANSITIONS."""
    for state in AgentState:
        assert state in _ALLOWED_TRANSITIONS, f"{state.value} missing from _ALLOWED_TRANSITIONS"
