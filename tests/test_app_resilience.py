"""Tests for MoonrakerOwlApp resilience helpers."""

from __future__ import annotations

import asyncio
import contextlib
import types
from configparser import ConfigParser
from pathlib import Path
from typing import Any, Optional

import pytest

from moonraker_owl.app import AgentState, MoonrakerOwlApp
from moonraker_owl.config import (
    CloudConfig,
    CommandConfig,
    LoggingConfig,
    MoonrakerConfig,
    OwlConfig,
    ResilienceConfig,
    TelemetryCadenceConfig,
    TelemetryConfig,
)


def _build_config(*, breaker_threshold: int = 2) -> OwlConfig:
    parser = ConfigParser()
    parser.add_section("cloud")
    parser.set("cloud", "device_id", "device-123")
    parser.set("cloud", "tenant_id", "tenant-99")
    parser.set("cloud", "printer_id", "printer-17")

    return OwlConfig(
        cloud=CloudConfig(
            base_url="https://api.owl.dev",
            broker_host="broker.owl.dev",
            broker_port=1883,
            username="tenant-99:device-123",
            password="token",
        ),
        moonraker=MoonrakerConfig(),
        telemetry=TelemetryConfig(),
        telemetry_cadence=TelemetryCadenceConfig(),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        resilience=ResilienceConfig(moonraker_breaker_threshold=breaker_threshold),
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )


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
    config = _build_config(breaker_threshold=2)
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
    config = _build_config(breaker_threshold=1)
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

    presence_calls: list[tuple[str, bool, Optional[str]]] = []

    def _fake_queue(
        self: MoonrakerOwlApp,
        state: str,
        *,
        retain: bool = True,
        detail: Optional[str] = None,
    ) -> None:
        presence_calls.append((state, retain, detail))

    app._queue_presence_publish = types.MethodType(_fake_queue, app)

    async def _fake_restart(self: MoonrakerOwlApp) -> bool:
        self._telemetry_ready = True
        self._commands_ready = True
        return True

    app._restart_components = types.MethodType(_fake_restart, app)

    await app._register_moonraker_recovery()

    assert app._moonraker_failures == 0
    assert app._moonraker_breaker_tripped is False
    assert app._state == AgentState.ACTIVE
    assert presence_calls == [("online", True, "agent active")]
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


def test_moonraker_assessment_detects_shutdown_state() -> None:
    app = MoonrakerOwlApp(_build_config())
    snapshot = _build_snapshot(
        webhooks_state="shutdown",
        print_message="Emergency stop",
    )

    assessment = app._analyse_moonraker_snapshot(snapshot)

    assert assessment.healthy is False
    assert assessment.force_trip is True
    assert assessment.detail == "Emergency stop"


@pytest.mark.asyncio
async def test_push_status_listener_trips_breaker_on_shutdown() -> None:
    config = _build_config(breaker_threshold=1)
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

    assert app._moonraker_breaker_tripped is True
    assert telemetry.system_status_calls == [("error", "Emergency stop")]
    assert commands.stop_calls == 1
    assert commands.abandon_reasons == ["moonraker unavailable"]
    assert telemetry.stop_calls == 0
    assert app._telemetry_ready is True
    assert app._commands_ready is False


def test_moonraker_assessment_reports_healthy_state() -> None:
    app = MoonrakerOwlApp(_build_config())
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
    app = MoonrakerOwlApp(_build_config())
    snapshot = _build_snapshot(
        webhooks_state="error",
        print_state="standby",
    )

    assessment = app._analyse_moonraker_snapshot(snapshot)

    assert assessment.healthy is True
    assert assessment.force_trip is False
    assert assessment.detail is None


def test_moonraker_assessment_detects_print_stats_error() -> None:
    app = MoonrakerOwlApp(_build_config())
    snapshot = _build_snapshot(
        print_state="error",
        print_message="Emergency stop",
    )

    assessment = app._analyse_moonraker_snapshot(snapshot)

    assert assessment.healthy is False
    assert assessment.force_trip is True
    assert assessment.detail == "Emergency stop"


@pytest.mark.asyncio
async def test_moonraker_failure_force_trip_bypasses_threshold() -> None:
    config = _build_config(breaker_threshold=5)
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