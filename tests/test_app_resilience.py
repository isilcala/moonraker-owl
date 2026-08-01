"""Tests for MoonrakerOwlApp resilience helpers."""

from __future__ import annotations

import asyncio
import contextlib
import json
import types
from typing import Any, Optional

import pytest

from moonraker_owl.app import AgentState, MoonrakerOwlApp, _ALLOWED_TRANSITIONS
from moonraker_owl.connection import ReconnectReason
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


class _StubPrinterBackend:
    def __init__(self) -> None:
        self.stop_calls = 0

    async def stop(self) -> None:
        self.stop_calls += 1


class _StubTokenManager:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.started = False
        self.stopped = False
        self.renewal_started = False

    async def start(self) -> None:
        self.started = True

    def start_renewal_loop(self, on_renewed: Any = None) -> None:
        self.renewal_started = True

    def is_token_valid(self) -> bool:
        return True

    async def ensure_valid_token(self) -> None:
        return None

    async def stop(self) -> None:
        self.stopped = True

    def get_token(self) -> str:
        return "token"


class _StubCloudConfigManager:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.callbacks: list[Any] = []
        self.loaded = False

    def register_callback(self, callback: Any) -> None:
        self.callbacks.append(callback)

    def load_lkg(self) -> None:
        self.loaded = True

    async def fetch(self, *args: Any, **kwargs: Any) -> bool:
        return False

    async def stop(self) -> None:
        return None


class _StubMqttClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.disconnect_handlers: list[Any] = []
        self.connect_handlers: list[Any] = []
        self.last_will: dict[str, Any] | None = None

    def register_disconnect_handler(self, handler: Any) -> None:
        self.disconnect_handlers.append(handler)

    def register_connect_handler(self, handler: Any) -> None:
        self.connect_handlers.append(handler)

    def set_last_will(self, **kwargs: Any) -> None:
        self.last_will = kwargs


class _RecordingConnectionCoordinator:
    def __init__(self) -> None:
        self.requests: list[ReconnectReason] = []

    def request_reconnect(self, reason: ReconnectReason) -> None:
        self.requests.append(reason)


class _StubConnectionCoordinator:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.supervisor_starts = 0

    def register_disconnected_callback(self, callback: Any) -> None:
        return None

    def register_fatal_supervisor_failure_callback(self, callback: Any) -> None:
        return None

    def register_reconnected_callback(self, callback: Any) -> None:
        return None

    def register_disconnect_event_callback(self, callback: Any) -> None:
        return None

    def start_supervisor(self) -> None:
        self.supervisor_starts += 1


class _BlockingCloudConfigManager:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def fetch(self, *args: Any, **kwargs: Any) -> bool:
        self.started.set()
        await self.release.wait()
        return False


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
async def test_start_services_registers_contract_compliant_lwt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config()
    config.cloud.device_private_key = "test-private-key"
    backend = _StubPrinterBackend()
    app = MoonrakerOwlApp(config, printer_backend=backend)

    monkeypatch.setattr("moonraker_owl.app.TokenManager", _StubTokenManager)
    monkeypatch.setattr("moonraker_owl.app.CloudConfigManager", _StubCloudConfigManager)
    monkeypatch.setattr("moonraker_owl.app.MQTTClient", _StubMqttClient)

    async def _fake_start_metadata_reporter(self: MoonrakerOwlApp, device_id: str) -> None:
        return None

    async def _fake_connect_mqtt(self: MoonrakerOwlApp) -> bool:
        return False

    monkeypatch.setattr(MoonrakerOwlApp, "_start_metadata_reporter", _fake_start_metadata_reporter)
    monkeypatch.setattr(MoonrakerOwlApp, "_connect_mqtt", _fake_connect_mqtt)

    started = await app._start_services()

    assert started is False
    assert backend.stop_calls == 1
    assert isinstance(app._mqtt_client, _StubMqttClient)
    assert app._mqtt_client.last_will is not None
    assert app._device_id is not None

    last_will = app._mqtt_client.last_will
    assert last_will["topic"] == f"owl/printers/{app._device_id}/status"
    assert last_will["qos"] == 1
    assert last_will["retain"] is True

    document = json.loads(last_will["payload"].decode("utf-8"))
    assert document["$type"] == "telemetry.status"
    assert document["deviceId"] == app._device_id
    assert document["$seq"] == 0
    assert document["kind"] == "full"
    assert document["sessionId"] is None
    assert document["$ts"]
    assert document["payload"]["lastUpdated"] == document["$ts"]
    assert document["payload"]["lifecycle"]["phase"] == "Offline"
    assert document["payload"]["lifecycle"]["isShutdown"] is False
    assert document["payload"]["cadence"]["heartbeatSeconds"] == config.telemetry_cadence.status_heartbeat_seconds


@pytest.mark.asyncio
async def test_token_renewal_reconnect_is_not_blocked_by_config_fetch() -> None:
    app = MoonrakerOwlApp(build_config())
    coordinator = _RecordingConnectionCoordinator()
    cloud_config = _BlockingCloudConfigManager()
    app._connection_coordinator = coordinator  # type: ignore[assignment]
    app._cloud_config_manager = cloud_config  # type: ignore[assignment]

    await asyncio.wait_for(app._on_token_renewed(), timeout=0.1)

    assert coordinator.requests == [ReconnectReason.TOKEN_RENEWED]
    await asyncio.wait_for(cloud_config.started.wait(), timeout=0.1)

    cloud_config.release.set()
    pending = list(app._background_tasks)
    if pending:
        await asyncio.gather(*pending)


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


@pytest.mark.asyncio
async def test_spawn_background_tracks_and_clears_task() -> None:
    """A spawned background task is tracked until completion then discarded."""
    app = MoonrakerOwlApp(build_config())
    app._loop = asyncio.get_running_loop()

    started = asyncio.Event()
    release = asyncio.Event()

    async def _work() -> None:
        started.set()
        await release.wait()

    task = app._spawn_background(_work(), name="unit-test")
    await asyncio.wait_for(started.wait(), timeout=1.0)
    assert task in app._background_tasks

    release.set()
    await task
    # Done callback runs via the event loop; yield once so it can fire.
    await asyncio.sleep(0)
    assert task not in app._background_tasks


@pytest.mark.asyncio
async def test_spawn_background_logs_unhandled_exception(caplog) -> None:
    """A crashing background task surfaces its exception via the logger."""
    app = MoonrakerOwlApp(build_config())
    app._loop = asyncio.get_running_loop()

    async def _boom() -> None:
        raise RuntimeError("boom-in-background")

    with caplog.at_level("ERROR", logger="moonraker_owl.app"):
        task = app._spawn_background(_boom(), name="boomer")
        with contextlib.suppress(RuntimeError):
            await task
        await asyncio.sleep(0)

    assert any("boom-in-background" in record.getMessage() for record in caplog.records)
    assert task not in app._background_tasks


@pytest.mark.asyncio
async def test_schedule_state_transition_uses_tracked_task() -> None:
    """Scheduling a same-loop transition registers a tracked background task."""
    app = MoonrakerOwlApp(build_config())
    app._loop = asyncio.get_running_loop()
    app._state = AgentState.AWAITING_MOONRAKER

    app._schedule_state_transition(AgentState.ACTIVE, detail="unit-test")
    # The transition coroutine should have been registered as a tracked task.
    assert len(app._background_tasks) >= 1

    # Drain tracked tasks so the transition completes.
    await asyncio.gather(*list(app._background_tasks), return_exceptions=True)
    await asyncio.sleep(0)
    assert app._state == AgentState.ACTIVE


@pytest.mark.asyncio
async def test_connection_restored_retries_when_reconnect_happens_mid_restore() -> None:
    """A later reconnect must queue another restore instead of being dropped."""
    app = MoonrakerOwlApp(build_config())
    app._loop = asyncio.get_running_loop()
    app._state = AgentState.ACTIVE
    app._mqtt_ready = True
    app._component_restart_lock = asyncio.Lock()

    restore_started = asyncio.Event()
    release_first_restore = asyncio.Event()
    restore_runs: list[ReconnectReason | None] = []

    async def _fake_do_connection_restored(
        self: MoonrakerOwlApp,
        recovery_epoch: int,
        reason: ReconnectReason | None,
    ) -> None:
        restore_runs.append(reason)
        restore_started.set()
        if len(restore_runs) == 1:
            await release_first_restore.wait()

    app._do_connection_restored = types.MethodType(  # type: ignore[method-assign]
        _fake_do_connection_restored,
        app,
    )

    await app._on_connection_lost(ReconnectReason.CONNECTION_LOST)
    await app._on_connection_restored()
    await asyncio.wait_for(restore_started.wait(), timeout=0.2)

    await app._on_connection_lost(ReconnectReason.TOKEN_RENEWED)
    await app._on_connection_restored()

    await asyncio.sleep(0)
    release_first_restore.set()
    await asyncio.gather(*list(app._background_tasks), return_exceptions=True)

    assert restore_runs == [
        ReconnectReason.CONNECTION_LOST,
        ReconnectReason.TOKEN_RENEWED,
    ]


@pytest.mark.asyncio
async def test_stale_connection_restore_does_not_mark_agent_active() -> None:
    """A restore for an old reconnect epoch must not reactivate the agent."""
    app = MoonrakerOwlApp(build_config())
    app._loop = asyncio.get_running_loop()
    app._state = AgentState.RECOVERING
    app._mqtt_ready = False
    app._connection_recovery_epoch = 1

    async def _fake_restart_components(
        self: MoonrakerOwlApp,
        *,
        preserve_print_state: bool = False,
    ) -> bool:
        self._connection_recovery_epoch = 2
        self._mqtt_ready = False
        return True

    app._restart_components = types.MethodType(  # type: ignore[method-assign]
        _fake_restart_components,
        app,
    )

    await app._do_connection_restored(
        1,
        ReconnectReason.CONNECTION_LOST,
    )

    assert app._state == AgentState.RECOVERING
    assert app._mqtt_ready is False


@pytest.mark.asyncio
async def test_connection_restored_continues_to_new_epoch_after_restore_exception() -> None:
    """A crashing stale restore must not drop a newer queued restore request."""
    app = MoonrakerOwlApp(build_config())
    app._loop = asyncio.get_running_loop()
    app._state = AgentState.ACTIVE
    app._mqtt_ready = True
    app._component_restart_lock = asyncio.Lock()

    restore_started = asyncio.Event()
    release_first_restore = asyncio.Event()
    restore_runs: list[ReconnectReason | None] = []

    async def _fake_do_connection_restored(
        self: MoonrakerOwlApp,
        recovery_epoch: int,
        reason: ReconnectReason | None,
    ) -> None:
        restore_runs.append(reason)
        if len(restore_runs) == 1:
            restore_started.set()
            await release_first_restore.wait()
            raise RuntimeError("boom-first-restore")

    app._do_connection_restored = types.MethodType(  # type: ignore[method-assign]
        _fake_do_connection_restored,
        app,
    )

    await app._on_connection_lost(ReconnectReason.CONNECTION_LOST)
    await app._on_connection_restored()
    await asyncio.wait_for(restore_started.wait(), timeout=0.2)

    await app._on_connection_lost(ReconnectReason.TOKEN_RENEWED)
    await app._on_connection_restored()

    release_first_restore.set()
    await asyncio.gather(*list(app._background_tasks), return_exceptions=True)

    assert restore_runs == [
        ReconnectReason.CONNECTION_LOST,
        ReconnectReason.TOKEN_RENEWED,
    ]



class _FakePublishClient:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload, qos=1, retain=False, **kwargs):
        self.published.append(
            {"topic": topic, "payload": payload, "qos": qos, "retain": retain}
        )


def test_publish_graceful_offline_emits_retained_status() -> None:
    app = MoonrakerOwlApp(build_config())
    client = _FakePublishClient()
    app._mqtt_client = client
    app._device_id = "printer-graceful"

    app._publish_graceful_offline()

    assert len(client.published) == 1
    msg = client.published[0]
    assert msg["topic"].endswith("printer-graceful/status")
    assert msg["retain"] is True
    assert msg["qos"] == 1
    body = json.loads(msg["payload"].decode("utf-8"))
    assert body["payload"]["lifecycle"]["phase"] == "Offline"
    assert body["payload"]["lifecycle"]["reason"] == "Graceful shutdown"


def test_publish_graceful_offline_is_noop_without_client() -> None:
    app = MoonrakerOwlApp(build_config())
    app._mqtt_client = None
    app._device_id = "printer-x"
    # Must not raise.
    app._publish_graceful_offline()


@pytest.mark.asyncio
async def test_unrecoverable_connection_supervisor_failure_requests_shutdown() -> None:
    app = MoonrakerOwlApp(build_config())
    app._loop = asyncio.get_running_loop()
    app._shutdown_event = asyncio.Event()

    app._on_unrecoverable_connection_supervisor_failure(6)

    assert app._fatal_exit_code == 1
    assert app._stopping is True
    assert app._shutdown_event.is_set()

    await asyncio.gather(*list(app._background_tasks), return_exceptions=True)
    await asyncio.sleep(0)
    assert app._state == AgentState.STOPPING


def test_start_exits_nonzero_after_fatal_runtime(monkeypatch) -> None:
    async def _fatal_run(self: MoonrakerOwlApp) -> None:
        self._fatal_exit_code = 1

    monkeypatch.setattr(MoonrakerOwlApp, "run", _fatal_run)
    monkeypatch.setattr("moonraker_owl.app.configure_logging", lambda *args, **kwargs: None)

    with pytest.raises(SystemExit) as exc:
        MoonrakerOwlApp.start(build_config())

    assert exc.value.code == 1


@pytest.mark.asyncio
async def test_start_runtime_components_serializes_concurrent_restarts() -> None:
    """All runtime-restart paths must be serialized by a single lock.

    Regression for the staging telemetry-loss incident (2026-07-30): a
    boot-order race let the startup-retry loop and the Moonraker-recovery path
    call _start_runtime_components() concurrently, running telemetry.start() on
    the shared publisher in parallel and corrupting its worker + Moonraker
    subscription. Telemetry then went silent while the printer still showed
    online. The lock must let only one (re)start run at a time.
    """
    app = MoonrakerOwlApp(build_config())
    app._loop = asyncio.get_running_loop()

    active = 0
    max_active = 0

    async def _fake_locked(
        self: MoonrakerOwlApp, *, preserve_print_state: bool = False
    ) -> bool:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.02)
        active -= 1
        return True

    app._start_runtime_components_locked = types.MethodType(  # type: ignore[method-assign]
        _fake_locked, app
    )

    results = await asyncio.gather(
        app._start_runtime_components(),
        app._start_runtime_components(),
        app._start_runtime_components(),
    )

    assert results == [True, True, True]
    assert max_active == 1


@pytest.mark.asyncio
async def test_startup_retry_loop_stops_when_runtime_active(monkeypatch) -> None:
    """The startup-retry loop must defer to other recovery paths once active.

    Once any path (Moonraker recovery / connection restored) brings the runtime
    up, the startup-retry loop must exit instead of re-running full startup and
    racing those paths.
    """
    app = MoonrakerOwlApp(build_config())
    app._loop = asyncio.get_running_loop()
    app._state = AgentState.ACTIVE

    start_services_calls = 0

    async def _fake_start_services(self: MoonrakerOwlApp) -> bool:
        nonlocal start_services_calls
        start_services_calls += 1
        return False

    app._start_services = types.MethodType(  # type: ignore[method-assign]
        _fake_start_services, app
    )

    _real_sleep = asyncio.sleep

    async def _fast_sleep(_delay: float, *args: Any, **kwargs: Any) -> None:
        await _real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", _fast_sleep)

    await asyncio.wait_for(app._startup_retry_loop(), timeout=1.0)

    assert start_services_calls == 0


@pytest.mark.asyncio
async def test_start_health_publisher_replaces_previous_publisher(monkeypatch) -> None:
    """Repeated _start_health_publisher calls must not orphan the old task.

    The startup-retry loop can invoke _start_health_publisher across multiple
    degraded retries. Each call must stop the previous publisher before creating
    a new one so we never leak a running HealthPublisher task.
    """
    app = MoonrakerOwlApp(build_config())
    app._loop = asyncio.get_running_loop()
    app._mqtt_client = object()

    created: list[Any] = []

    class _StubHealthPublisher:
        def __init__(self, **kwargs: Any) -> None:
            self.started = False
            self.stopped = False
            created.append(self)

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.stopped = True

    monkeypatch.setattr("moonraker_owl.app.HealthPublisher", _StubHealthPublisher)

    await app._start_health_publisher("device-1")
    await app._start_health_publisher("device-1")

    assert len(created) == 2
    assert created[0].stopped is True
    assert created[1].started is True
    assert app._health_publisher is created[1]


@pytest.mark.asyncio
async def test_start_services_starts_supervisor_even_if_runtime_start_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconnect supervisor must start even when runtime startup raises.

    Regression for staging 2026-08-01: during a boot-order race,
    _start_runtime_components() raised (Klipper not ready), which skipped
    start_supervisor(). The agent later reached ACTIVE via a recovery path with
    no reconnect supervisor, so when the token was renewed the reconnect was
    requested but never executed, and the broker kicked the client at token
    expiry with no recovery.
    """
    config = build_config()
    config.cloud.device_private_key = "test-private-key"
    backend = _StubPrinterBackend()
    app = MoonrakerOwlApp(config, printer_backend=backend)

    monkeypatch.setattr("moonraker_owl.app.TokenManager", _StubTokenManager)
    monkeypatch.setattr("moonraker_owl.app.CloudConfigManager", _StubCloudConfigManager)
    monkeypatch.setattr("moonraker_owl.app.MQTTClient", _StubMqttClient)
    monkeypatch.setattr(
        "moonraker_owl.app.ConnectionCoordinator", _StubConnectionCoordinator
    )

    async def _fake_start_metadata_reporter(
        self: MoonrakerOwlApp, device_id: str
    ) -> None:
        return None

    async def _fake_connect_mqtt(self: MoonrakerOwlApp) -> bool:
        self._mqtt_ready = True
        return True

    async def _fake_start_runtime_components(
        self: MoonrakerOwlApp, *, preserve_print_state: bool = False
    ) -> bool:
        raise RuntimeError("klipper not ready")

    async def _fake_start_health_server(self: MoonrakerOwlApp) -> None:
        return None

    async def _fake_start_health_publisher(
        self: MoonrakerOwlApp, device_id: str
    ) -> None:
        return None

    def _fake_subscribe_config_notifications(self: MoonrakerOwlApp) -> None:
        return None

    def _fake_start_moonraker_monitor(self: MoonrakerOwlApp) -> None:
        return None

    monkeypatch.setattr(
        MoonrakerOwlApp, "_start_metadata_reporter", _fake_start_metadata_reporter
    )
    monkeypatch.setattr(MoonrakerOwlApp, "_connect_mqtt", _fake_connect_mqtt)
    monkeypatch.setattr(
        MoonrakerOwlApp, "_start_runtime_components", _fake_start_runtime_components
    )
    monkeypatch.setattr(
        MoonrakerOwlApp, "_start_health_server", _fake_start_health_server
    )
    monkeypatch.setattr(
        MoonrakerOwlApp, "_start_health_publisher", _fake_start_health_publisher
    )
    monkeypatch.setattr(
        MoonrakerOwlApp,
        "_subscribe_config_notifications",
        _fake_subscribe_config_notifications,
    )
    monkeypatch.setattr(
        MoonrakerOwlApp, "_start_moonraker_monitor", _fake_start_moonraker_monitor
    )

    started = await app._start_services()

    assert started is False
    assert isinstance(app._connection_coordinator, _StubConnectionCoordinator)
    assert app._connection_coordinator.supervisor_starts == 1
