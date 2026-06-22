"""Unit tests for ConnectionCoordinator.

Tests the unified reconnection mechanism that handles:
- Token renewal reconnections
- Connection loss recovery
- Auth failure handling
- Concurrent reconnect request coordination
"""

import asyncio
import logging
from unittest.mock import AsyncMock, Mock

import pytest

from moonraker_owl.connection import (
    ConnectionCoordinator,
    ConnectionState,
    ReconnectReason,
)


class FakeMQTTClient:
    """Minimal mock for MQTTClient used in coordinator tests."""

    def __init__(self, connect_succeeds: bool = True, connect_delay: float = 0):
        self.connect_succeeds = connect_succeeds
        self.connect_delay = connect_delay
        self.connect_call_count = 0
        self.disconnect_call_count = 0
        self._connected = False
        self._last_connect_rc: int | None = None
        # Declared (typed) so tests configure a real attribute rather than an ad-hoc instance-dict
        # entry. The coordinator reads it via getattr in _read_last_disconnect_rc.
        self.last_disconnect_rc: int | None = None

    @property
    def last_connect_rc(self) -> int | None:
        return self._last_connect_rc

    async def connect(
        self, *, clean_start: bool = True, session_expiry: int | None = None
    ):
        self.connect_call_count += 1
        if self.connect_delay > 0:
            await asyncio.sleep(self.connect_delay)
        if not self.connect_succeeds:
            self._last_connect_rc = 134  # Simulate auth failure
            raise ConnectionError("Simulated connection failure")
        self._last_connect_rc = 0
        self._connected = True

    async def disconnect(self):
        self.disconnect_call_count += 1
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected


class FakeTokenManager:
    """Minimal mock for TokenManager used in coordinator tests."""

    def __init__(self, refresh_succeeds: bool = True):
        self.refresh_succeeds = refresh_succeeds
        self.refresh_call_count = 0

    async def refresh_token_now(self):
        self.refresh_call_count += 1
        if not self.refresh_succeeds:
            raise Exception("Token refresh failed")


class FakeResilienceConfig:
    """Minimal mock for ResilienceConfig."""

    def __init__(self):
        self.reconnect_initial_seconds = 0.05
        self.reconnect_max_seconds = 0.2
        self.reconnect_jitter_ratio = 0.0
        self.reconnect_perpetual_seconds = 0.3


@pytest.fixture
def coordinator_setup():
    """Create a ConnectionCoordinator with fake dependencies."""

    def _create(
        connect_succeeds: bool = True,
        connect_delay: float = 0,
        refresh_succeeds: bool = True,
    ):
        mqtt_client = FakeMQTTClient(
            connect_succeeds=connect_succeeds, connect_delay=connect_delay
        )
        token_manager = FakeTokenManager(refresh_succeeds=refresh_succeeds)
        resilience_config = FakeResilienceConfig()

        coordinator = ConnectionCoordinator(
            mqtt_client=mqtt_client,
            token_manager=token_manager,
            resilience_config=resilience_config,
            session_expiry=3600,
        )

        return coordinator, mqtt_client, token_manager

    return _create


@pytest.mark.asyncio
async def test_reconnect_reason_enum_values():
    """Verify ReconnectReason enum has all expected values."""
    assert ReconnectReason.TOKEN_RENEWED.value == "token_renewed"
    assert ReconnectReason.CONNECTION_LOST.value == "connection_lost"
    assert ReconnectReason.AUTH_FAILURE.value == "auth_failure"
    assert ReconnectReason.BROKER_DISCONNECT.value == "broker_disconnect"


@pytest.mark.asyncio
async def test_connection_state_enum_values():
    """Verify ConnectionState enum has all expected values."""
    assert ConnectionState.DISCONNECTED.value == "disconnected"
    assert ConnectionState.CONNECTING.value == "connecting"
    assert ConnectionState.CONNECTED.value == "connected"
    assert ConnectionState.RECONNECTING.value == "reconnecting"


@pytest.mark.asyncio
async def test_initial_state_is_disconnected(coordinator_setup):
    """Test that coordinator starts in disconnected state."""
    coordinator, _, _ = coordinator_setup()
    assert coordinator.state == ConnectionState.DISCONNECTED
    assert coordinator.is_connected is False


@pytest.mark.asyncio
async def test_connect_transitions_to_connected(coordinator_setup):
    """Test that connect() changes state to CONNECTED."""
    coordinator, mqtt_client, _ = coordinator_setup()

    await coordinator.connect()

    assert coordinator.state == ConnectionState.CONNECTED
    assert coordinator.is_connected is True
    assert mqtt_client.connect_call_count == 1


@pytest.mark.asyncio
async def test_connect_failure_stays_disconnected(coordinator_setup):
    """Test that failed connect stays in DISCONNECTED state."""
    coordinator, mqtt_client, _ = coordinator_setup(connect_succeeds=False)

    with pytest.raises(ConnectionError):
        await coordinator.connect()

    assert coordinator.state == ConnectionState.DISCONNECTED
    assert coordinator.is_connected is False


@pytest.mark.asyncio
async def test_disconnect_transitions_to_disconnected(coordinator_setup):
    """Test that disconnect() changes state to DISCONNECTED."""
    coordinator, mqtt_client, _ = coordinator_setup()

    await coordinator.connect()
    assert coordinator.is_connected is True

    await coordinator.disconnect()

    assert coordinator.state == ConnectionState.DISCONNECTED
    assert mqtt_client.disconnect_call_count == 1


@pytest.mark.asyncio
async def test_request_reconnect_sets_pending_reason(coordinator_setup):
    """Test that request_reconnect sets a pending reason."""
    coordinator, _, _ = coordinator_setup()

    coordinator.request_reconnect(ReconnectReason.CONNECTION_LOST)

    assert coordinator._pending_reason == ReconnectReason.CONNECTION_LOST
    assert coordinator._reconnect_event.is_set()


@pytest.mark.asyncio
async def test_token_renewed_has_highest_priority(coordinator_setup):
    """Test that TOKEN_RENEWED overrides other pending reasons."""
    coordinator, _, _ = coordinator_setup()

    coordinator.request_reconnect(ReconnectReason.CONNECTION_LOST)
    assert coordinator._pending_reason == ReconnectReason.CONNECTION_LOST

    coordinator.request_reconnect(ReconnectReason.TOKEN_RENEWED)
    assert coordinator._pending_reason == ReconnectReason.TOKEN_RENEWED


@pytest.mark.asyncio
async def test_auth_failure_overrides_connection_lost(coordinator_setup):
    """Test that AUTH_FAILURE overrides CONNECTION_LOST."""
    coordinator, _, _ = coordinator_setup()

    coordinator.request_reconnect(ReconnectReason.CONNECTION_LOST)
    assert coordinator._pending_reason == ReconnectReason.CONNECTION_LOST

    coordinator.request_reconnect(ReconnectReason.AUTH_FAILURE)
    assert coordinator._pending_reason == ReconnectReason.AUTH_FAILURE


@pytest.mark.asyncio
async def test_supervisor_processes_reconnect_request(coordinator_setup):
    """Test that supervisor loop processes reconnect requests."""
    coordinator, mqtt_client, _ = coordinator_setup()

    # Start connected
    await coordinator.connect()
    assert mqtt_client.connect_call_count == 1

    # Start supervisor
    coordinator.start_supervisor()

    # Request reconnect
    coordinator.request_reconnect(ReconnectReason.CONNECTION_LOST)

    # Wait for processing
    await asyncio.sleep(0.2)

    await coordinator.stop_supervisor()

    # Should have disconnected and reconnected
    assert mqtt_client.disconnect_call_count >= 1
    assert mqtt_client.connect_call_count >= 2


@pytest.mark.asyncio
async def test_stop_supervisor_cancels_task(coordinator_setup):
    """Test that stop_supervisor properly cancels the supervisor task."""
    coordinator, _, _ = coordinator_setup()

    coordinator.start_supervisor()
    assert coordinator._supervisor_task is not None
    assert not coordinator._supervisor_task.done()

    await coordinator.stop_supervisor()
    await asyncio.sleep(0.05)  # Give time for cancellation

    assert coordinator._supervisor_task is None


@pytest.mark.asyncio
async def test_reconnect_ignored_during_shutdown(coordinator_setup):
    """Test that reconnect requests are ignored during shutdown."""
    coordinator, _, _ = coordinator_setup()

    # Set stop event (simulating shutdown)
    coordinator._stop_event.set()

    # Clear any pending state
    coordinator._pending_reason = None
    coordinator._reconnect_event.clear()

    coordinator.request_reconnect(ReconnectReason.CONNECTION_LOST)

    # Should not set pending reason during shutdown
    assert coordinator._pending_reason is None


@pytest.mark.asyncio
async def test_auth_failure_triggers_token_refresh(coordinator_setup):
    """Test that AUTH_FAILURE reason triggers token refresh."""
    coordinator, mqtt_client, token_manager = coordinator_setup()

    await coordinator.connect()
    coordinator.start_supervisor()

    # Request reconnect due to auth failure
    coordinator.request_reconnect(ReconnectReason.AUTH_FAILURE)

    # Wait for processing
    await asyncio.sleep(0.3)

    await coordinator.stop_supervisor()

    # Token should have been refreshed
    assert token_manager.refresh_call_count >= 1


@pytest.mark.asyncio
async def test_reconnected_callback_invoked(coordinator_setup):
    """Test that registered callbacks are invoked after reconnection."""
    coordinator, mqtt_client, _ = coordinator_setup()

    callback_invoked = asyncio.Event()

    def on_reconnected():
        callback_invoked.set()

    coordinator.register_reconnected_callback(on_reconnected)

    await coordinator.connect()
    coordinator.start_supervisor()

    # Trigger reconnect
    coordinator.request_reconnect(ReconnectReason.CONNECTION_LOST)

    # Wait for processing and callback
    await asyncio.sleep(0.3)

    await coordinator.stop_supervisor()

    assert callback_invoked.is_set()


@pytest.mark.asyncio
async def test_disconnected_callback_invoked(coordinator_setup):
    """Test that disconnected callbacks are invoked during reconnection."""
    coordinator, mqtt_client, _ = coordinator_setup()

    callback_invoked = asyncio.Event()
    received_reason = None

    def on_disconnected(reason):
        nonlocal received_reason
        received_reason = reason
        callback_invoked.set()

    coordinator.register_disconnected_callback(on_disconnected)

    await coordinator.connect()
    coordinator.start_supervisor()

    # Trigger reconnect
    coordinator.request_reconnect(ReconnectReason.TOKEN_RENEWED)

    # Wait for processing
    await asyncio.sleep(0.3)

    await coordinator.stop_supervisor()

    assert callback_invoked.is_set()
    assert received_reason == ReconnectReason.TOKEN_RENEWED


# ── Connection-health by-cause counters (ADR-0045) ──────────────────────────


@pytest.mark.asyncio
async def test_token_renew_reconnect_excluded_from_churn(coordinator_setup):
    """MANDATORY (ADR-0045): a token-refresh reconnect must NOT register as churn.

    The old panel counted every (re)connect — including the ~1.2/h planned token
    renewal — which pinned a perfectly healthy printer in permanent "Attention".
    Token refresh now lives only in the full reconnect ledger; the user-facing
    churn signal (unexpected disconnects) excludes it entirely.
    """
    coordinator, _mqtt, _ = coordinator_setup()

    await coordinator.connect()
    coordinator.start_supervisor()

    coordinator.request_reconnect(ReconnectReason.TOKEN_RENEWED)
    await asyncio.sleep(0.3)

    await coordinator.stop_supervisor()

    # Full ledger keeps the planned reconnect visible for diagnostics...
    assert coordinator.reconnects_by_cause[ReconnectReason.TOKEN_RENEWED.value] == 1
    # ...but it is excluded from the churn (unexpected-disconnect) signal.
    assert ReconnectReason.TOKEN_RENEWED.value not in coordinator.disconnects_by_cause
    assert coordinator.churn_disconnects_total == 0
    assert coordinator.recent_disconnects_snapshot() == []


@pytest.mark.asyncio
async def test_connection_lost_counts_as_churn(coordinator_setup):
    """An unexpected drop increments both the reconnect ledger and the churn signal."""
    coordinator, mqtt_client, _ = coordinator_setup()
    mqtt_client.last_disconnect_rc = 7  # MQTT5 "connection lost / keepalive"

    await coordinator.connect()
    coordinator.start_supervisor()

    coordinator.request_reconnect(ReconnectReason.CONNECTION_LOST)
    await asyncio.sleep(0.3)

    await coordinator.stop_supervisor()

    assert coordinator.reconnects_by_cause[ReconnectReason.CONNECTION_LOST.value] == 1
    assert coordinator.disconnects_by_cause[ReconnectReason.CONNECTION_LOST.value] == 1
    assert coordinator.churn_disconnects_total == 1

    recent = coordinator.recent_disconnects_snapshot()
    assert len(recent) == 1
    assert recent[0].cause == ReconnectReason.CONNECTION_LOST.value
    assert recent[0].reason_code == 7
    assert recent[0].will_reconnect is True


@pytest.mark.asyncio
async def test_disconnect_event_callback_fires_for_unexpected_only(coordinator_setup):
    """The disconnect-event publisher fires for churn, never for token refresh."""
    coordinator, mqtt_client, _ = coordinator_setup()
    mqtt_client.last_disconnect_rc = 142

    events = []
    coordinator.register_disconnect_event_callback(events.append)

    await coordinator.connect()
    coordinator.start_supervisor()

    # Planned token refresh: no event.
    coordinator.request_reconnect(ReconnectReason.TOKEN_RENEWED)
    await asyncio.sleep(0.2)
    assert events == []

    # Unexpected broker disconnect: one event, after the link is back up.
    coordinator.request_reconnect(ReconnectReason.BROKER_DISCONNECT)
    await asyncio.sleep(0.3)

    await coordinator.stop_supervisor()

    assert len(events) == 1
    record = events[0]
    assert record.cause == ReconnectReason.BROKER_DISCONNECT.value
    assert record.reason_code == 142
    payload = record.as_event_dict()
    assert payload["cause"] == "broker_disconnect"
    assert payload["reasonCode"] == 142
    assert payload["willReconnect"] is True
    assert "T" in payload["at"]  # ISO-8601 timestamp


@pytest.mark.asyncio
async def test_connect_with_backoff_retries(coordinator_setup):
    """Test that _connect_with_backoff retries on failure until stopped."""
    coordinator, mqtt_client, _ = coordinator_setup(connect_succeeds=False)

    async def stop_after_delay():
        await asyncio.sleep(1.5)
        coordinator._stop_event.set()

    stop_task = asyncio.create_task(stop_after_delay())

    result = await coordinator._connect_with_backoff()

    assert result is False
    # Should have made multiple attempts
    assert mqtt_client.connect_call_count > 1
    await stop_task


@pytest.mark.asyncio
async def test_connect_with_backoff_uses_full_jitter(coordinator_setup):
    """Backoff sleeps should be drawn from the full [floor, delay] range.

    With full jitter the sampled sleep must never exceed the current backoff
    delay (symmetric jitter could exceed it). We capture the timeouts passed to
    asyncio.wait_for and assert they stay within the expected bounds.
    """
    coordinator, mqtt_client, _ = coordinator_setup(connect_succeeds=False)
    coordinator._resilience.reconnect_jitter_ratio = 1.0

    captured: list[float] = []
    real_wait_for = asyncio.wait_for

    async def spy_wait_for(awaitable, timeout):
        captured.append(timeout)
        if len(captured) >= 4:
            coordinator._stop_event.set()
        return await real_wait_for(awaitable, timeout)

    import moonraker_owl.connection as connection_module

    original = connection_module.asyncio.wait_for
    connection_module.asyncio.wait_for = spy_wait_for
    try:
        await coordinator._connect_with_backoff()
    finally:
        connection_module.asyncio.wait_for = original

    assert captured, "expected at least one backoff sleep"
    # The coordinator floors the initial delay at 0.5s.
    effective_initial = max(0.5, coordinator._resilience.reconnect_initial_seconds)
    upper = max(effective_initial, coordinator._resilience.reconnect_max_seconds)
    # full jitter => each sleep in [floor, current_delay] <= upper
    for sleep_for in captured:
        assert 0.0 <= sleep_for <= upper + 1e-9
    # Not every sample should equal the deterministic delay (jitter applied).
    assert any(abs(s - effective_initial) > 1e-6 for s in captured)


@pytest.mark.asyncio
async def test_connection_lost_ignored_during_reconnecting(coordinator_setup):
    """Test that CONNECTION_LOST requests are ignored while RECONNECTING.

    This prevents a reconnection loop when disconnect() is called as part of
    the reconnection process - the resulting on_disconnect callback should not
    trigger another reconnection.
    """
    coordinator, _, _ = coordinator_setup()

    # Simulate being in RECONNECTING state
    coordinator._state = ConnectionState.RECONNECTING

    # Clear any pending state
    coordinator._pending_reason = None
    coordinator._reconnect_event.clear()

    # Request reconnect with CONNECTION_LOST (as would happen from on_disconnect)
    coordinator.request_reconnect(ReconnectReason.CONNECTION_LOST)

    # Should be ignored - no pending reason set
    assert coordinator._pending_reason is None
    assert not coordinator._reconnect_event.is_set()


@pytest.mark.asyncio
async def test_token_renewed_ignored_during_reconnecting(coordinator_setup):
    """Test that TOKEN_RENEWED is suppressed during RECONNECTING.

    All reconnect requests are suppressed while a reconnection is in progress.
    TOKEN_RENEWED is unnecessary because _connect_with_backoff() calls
    get_mqtt_credentials() on every retry, which picks up the latest token.
    Allowing it would cause a redundant disconnect → reconnect cycle after
    the current one finishes (reconnection storm).
    """
    coordinator, _, _ = coordinator_setup()

    # Simulate being in RECONNECTING state
    coordinator._state = ConnectionState.RECONNECTING

    # Clear any pending state
    coordinator._pending_reason = None
    coordinator._reconnect_event.clear()

    # Request reconnect with TOKEN_RENEWED
    coordinator.request_reconnect(ReconnectReason.TOKEN_RENEWED)

    # Should be suppressed — no pending reason set
    assert coordinator._pending_reason is None
    assert not coordinator._reconnect_event.is_set()


@pytest.mark.asyncio
async def test_auth_failure_ignored_during_reconnecting(coordinator_setup):
    """Test that AUTH_FAILURE is suppressed during RECONNECTING.

    During _connect_with_backoff(), failed connection attempts fire paho's
    on_disconnect(rc=134) which schedules AUTH_FAILURE via call_soon_threadsafe.
    If queued, these stale events trigger a redundant _execute_reconnect after
    the current one finishes — disconnecting the just-established session and
    causing a reconnection storm.
    """
    coordinator, _, _ = coordinator_setup()

    # Simulate being in RECONNECTING state
    coordinator._state = ConnectionState.RECONNECTING

    # Clear any pending state
    coordinator._pending_reason = None
    coordinator._reconnect_event.clear()

    # Request reconnect with AUTH_FAILURE
    coordinator.request_reconnect(ReconnectReason.AUTH_FAILURE)

    # Should be suppressed — no pending reason set
    assert coordinator._pending_reason is None
    assert not coordinator._reconnect_event.is_set()


@pytest.mark.asyncio
async def test_perpetual_retry_after_backoff_exhaustion(coordinator_setup):
    """Test that connection retries perpetually after exponential backoff is exhausted.

    After _BACKOFF_MAX_ATTEMPTS (10) failed exponential-backoff attempts,
    the coordinator should keep retrying at a fixed interval until it succeeds.
    """
    from moonraker_owl.connection import _BACKOFF_MAX_ATTEMPTS

    # Create a client that fails first N times then succeeds
    succeed_after = _BACKOFF_MAX_ATTEMPTS + 3  # fail all backoff + 3 perpetual rounds
    coordinator, mqtt_client, _ = coordinator_setup(connect_succeeds=False)

    original_connect = mqtt_client.connect

    async def connect_eventually(**kwargs):
        mqtt_client.connect_call_count += 1
        if mqtt_client.connect_call_count >= succeed_after:
            mqtt_client.connect_succeeds = True
            mqtt_client._connected = True
            return
        raise ConnectionError("Simulated connection failure")

    # Patch connect to not double-count (original increments too)
    mqtt_client.connect_call_count = 0
    mqtt_client.connect = connect_eventually

    result = await coordinator._connect_with_backoff()

    assert result is True
    # Must have gone past exponential backoff into perpetual retry
    assert mqtt_client.connect_call_count >= succeed_after


@pytest.mark.asyncio
async def test_perpetual_retry_stops_on_stop_event(coordinator_setup):
    """Test that perpetual retry stops when stop event is set."""
    coordinator, mqtt_client, _ = coordinator_setup(connect_succeeds=False)

    async def set_stop_after_delay():
        await asyncio.sleep(0.3)
        coordinator._stop_event.set()

    stop_task = asyncio.create_task(set_stop_after_delay())

    result = await coordinator._connect_with_backoff()

    assert result is False
    # Should have made at least one attempt before being stopped
    assert mqtt_client.connect_call_count >= 1
    await stop_task


@pytest.mark.asyncio
async def test_auth_failure_during_backoff_refreshes_token(coordinator_setup):
    """Test that auth failures during _connect_with_backoff trigger token refresh.

    Regression test: when EMQX disconnects the agent due to token expiry with
    rc=0 (clean disconnect), the coordinator classifies it as CONNECTION_LOST
    and skips the upfront token refresh.  Without this fix, _connect_with_backoff
    retries with the same expired token forever because the auth-retry inside
    MQTTClient.connect() is bypassed when _connect_with_backoff catches the
    exception.
    """
    coordinator, mqtt_client, token_manager = coordinator_setup(
        connect_succeeds=False
    )

    # Stop after a few attempts to avoid infinite loop
    async def stop_after_delay():
        await asyncio.sleep(0.5)
        coordinator._stop_event.set()

    stop_task = asyncio.create_task(stop_after_delay())

    await coordinator._connect_with_backoff()

    # Token should have been refreshed on each auth-failure attempt
    assert token_manager.refresh_call_count >= 1
    await stop_task


@pytest.mark.asyncio
async def test_supervisor_exception_restarts_supervisor(
    coordinator_setup, caplog, monkeypatch
):
    # Tiny backoff so the test does not sleep a full second.
    monkeypatch.setattr(
        "moonraker_owl.connection._SUPERVISOR_RESTART_BACKOFF_INITIAL_SECONDS", 0.01
    )

    coordinator, _, _ = coordinator_setup()

    async def boom():
        raise RuntimeError("supervisor boom")

    task = asyncio.create_task(boom())
    await asyncio.sleep(0)

    coordinator._supervisor_task = task

    with caplog.at_level(logging.WARNING):
        coordinator._handle_supervisor_exit(task)

    assert "Connection supervisor crashed" in caplog.text
    # Restart is scheduled, not synchronous — task is None until the timer fires.
    assert coordinator._supervisor_task is None
    assert coordinator._supervisor_restart_handle is not None
    assert coordinator._supervisor_restart_attempts == 1

    # Let the scheduled restart fire.
    await asyncio.sleep(0.05)
    assert coordinator._supervisor_task is not None
    assert not coordinator._supervisor_task.done()

    await coordinator.stop_supervisor()


@pytest.mark.asyncio
async def test_supervisor_normal_return_restarts_and_logs(
    coordinator_setup, caplog, monkeypatch
):
    monkeypatch.setattr(
        "moonraker_owl.connection._SUPERVISOR_RESTART_BACKOFF_INITIAL_SECONDS", 0.01
    )

    coordinator, _, _ = coordinator_setup()

    async def noop():
        return None

    task = asyncio.create_task(noop())
    await asyncio.sleep(0)

    coordinator._supervisor_task = task

    with caplog.at_level(logging.WARNING):
        coordinator._handle_supervisor_exit(task)

    assert "Connection supervisor exited unexpectedly without raising an exception" in caplog.text
    assert coordinator._supervisor_task is None
    assert coordinator._supervisor_restart_handle is not None

    await asyncio.sleep(0.05)
    assert coordinator._supervisor_task is not None
    assert not coordinator._supervisor_task.done()

    await coordinator.stop_supervisor()


@pytest.mark.asyncio
async def test_supervisor_restart_backoff_is_exponential(
    coordinator_setup, caplog, monkeypatch
):
    """Each rapid crash should double the scheduled delay, capped at the max."""
    monkeypatch.setattr(
        "moonraker_owl.connection._SUPERVISOR_RESTART_BACKOFF_INITIAL_SECONDS", 1.0
    )
    monkeypatch.setattr(
        "moonraker_owl.connection._SUPERVISOR_RESTART_BACKOFF_MAX_SECONDS", 5.0
    )

    coordinator, _, _ = coordinator_setup()
    loop = asyncio.get_running_loop()

    expected_delays = [1.0, 2.0, 4.0, 5.0]  # 1, 2, 4, 8→capped at 5
    for attempt_index, expected in enumerate(expected_delays, start=1):
        async def boom():
            raise RuntimeError(f"boom-{attempt_index}")

        task = asyncio.create_task(boom())
        await asyncio.sleep(0)
        coordinator._supervisor_task = task
        # Stamp a fresh start time so the liveness reset does not trigger.
        coordinator._supervisor_started_at = loop.time()

        coordinator._handle_supervisor_exit(task)

        assert coordinator._supervisor_restart_attempts == attempt_index
        handle = coordinator._supervisor_restart_handle
        assert handle is not None
        actual_delay = handle.when() - loop.time()
        # Allow a small slop for scheduler timing.
        assert abs(actual_delay - expected) < 0.05, (
            f"attempt {attempt_index}: expected ~{expected}s, got {actual_delay:.3f}s"
        )
        handle.cancel()
        coordinator._supervisor_restart_handle = None


@pytest.mark.asyncio
async def test_supervisor_gives_up_after_max_attempts(
    coordinator_setup, caplog, monkeypatch
):
    """Once MAX_ATTEMPTS rapid restarts are exhausted, no further restart is scheduled."""
    monkeypatch.setattr(
        "moonraker_owl.connection._SUPERVISOR_RESTART_MAX_ATTEMPTS", 2
    )
    monkeypatch.setattr(
        "moonraker_owl.connection._SUPERVISOR_RESTART_BACKOFF_INITIAL_SECONDS", 0.01
    )

    coordinator, _, _ = coordinator_setup()
    loop = asyncio.get_running_loop()
    fatal_attempts: list[int] = []
    coordinator.register_fatal_supervisor_failure_callback(fatal_attempts.append)

    for i in range(3):  # 2 allowed restarts, third call should give up
        async def boom():
            raise RuntimeError(f"boom-{i}")

        task = asyncio.create_task(boom())
        await asyncio.sleep(0)
        coordinator._supervisor_task = task
        coordinator._supervisor_started_at = loop.time()

        with caplog.at_level(logging.CRITICAL):
            coordinator._handle_supervisor_exit(task)

        # Cancel any scheduled restart so a fired call_later does not race the
        # next iteration's manual injection.
        if coordinator._supervisor_restart_handle is not None:
            coordinator._supervisor_restart_handle.cancel()
            coordinator._supervisor_restart_handle = None

    assert coordinator._supervisor_restart_attempts == 3
    assert coordinator._supervisor_restart_handle is None
    assert coordinator._supervisor_task is None
    assert coordinator._stop_event.is_set()
    assert fatal_attempts == [3]
    assert "requesting full agent restart" in caplog.text


@pytest.mark.asyncio
async def test_supervisor_long_lifetime_resets_restart_counter(
    coordinator_setup, monkeypatch
):
    """A supervisor that lived past the liveness threshold should not count toward rapid restarts."""
    monkeypatch.setattr(
        "moonraker_owl.connection._SUPERVISOR_RESTART_LIVENESS_SECONDS", 0.1
    )
    monkeypatch.setattr(
        "moonraker_owl.connection._SUPERVISOR_RESTART_BACKOFF_INITIAL_SECONDS", 0.01
    )

    coordinator, _, _ = coordinator_setup()
    loop = asyncio.get_running_loop()

    # First crash bumps the counter to 1.
    async def boom1():
        raise RuntimeError("first")

    t1 = asyncio.create_task(boom1())
    await asyncio.sleep(0)
    coordinator._supervisor_task = t1
    coordinator._supervisor_started_at = loop.time()
    coordinator._handle_supervisor_exit(t1)
    assert coordinator._supervisor_restart_attempts == 1
    if coordinator._supervisor_restart_handle is not None:
        coordinator._supervisor_restart_handle.cancel()
        coordinator._supervisor_restart_handle = None

    # Simulate a supervisor that ran long enough to clear the threshold.
    async def boom2():
        raise RuntimeError("second")

    t2 = asyncio.create_task(boom2())
    await asyncio.sleep(0)
    coordinator._supervisor_task = t2
    coordinator._supervisor_started_at = loop.time() - 1.0  # well past 0.1s threshold

    coordinator._handle_supervisor_exit(t2)
    assert coordinator._supervisor_restart_attempts == 1, (
        "long-lived supervisor death should reset the counter before incrementing"
    )
    if coordinator._supervisor_restart_handle is not None:
        coordinator._supervisor_restart_handle.cancel()
        coordinator._supervisor_restart_handle = None


@pytest.mark.asyncio
async def test_stop_supervisor_cancels_pending_restart(
    coordinator_setup, monkeypatch
):
    """stop_supervisor must cancel a scheduled restart so it does not fire after shutdown."""
    monkeypatch.setattr(
        "moonraker_owl.connection._SUPERVISOR_RESTART_BACKOFF_INITIAL_SECONDS", 1.0
    )

    coordinator, _, _ = coordinator_setup()
    loop = asyncio.get_running_loop()

    async def boom():
        raise RuntimeError("boom")

    task = asyncio.create_task(boom())
    await asyncio.sleep(0)
    coordinator._supervisor_task = task
    coordinator._supervisor_started_at = loop.time()
    coordinator._handle_supervisor_exit(task)

    handle = coordinator._supervisor_restart_handle
    assert handle is not None

    await coordinator.stop_supervisor()

    assert handle.cancelled()
    assert coordinator._supervisor_restart_handle is None
    assert coordinator._supervisor_restart_attempts == 0


@pytest.mark.asyncio
async def test_start_supervisor_ignores_requests_after_stop_signaled(
    coordinator_setup, caplog
):
    coordinator, _, _ = coordinator_setup()
    coordinator._stop_event.set()

    with caplog.at_level(logging.WARNING):
        coordinator.start_supervisor()

    assert coordinator._supervisor_task is None
    assert "shutdown was signaled; ignoring" in caplog.text