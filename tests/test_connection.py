"""Unit tests for ConnectionCoordinator.

Tests the unified reconnection mechanism that handles:
- Token renewal reconnections
- Connection loss recovery
- Auth failure handling
- Concurrent reconnect request coordination
"""

import asyncio
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

    async def connect(
        self, *, clean_start: bool = True, session_expiry: int | None = None
    ):
        self.connect_call_count += 1
        if self.connect_delay > 0:
            await asyncio.sleep(self.connect_delay)
        if not self.connect_succeeds:
            raise ConnectionError("Simulated connection failure")
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


@pytest.mark.asyncio
async def test_connect_with_backoff_retries(coordinator_setup):
    """Test that _connect_with_backoff retries on failure."""
    coordinator, mqtt_client, _ = coordinator_setup(connect_succeeds=False)

    # This should attempt multiple times before giving up
    result = await coordinator._connect_with_backoff()

    assert result is False
    # Should have made multiple attempts
    assert mqtt_client.connect_call_count > 1


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
async def test_token_renewed_not_ignored_during_reconnecting(coordinator_setup):
    """Test that TOKEN_RENEWED is still processed during RECONNECTING.

    Unlike CONNECTION_LOST, TOKEN_RENEWED should be processed even during
    reconnection as it indicates we have new credentials that need to be used.
    """
    coordinator, _, _ = coordinator_setup()

    # Simulate being in RECONNECTING state
    coordinator._state = ConnectionState.RECONNECTING

    # Clear any pending state
    coordinator._pending_reason = None
    coordinator._reconnect_event.clear()

    # Request reconnect with TOKEN_RENEWED
    coordinator.request_reconnect(ReconnectReason.TOKEN_RENEWED)

    # Should be accepted
    assert coordinator._pending_reason == ReconnectReason.TOKEN_RENEWED
    assert coordinator._reconnect_event.is_set()


@pytest.mark.asyncio
async def test_auth_failure_not_ignored_during_reconnecting(coordinator_setup):
    """Test that AUTH_FAILURE is still processed during RECONNECTING.

    AUTH_FAILURE indicates a credential issue that needs immediate attention.
    """
    coordinator, _, _ = coordinator_setup()

    # Simulate being in RECONNECTING state
    coordinator._state = ConnectionState.RECONNECTING

    # Clear any pending state
    coordinator._pending_reason = None
    coordinator._reconnect_event.clear()

    # Request reconnect with AUTH_FAILURE
    coordinator.request_reconnect(ReconnectReason.AUTH_FAILURE)

    # Should be accepted
    assert coordinator._pending_reason == ReconnectReason.AUTH_FAILURE
    assert coordinator._reconnect_event.is_set()