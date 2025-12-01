"""Connection coordination and reconnection management.

This module provides unified connection management for the MQTT client,
handling token renewal, connection loss, and reconnection with proper
coordination to avoid race conditions.
"""

from __future__ import annotations

import asyncio
import logging
import random
from enum import Enum
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .adapters.mqtt import MQTTClient
    from .config import ResilienceConfig
    from .token_manager import TokenManager

LOGGER = logging.getLogger(__name__)


class ReconnectReason(str, Enum):
    """Reason for requesting a reconnection."""

    TOKEN_RENEWED = "token_renewed"
    """JWT token was renewed and MQTT needs to reconnect with new credentials."""

    CONNECTION_LOST = "connection_lost"
    """MQTT connection was lost unexpectedly."""

    AUTH_FAILURE = "auth_failure"
    """MQTT authentication failed (CONNACK code 5 or similar)."""

    BROKER_DISCONNECT = "broker_disconnect"
    """Broker initiated the disconnect."""


class ConnectionState(str, Enum):
    """Current state of the MQTT connection."""

    DISCONNECTED = "disconnected"
    """Not connected to MQTT broker."""

    CONNECTING = "connecting"
    """Attempting to connect to MQTT broker."""

    CONNECTED = "connected"
    """Successfully connected to MQTT broker."""

    RECONNECTING = "reconnecting"
    """Attempting to reconnect after connection loss."""


class ConnectionCoordinator:
    """Coordinates MQTT connection lifecycle and reconnection.

    This class provides a single point of control for all reconnection logic,
    preventing race conditions between token renewal and connection supervision.

    Key responsibilities:
    - Accept reconnection requests from multiple sources
    - Serialize reconnection attempts (only one at a time)
    - Coordinate with TokenManager for credential refresh
    - Manage backoff delays for failed reconnection attempts
    """

    def __init__(
        self,
        *,
        mqtt_client: MQTTClient,
        token_manager: TokenManager,
        resilience_config: ResilienceConfig,
        session_expiry: Optional[int] = None,
    ) -> None:
        self._mqtt_client = mqtt_client
        self._token_manager = token_manager
        self._resilience = resilience_config
        self._session_expiry = session_expiry

        self._state = ConnectionState.DISCONNECTED
        self._reconnect_lock = asyncio.Lock()
        self._reconnect_event = asyncio.Event()
        self._pending_reason: Optional[ReconnectReason] = None
        self._stop_event = asyncio.Event()
        self._supervisor_task: Optional[asyncio.Task[None]] = None

        # Callbacks
        self._on_connected: Optional[asyncio.Future[None]] = None
        self._on_disconnected_callbacks: list = []
        self._on_reconnected_callbacks: list = []

    @property
    def state(self) -> ConnectionState:
        """Current connection state."""
        return self._state

    @property
    def is_connected(self) -> bool:
        """Whether currently connected to MQTT broker."""
        return self._state == ConnectionState.CONNECTED

    def request_reconnect(self, reason: ReconnectReason) -> None:
        """Request a reconnection (thread-safe).

        Multiple requests are coalesced - only one reconnection will occur.
        TOKEN_RENEWED has highest priority to ensure new credentials are used.

        Args:
            reason: Why reconnection is needed.
        """
        if self._stop_event.is_set():
            return

        # Ignore CONNECTION_LOST during reconnection - this is expected when we
        # disconnect as part of the reconnection process
        if reason == ReconnectReason.CONNECTION_LOST and self._state == ConnectionState.RECONNECTING:
            return

        # Priority: TOKEN_RENEWED > AUTH_FAILURE > others
        previous_reason = self._pending_reason
        if self._pending_reason is None:
            self._pending_reason = reason
        elif reason == ReconnectReason.TOKEN_RENEWED:
            self._pending_reason = reason
        elif (
            reason == ReconnectReason.AUTH_FAILURE
            and self._pending_reason not in (ReconnectReason.TOKEN_RENEWED,)
        ):
            self._pending_reason = reason
        
        if self._pending_reason != previous_reason or previous_reason is None:
            LOGGER.debug("Reconnect requested: %s", self._pending_reason.value)

        self._reconnect_event.set()

    async def connect(self) -> None:
        """Establish initial MQTT connection.

        Raises:
            MQTTConnectionError: If connection fails.
        """
        self._state = ConnectionState.CONNECTING
        LOGGER.info("Establishing MQTT connection")

        try:
            await self._mqtt_client.connect(
                clean_start=False,
                session_expiry=self._session_expiry,
            )
            self._state = ConnectionState.CONNECTED
            LOGGER.info("MQTT connection established")
        except Exception:
            self._state = ConnectionState.DISCONNECTED
            raise

    async def disconnect(self) -> None:
        """Gracefully disconnect from MQTT broker."""
        LOGGER.info("Disconnecting from MQTT broker")
        self._state = ConnectionState.DISCONNECTED

        try:
            await self._mqtt_client.disconnect()
        except Exception:
            pass  # Disconnect cleanup - ignore errors

    def start_supervisor(self) -> None:
        """Start the connection supervision task."""
        if self._supervisor_task is not None and not self._supervisor_task.done():
            LOGGER.warning("Supervisor already running")
            return

        self._stop_event.clear()
        self._supervisor_task = asyncio.create_task(self._supervision_loop())

    async def stop_supervisor(self) -> None:
        """Stop the connection supervision task."""
        self._stop_event.set()
        self._reconnect_event.set()  # Wake up the loop

        if self._supervisor_task is not None:
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except asyncio.CancelledError:
                pass
            self._supervisor_task = None

    def register_reconnected_callback(self, callback) -> None:
        """Register callback to be invoked after successful reconnection."""
        self._on_reconnected_callbacks.append(callback)

    def register_disconnected_callback(self, callback) -> None:
        """Register callback to be invoked on disconnection."""
        self._on_disconnected_callbacks.append(callback)

    async def _supervision_loop(self) -> None:
        """Main supervision loop that handles reconnection requests."""
        while not self._stop_event.is_set():
            try:
                await self._reconnect_event.wait()
            except asyncio.CancelledError:
                break

            self._reconnect_event.clear()

            if self._stop_event.is_set():
                break

            # Get and clear pending reason atomically
            async with self._reconnect_lock:
                reason = self._pending_reason
                self._pending_reason = None

                if reason is None:
                    continue

                await self._execute_reconnect(reason)

    async def _execute_reconnect(self, reason: ReconnectReason) -> None:
        """Execute reconnection with proper state management and backoff.

        Args:
            reason: Why reconnection is needed.
        """
        LOGGER.info("Executing reconnection (reason=%s)", reason.value)
        self._state = ConnectionState.RECONNECTING

        # Notify disconnection handlers
        for callback in self._on_disconnected_callbacks:
            try:
                result = callback(reason)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                LOGGER.warning("Disconnected callback failed", exc_info=True)

        # Disconnect cleanly if still connected
        try:
            await self._mqtt_client.disconnect()
        except Exception:
            pass  # Cleanup disconnect - ignore errors

        # For auth failures, try to refresh token first
        if reason == ReconnectReason.AUTH_FAILURE:
            try:
                LOGGER.info("Refreshing JWT token due to auth failure")
                await self._token_manager.refresh_token_now()
            except Exception as exc:
                LOGGER.error("Failed to refresh token: %s", exc)
                # Continue with reconnection anyway - token might still be valid

        # Attempt reconnection with exponential backoff
        connected = await self._connect_with_backoff()

        if connected:
            self._state = ConnectionState.CONNECTED

            # Notify reconnection handlers
            for callback in self._on_reconnected_callbacks:
                try:
                    result = callback()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    LOGGER.exception("Reconnected callback failed")
        else:
            self._state = ConnectionState.DISCONNECTED
            LOGGER.error("Failed to reconnect after all attempts, will retry on next trigger")
            # Note: We don't schedule automatic retry here.
            # The TokenManager renewal loop will eventually succeed when the cloud
            # comes back online, and _on_token_renewed will trigger a new reconnect.

    async def _connect_with_backoff(self) -> bool:
        """Attempt connection with exponential backoff.

        Returns:
            True if connection succeeded, False otherwise.
        """
        delay = max(0.5, self._resilience.reconnect_initial_seconds)
        max_delay = max(delay, self._resilience.reconnect_max_seconds)
        jitter_ratio = max(0.0, min(1.0, self._resilience.reconnect_jitter_ratio))

        attempt = 0
        max_attempts = 10  # Reasonable limit to prevent infinite loops

        while not self._stop_event.is_set() and attempt < max_attempts:
            attempt += 1

            try:
                LOGGER.debug("MQTT connection attempt %d", attempt)
                await self._mqtt_client.connect(
                    clean_start=False,
                    session_expiry=self._session_expiry,
                )
                LOGGER.debug("MQTT connection attempt %d succeeded", attempt)
                return True

            except Exception as exc:
                LOGGER.warning(
                    "Connection attempt %d failed: %s, retrying in %.1fs",
                    attempt,
                    exc,
                    delay,
                )

                # Apply jitter to delay
                sleep_for = delay
                if jitter_ratio > 0.0:
                    jitter = delay * jitter_ratio
                    lower = max(0.1, delay - jitter)
                    upper = delay + jitter
                    sleep_for = random.uniform(lower, upper)

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=sleep_for,
                    )
                    # Stop event was set
                    break
                except asyncio.TimeoutError:
                    pass

                # Exponential backoff
                delay = min(delay * 2, max_delay)

        return False
