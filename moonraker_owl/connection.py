"""Connection coordination and reconnection management.

This module provides unified connection management for the MQTT client,
handling token renewal, connection loss, and reconnection with proper
coordination to avoid race conditions.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Awaitable, Callable, Deque, List, Optional, Union

if TYPE_CHECKING:
    from .adapters.mqtt import MQTTClient
    from .config import ResilienceConfig
    from .token_manager import TokenManager

LOGGER = logging.getLogger(__name__)

_RECENT_DISCONNECT_WINDOW_SECONDS = 60.0
"""How long an unexpected disconnect stays in the health-channel fallback list."""

_RECENT_DISCONNECT_MAX = 64
"""Hard cap on the in-memory recent-disconnect list, bounding memory under a reconnect storm."""

_BACKOFF_MAX_ATTEMPTS = 10
"""Number of exponential-backoff retries before switching to perpetual fixed-interval."""

_SUPERVISOR_RESTART_LIVENESS_SECONDS = 30.0
"""How long a supervisor must stay alive before its eventual death is treated as
an isolated failure (resets the rapid-restart counter)."""

_SUPERVISOR_RESTART_MAX_ATTEMPTS = 5
"""Maximum consecutive rapid restarts before the coordinator gives up. Stops a
deterministic crash from spinning the event loop and flooding the log forever."""

_SUPERVISOR_RESTART_BACKOFF_INITIAL_SECONDS = 1.0
"""Initial delay before the first restart attempt."""

_SUPERVISOR_RESTART_BACKOFF_MAX_SECONDS = 30.0
"""Cap on the exponential restart backoff."""


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


@dataclass(frozen=True)
class DisconnectRecord:
    """A single *unexpected* session drop, captured for the ``events/disconnect``
    MQTT channel and the health-channel fallback list.

    Planned token-refresh reconnections deliberately never produce a record — they
    are not churn and must not pollute the user-facing connection-quality signal
    (ADR-0045). ``reason_code`` is the MQTT 5 disconnect reason code (0 when the
    transport could not report one).
    """

    at: datetime
    cause: str
    reason_code: int
    will_reconnect: bool

    def as_event_dict(self) -> dict:
        """Serialize to the JSON shape shared by the event channel and health list."""
        return {
            "at": self.at.replace(microsecond=0).isoformat(),
            "cause": self.cause,
            "reasonCode": self.reason_code,
            "willReconnect": self.will_reconnect,
        }


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

        # Supervisor restart bookkeeping. A deterministic crash inside
        # _supervision_loop would otherwise let _handle_supervisor_exit
        # re-spawn it instantly forever, burning CPU and drowning the log.
        self._supervisor_started_at: Optional[float] = None
        self._supervisor_restart_attempts = 0
        self._supervisor_restart_handle: Optional[asyncio.TimerHandle] = None

        # Callbacks
        self._on_connected: Optional[asyncio.Future[None]] = None
        self._on_disconnected_callbacks: list = []
        self._on_fatal_supervisor_failure_callbacks: list[Callable[[int], None]] = []
        self._on_reconnected_callbacks: list = []
        # Fired (after a successful reconnect) so a publisher can emit the disconnect
        # event over MQTT once the link is back up.
        self._on_disconnect_event_callbacks: List[
            Callable[[DisconnectRecord], Union[None, Awaitable[None]]]
        ] = []

        # Cumulative count of successful reconnections (health reporting).
        self._reconnects_total = 0
        # Per-cause reconnect ledger — the *complete* record, including planned
        # TOKEN_RENEWED cycles. Diagnostic only; the cloud subtracts token refresh.
        self._reconnects_by_cause: dict[str, int] = {r.value: 0 for r in ReconnectReason}
        # Per-cause *unexpected* disconnect ledger (token refresh excluded). This is
        # the user-facing churn signal — see ADR-0045's "polluted reconnect count" fix.
        self._disconnects_by_cause: dict[str, int] = {
            r.value: 0 for r in ReconnectReason if r is not ReconnectReason.TOKEN_RENEWED
        }
        # Rolling window of recent unexpected disconnects (health-channel fallback).
        self._recent_disconnects: Deque[DisconnectRecord] = deque()

    @property
    def state(self) -> ConnectionState:
        """Current connection state."""
        return self._state

    @property
    def reconnects_total(self) -> int:
        """Cumulative number of successful reconnections since process start."""
        return self._reconnects_total

    @property
    def reconnects_by_cause(self) -> dict[str, int]:
        """Per-cause reconnect ledger (complete; includes planned token refresh)."""
        return dict(self._reconnects_by_cause)

    @property
    def disconnects_by_cause(self) -> dict[str, int]:
        """Per-cause *unexpected* disconnect ledger (token refresh excluded)."""
        return dict(self._disconnects_by_cause)

    @property
    def churn_disconnects_total(self) -> int:
        """Total unexpected disconnects — the user-facing churn count. Excludes the
        planned ~1.2/h token-refresh reconnects that used to false-flag healthy
        printers (ADR-0045)."""
        return sum(self._disconnects_by_cause.values())

    def recent_disconnects_snapshot(
        self,
        *,
        now: Optional[datetime] = None,
        window_seconds: float = _RECENT_DISCONNECT_WINDOW_SECONDS,
    ) -> List[DisconnectRecord]:
        """Return a snapshot of unexpected disconnects within the trailing window.

        Side effect: prunes entries older than ``window_seconds`` from the internal
        deque before snapshotting, so repeated reads keep the ledger bounded. The
        returned list is a copy — callers may iterate it without holding the deque.
        """
        reference = now or datetime.now(timezone.utc)
        self._prune_recent_disconnects(reference, window_seconds)
        return list(self._recent_disconnects)

    def _record_unexpected_disconnect(self, record: DisconnectRecord) -> None:
        self._disconnects_by_cause[record.cause] = (
            self._disconnects_by_cause.get(record.cause, 0) + 1
        )
        self._recent_disconnects.append(record)
        self._prune_recent_disconnects(record.at, _RECENT_DISCONNECT_WINDOW_SECONDS)
        while len(self._recent_disconnects) > _RECENT_DISCONNECT_MAX:
            self._recent_disconnects.popleft()

    def _prune_recent_disconnects(self, now: datetime, window_seconds: float) -> None:
        cutoff = now - timedelta(seconds=window_seconds)
        while self._recent_disconnects and self._recent_disconnects[0].at < cutoff:
            self._recent_disconnects.popleft()

    def _read_last_disconnect_rc(self) -> int:
        """Best-effort read of the transport's last MQTT 5 disconnect reason code."""
        try:
            rc = getattr(self._mqtt_client, "last_disconnect_rc", None)
            return int(rc) if rc is not None else 0
        except (TypeError, ValueError):
            return 0

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

        # Suppress ALL reconnect requests while a reconnection is in progress.
        #
        # Why this is safe for every reason:
        #
        #  CONNECTION_LOST / AUTH_FAILURE — paho fires on_disconnect from its
        #  network thread for *each failed* connect attempt inside
        #  _connect_with_backoff().  Those callbacks are dispatched via
        #  call_soon_threadsafe and arrive on the asyncio event loop during the
        #  backoff sleep.  If we queue them, the supervisor will trigger a
        #  redundant _execute_reconnect immediately after the current one
        #  finishes, disconnecting the just-established session.  This is the
        #  root cause of the intermittent reconnection storm.
        #
        #  TOKEN_RENEWED — the renewed token updates _current_token in
        #  TokenManager.  Each retry inside _connect_with_backoff() calls
        #  get_mqtt_credentials(), which reads the latest _current_token.
        #  A separate reconnection cycle is unnecessary and wasteful.
        if self._state == ConnectionState.RECONNECTING:
            LOGGER.debug(
                "Ignoring reconnect request (%s) — already reconnecting",
                reason.value,
            )
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
            # clean_start=False: resume existing broker session (subscriptions +
            # queued QoS 1/2 messages) across reconnects.  Session expires after
            # session_expiry seconds of inactivity (default 1h).
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

        if self._stop_event.is_set():
            LOGGER.warning(
                "Supervisor start requested after shutdown was signaled; ignoring"
            )
            return

        # Cancel any pending restart so we don't end up with two supervisor
        # tasks if start_supervisor is called manually while a backoff timer
        # is in flight.
        if self._supervisor_restart_handle is not None:
            self._supervisor_restart_handle.cancel()
            self._supervisor_restart_handle = None

        loop = asyncio.get_running_loop()
        self._supervisor_started_at = loop.time()
        self._supervisor_task = asyncio.create_task(self._supervision_loop())
        self._supervisor_task.add_done_callback(self._handle_supervisor_exit)

    async def stop_supervisor(self) -> None:
        """Stop the connection supervision task."""
        self._stop_event.set()
        self._reconnect_event.set()  # Wake up the loop

        if self._supervisor_restart_handle is not None:
            self._supervisor_restart_handle.cancel()
            self._supervisor_restart_handle = None

        if self._supervisor_task is not None:
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except asyncio.CancelledError:
                pass
            self._supervisor_task = None

        self._supervisor_restart_attempts = 0
        self._supervisor_started_at = None

    def _handle_supervisor_exit(self, task: asyncio.Task[None]) -> None:
        """Restart the supervisor when it exits unexpectedly, with bounded backoff.

        The coordinator is the single owner of reconnect policy. If its task dies,
        the agent must not remain alive in a permanently non-reconnecting state.
        A deterministic crash, however, must not be allowed to spin the loop —
        rapid restarts are exponentially backed off and capped at
        ``_SUPERVISOR_RESTART_MAX_ATTEMPTS``, after which the coordinator stops
        trying and lets the operator (or the process supervisor) intervene.
        """
        self._supervisor_task = None

        if self._stop_event.is_set():
            self._supervisor_restart_attempts = 0
            self._supervisor_started_at = None
            return

        exc: BaseException | None = None
        with suppress(asyncio.CancelledError):
            exc = task.exception()

        if task.cancelled():
            LOGGER.error(
                "Connection supervisor exited unexpectedly via cancellation while the coordinator is still running"
            )
        else:
            if exc is not None:
                LOGGER.error("Connection supervisor crashed", exc_info=exc)
            else:
                LOGGER.error(
                    "Connection supervisor exited unexpectedly without raising an exception"
                )

        loop = asyncio.get_running_loop()

        # A supervisor that lived past the liveness threshold is treated as a
        # one-off failure, not part of a rapid-restart cluster.
        started_at = self._supervisor_started_at
        if started_at is not None and (loop.time() - started_at) >= _SUPERVISOR_RESTART_LIVENESS_SECONDS:
            self._supervisor_restart_attempts = 0
        self._supervisor_started_at = None

        self._supervisor_restart_attempts += 1

        if self._supervisor_restart_attempts > _SUPERVISOR_RESTART_MAX_ATTEMPTS:
            LOGGER.critical(
                "Connection supervisor has crashed %d times in rapid succession; "
                "stopping reconnect control and requesting full agent restart.",
                self._supervisor_restart_attempts,
            )
            # Defensive: ensure no stale handle from a prior iteration lingers.
            if self._supervisor_restart_handle is not None:
                self._supervisor_restart_handle.cancel()
                self._supervisor_restart_handle = None
            self._pending_reason = None
            self._stop_event.set()
            self._notify_fatal_supervisor_failure(self._supervisor_restart_attempts)
            return

        # Exponential backoff: 1s, 2s, 4s, 8s, 16s, capped at 30s.
        backoff = min(
            _SUPERVISOR_RESTART_BACKOFF_INITIAL_SECONDS
            * (2 ** (self._supervisor_restart_attempts - 1)),
            _SUPERVISOR_RESTART_BACKOFF_MAX_SECONDS,
        )

        # Re-arm the event so any pending reconnect reason is not stranded.
        self._reconnect_event.set()
        LOGGER.warning(
            "Restarting connection supervisor in %.1fs (attempt %d/%d after unexpected exit)",
            backoff,
            self._supervisor_restart_attempts,
            _SUPERVISOR_RESTART_MAX_ATTEMPTS,
        )
        self._supervisor_restart_handle = loop.call_later(
            backoff, self._restart_supervisor
        )

    def _restart_supervisor(self) -> None:
        """Fire the scheduled supervisor restart, unless stop was requested in the meantime."""
        self._supervisor_restart_handle = None
        if self._stop_event.is_set():
            return
        self.start_supervisor()

    def register_reconnected_callback(self, callback) -> None:
        """Register callback to be invoked after successful reconnection."""
        self._on_reconnected_callbacks.append(callback)

    def register_fatal_supervisor_failure_callback(
        self, callback: Callable[[int], None]
    ) -> None:
        """Register callback to be invoked when supervisor restart is exhausted."""
        self._on_fatal_supervisor_failure_callbacks.append(callback)

    def register_disconnected_callback(self, callback) -> None:
        """Register callback to be invoked on disconnection."""
        self._on_disconnected_callbacks.append(callback)

    def register_disconnect_event_callback(
        self, callback: Callable[[DisconnectRecord], Union[None, Awaitable[None]]]
    ) -> None:
        """Register a callback fired after a successful reconnect to publish the
        preceding *unexpected* disconnect over MQTT (planned token refresh excluded).
        Invoked once the link is back up so the publish actually reaches the broker.
        """
        self._on_disconnect_event_callbacks.append(callback)

    def _notify_fatal_supervisor_failure(self, attempts: int) -> None:
        for callback in self._on_fatal_supervisor_failure_callbacks:
            try:
                callback(attempts)
            except Exception:
                LOGGER.exception("Fatal supervisor failure callback failed")

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

        # Capture the disconnect for churn accounting. A planned token-refresh cycle
        # is NOT churn, so it is deliberately excluded here (ADR-0045) — it still
        # increments the full reconnect ledger on success below.
        disconnect_record: Optional[DisconnectRecord] = None
        if reason is not ReconnectReason.TOKEN_RENEWED:
            disconnect_record = DisconnectRecord(
                at=datetime.now(timezone.utc),
                cause=reason.value,
                reason_code=self._read_last_disconnect_rc(),
                will_reconnect=True,
            )
            self._record_unexpected_disconnect(disconnect_record)

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
            self._reconnects_total += 1
            self._reconnects_by_cause[reason.value] = (
                self._reconnects_by_cause.get(reason.value, 0) + 1
            )

            # Now that the link is back up, publish the disconnect event (unexpected
            # drops only — token refresh produced no record above).
            if disconnect_record is not None:
                for callback in self._on_disconnect_event_callbacks:
                    try:
                        result = callback(disconnect_record)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        LOGGER.warning(
                            "Disconnect-event callback failed", exc_info=True
                        )

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
            LOGGER.warning("Reconnection abandoned (coordinator stopping)")

    async def _connect_with_backoff(self) -> bool:
        """Attempt connection with exponential backoff, then perpetual retry.

        First tries up to ``_BACKOFF_MAX_ATTEMPTS`` with exponential backoff.
        If those all fail, switches to a fixed interval
        (``reconnect_perpetual_seconds``) and retries indefinitely until
        connection succeeds or the coordinator is stopped.

        Returns:
            True if connection succeeded, False otherwise.
        """
        delay = max(0.5, self._resilience.reconnect_initial_seconds)
        max_delay = max(delay, self._resilience.reconnect_max_seconds)
        jitter_ratio = max(0.0, min(1.0, self._resilience.reconnect_jitter_ratio))
        perpetual_delay = self._resilience.reconnect_perpetual_seconds

        attempt = 0

        while not self._stop_event.is_set():
            attempt += 1
            in_perpetual = attempt > _BACKOFF_MAX_ATTEMPTS

            try:
                LOGGER.debug("MQTT connection attempt %d", attempt)
                await self._mqtt_client.connect(
                    clean_start=False,
                    session_expiry=self._session_expiry,
                )
                LOGGER.debug("MQTT connection attempt %d succeeded", attempt)
                return True

            except Exception as exc:
                # If the broker rejected us with an auth-failure code the
                # token is likely expired.  Refresh it *before* the back-off
                # sleep so the next attempt uses fresh credentials.  Without
                # this the agent loops forever with a stale token because the
                # auth-retry inside MQTTClient.connect() is bypassed when
                # _connect_with_backoff() catches the exception.
                if self._mqtt_client.last_connect_rc in (5, 134, 135):
                    try:
                        LOGGER.info(
                            "Auth failure during reconnection (rc=%d), refreshing JWT token",
                            self._mqtt_client.last_connect_rc,
                        )
                        await self._token_manager.refresh_token_now()
                    except Exception as refresh_exc:
                        LOGGER.error(
                            "Failed to refresh token during reconnection: %s",
                            refresh_exc,
                        )

                if in_perpetual:
                    sleep_for = perpetual_delay
                    if jitter_ratio > 0.0:
                        floor = min(
                            perpetual_delay,
                            max(0.1, perpetual_delay * (1.0 - jitter_ratio)),
                        )
                        sleep_for = random.uniform(floor, perpetual_delay)
                    LOGGER.warning(
                        "Connection attempt %d failed: %s, perpetual retry in %.0fs",
                        attempt,
                        exc,
                        sleep_for,
                    )
                else:
                    LOGGER.warning(
                        "Connection attempt %d failed: %s, retrying in %.1fs",
                        attempt,
                        exc,
                        delay,
                    )

                    # Apply "full jitter" (AWS recommended) to de-synchronise
                    # large fleets reconnecting after a broker restart:
                    # sleep ~ U(floor, current_backoff). Picking from the full
                    # [0, delay] range (rather than symmetric +/- around delay)
                    # spreads retries far more evenly than narrow symmetric
                    # jitter, avoiding thundering-herd reconnect storms.
                    sleep_for = delay
                    if jitter_ratio > 0.0:
                        floor = min(delay, max(0.1, delay * (1.0 - jitter_ratio)))
                        sleep_for = random.uniform(floor, delay)

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=sleep_for,
                    )
                    # Stop event was set
                    break
                except asyncio.TimeoutError:
                    pass

                if not in_perpetual:
                    # Exponential backoff (only during initial phase)
                    delay = min(delay * 2, max_delay)

                    if attempt == _BACKOFF_MAX_ATTEMPTS:
                        LOGGER.warning(
                            "Exponential backoff exhausted after %d attempts, "
                            "switching to perpetual retry every %.0fs",
                            attempt,
                            perpetual_delay,
                        )

        return False
