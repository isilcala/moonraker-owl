"""Telemetry event collection with priority queues and rate limiting.

This module provides the EventCollector class which manages events with:
- Priority queues: P0 (critical) and P1 (important) business events
- Rate limiting: Token bucket algorithm for P1 events
- System events: Command acknowledgement events (no rate limiting)

Event Sources and Flow:
-----------------------
1. P0/P1 Business Events (printStarted, printPaused, klippyError, etc.):
   - Recorded via record(Event) into priority queues (_p0_queue, _p1_queue)
   - Retrieved via harvest() with token bucket rate limiting for P1
   - P0 events bypass rate limiting entirely
   - These are user-facing business events

2. System Events (commandStateChanged):
   - Recorded via record_command_state() into _pending list
   - Retrieved via drain() without rate limiting
   - These are Owl-specific system events for UI feedback (spinner, ack)
   - Command frequency is naturally limited by user interaction

Design Decision: Both event types share the same MQTT topic (events channel)
but use different collection mechanisms due to their different rate limiting
requirements. Consumers distinguish them by the eventName field.

Note: The events channel bypasses the cadence controller entirely.
Rate limiting is handled here in EventCollector, not at publish time.

See ADR-0009 and event_types.py for event definitions.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional

from .event_types import Event, EventPriority

LOGGER = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Configuration for event rate limiting.

    Attributes:
        max_per_second: Maximum events per second (token refill rate)
        max_per_minute: Hard cap on events per minute
        burst_size: Maximum tokens in bucket (allows bursts)
    """

    max_per_second: float = 2.0
    max_per_minute: int = 30
    burst_size: int = 10


class EventCollector:
    """Collects events with priority queues and rate limiting.

    This class manages two priority queues:
    - P0 (critical): Infrastructure failures, bypass rate limiting
    - P1 (important): Print lifecycle events, subject to rate limiting

    P0 events are always harvested immediately as they indicate critical
    issues like Klippy shutdown or errors.

    P1 events use a token bucket algorithm for rate limiting to prevent
    overwhelming the cloud service during rapid state changes.

    The collector also maintains backward compatibility with the legacy
    command state event interface.
    """

    def __init__(
        self,
        rate_config: Optional[RateLimitConfig] = None,
        max_queue_size: int = 100,
    ) -> None:
        """Initialize the event collector.

        Args:
            rate_config: Rate limiting configuration. Uses defaults if None.
            max_queue_size: Maximum size of each priority queue.
        """
        self._config = rate_config or RateLimitConfig()
        self._max_queue_size = max_queue_size

        # Priority queues - deque with maxlen prevents unbounded growth
        self._p0_queue: Deque[Event] = deque(maxlen=max_queue_size)
        self._p1_queue: Deque[Event] = deque(maxlen=max_queue_size)

        # Legacy pending list for backward compatibility
        self._pending: List[Dict[str, Any]] = []

        # Token bucket rate limiting state
        self._tokens = float(self._config.burst_size)
        self._last_refill = time.monotonic()
        self._minute_count = 0
        self._minute_start = time.monotonic()

    def record(self, event: Event) -> None:
        """Record an event into the appropriate priority queue.

        P0 events go to the critical queue and bypass rate limiting.
        P1 events go to the important queue and are rate limited.

        Args:
            event: The event to record.
        """
        if event.priority == EventPriority.P0_CRITICAL:
            self._p0_queue.append(event)
            LOGGER.info(
                "Recorded P0 event: %s (id=%s)",
                event.event_name.value,
                event.event_id[:8],
            )
        else:
            self._p1_queue.append(event)
            LOGGER.info(
                "EVENT_RECORDED: %s (id=%s, session=%s, msg='%s')",
                event.event_name.value,
                event.event_id[:8],
                event.session_id[:8] if event.session_id else "none",
                event.message[:50] if event.message else "",
            )

    def harvest(self) -> List[Event]:
        """Harvest events for publishing, respecting rate limits.

        P0 events bypass rate limiting and are always returned.
        P1 events are subject to rate limiting via token bucket.

        Returns:
            List of events to publish, ordered by priority then time.
        """
        events: List[Event] = []

        # P0 events always go through - drain entire queue
        while self._p0_queue:
            events.append(self._p0_queue.popleft())

        # P1 events subject to rate limiting
        self._refill_tokens()

        while self._p1_queue and self._try_acquire_token():
            events.append(self._p1_queue.popleft())

        if events:
            event_names = [e.event_name.value for e in events]
            LOGGER.debug(
                "EVENTS_HARVESTED: %d events [%s]",
                len(events),
                ", ".join(event_names),
            )

        return events

    def record_command_state(
        self,
        *,
        command_id: str,
        command_type: str,
        state: str,
        occurred_at: Optional[datetime] = None,
        session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a command state change event (system event).

        Command ack events are Owl-specific system events used for UI feedback
        (showing spinners, confirming command execution). They are NOT business
        events and don't need rate limiting - command frequency is naturally
        limited by user interaction.

        These events use a separate collection path (drain()) from business
        events (harvest()) to avoid interfering with the token bucket rate
        limiting used for P0/P1 events.

        Args:
            command_id: Unique identifier of the command
            command_type: Type of command (e.g., "pause", "resume")
            state: New state of the command (dispatched, accepted, completed, failed)
            occurred_at: When the state change occurred
            session_id: Associated print session ID
            details: Additional details about the state change
        """
        occurred = occurred_at or datetime.now(timezone.utc)
        payload: Dict[str, Any] = {
            "eventName": "commandStateChanged",
            "severity": "info",
            "occurredAtUtc": occurred.replace(microsecond=0).isoformat(),
            "data": {
                "commandId": command_id,
                "commandType": command_type,
                "state": state,
            },
        }
        if session_id:
            payload["sessionId"] = session_id
        if details:
            payload["data"].update(details)
        self._pending.append(payload)

    def drain(self) -> List[Dict[str, Any]]:
        """Drain command ack events (system events).

        These events don't go through rate limiting because command frequency
        is naturally limited by user interaction.

        Returns:
            List of command state event dictionaries.
        """
        events = list(self._pending)
        self._pending.clear()
        return events

    def reset(self) -> None:
        """Reset all queues and rate limiting state."""
        self._p0_queue.clear()
        self._p1_queue.clear()
        self._pending.clear()
        self._tokens = float(self._config.burst_size)
        self._minute_count = 0
        self._minute_start = time.monotonic()

    @property
    def pending_count(self) -> int:
        """Get total number of pending events across all queues."""
        return len(self._p0_queue) + len(self._p1_queue) + len(self._pending)

    @property
    def p0_queue_size(self) -> int:
        """Get number of P0 events waiting."""
        return len(self._p0_queue)

    @property
    def p1_queue_size(self) -> int:
        """Get number of P1 events waiting."""
        return len(self._p1_queue)

    def _refill_tokens(self) -> None:
        """Refill rate limit tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now

        # Refill tokens based on rate
        self._tokens = min(
            float(self._config.burst_size),
            self._tokens + elapsed * self._config.max_per_second,
        )

        # Reset minute counter if needed
        if now - self._minute_start >= 60:
            self._minute_count = 0
            self._minute_start = now

    def _try_acquire_token(self) -> bool:
        """Try to acquire a rate limit token.

        Returns:
            True if a token was acquired, False otherwise.
        """
        # Check minute limit
        if self._minute_count >= self._config.max_per_minute:
            return False

        # Check token bucket
        if self._tokens >= 1:
            self._tokens -= 1
            self._minute_count += 1
            return True

        return False
