"""Telemetry event collection with priority queues and rate limiting.

This module provides the EventCollector class which manages events with:
- Priority queues: P0 (critical) and P1 (important) events
- Rate limiting: Token bucket algorithm for P1 events
- Backward compatibility: Legacy command state events

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
            LOGGER.debug(
                "Recorded P1 event: %s (id=%s)",
                event.event_name.value,
                event.event_id[:8],
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
            p0_count = sum(1 for e in events if e.is_critical)
            p1_count = len(events) - p0_count
            LOGGER.debug(
                "Harvested %d events (P0=%d, P1=%d), %d P1 queued",
                len(events),
                p0_count,
                p1_count,
                len(self._p1_queue),
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
        """Record a command state change event (legacy interface).

        This method maintains backward compatibility with existing code
        that records command state changes. These events go to the legacy
        pending list and are drained separately.

        Args:
            command_id: Unique identifier of the command
            command_type: Type of command (e.g., "pause", "resume")
            state: New state of the command
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
        """Drain legacy pending events (backward compatibility).

        Returns:
            List of legacy event dictionaries.
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
        LOGGER.debug("Event collector reset")

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
