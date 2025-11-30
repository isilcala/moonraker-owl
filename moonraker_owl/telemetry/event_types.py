"""Event type definitions for the telemetry pipeline.

This module defines the event data model used by the Owl agent to report
significant state changes to the cloud. Events are categorized by priority:

- P0 (Critical): Infrastructure failures requiring immediate attention
- P1 (Important): Print lifecycle events for user awareness

See ADR-0009 for design rationale.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


class EventPriority(Enum):
    """Event priority levels.

    P0 events bypass rate limiting and use QoS 2 for guaranteed delivery.
    P1 events are subject to rate limiting and use QoS 1.
    """

    P0_CRITICAL = 0  # Infrastructure failures
    P1_IMPORTANT = 1  # Print lifecycle


class EventSeverity(str, Enum):
    """Event severity levels for UI display."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class EventName(str, Enum):
    """Standard event names.

    P0 - Infrastructure Events:
        klippyError: Klippy entered error state
        klippyShutdown: Klippy shutdown (heater fault, probe failure, etc.)
        klippyDisconnected: Klippy disconnected from Moonraker

    P1 - Print Lifecycle Events:
        printStarted: A print job has started
        printCompleted: A print job completed successfully
        printFailed: A print job failed due to an error
        printCancelled: A print job was cancelled
        printPaused: A print job was paused
        printResumed: A print job was resumed from pause
    """

    # P0 - Infrastructure
    KLIPPY_ERROR = "klippyError"
    KLIPPY_SHUTDOWN = "klippyShutdown"
    KLIPPY_DISCONNECTED = "klippyDisconnected"

    # P1 - Print Lifecycle
    PRINT_STARTED = "printStarted"
    PRINT_COMPLETED = "printCompleted"
    PRINT_FAILED = "printFailed"
    PRINT_CANCELLED = "printCancelled"
    PRINT_PAUSED = "printPaused"
    PRINT_RESUMED = "printResumed"


# Event metadata registry - maps event names to their properties
EVENT_METADATA: Dict[EventName, Dict[str, Any]] = {
    # P0 - Critical infrastructure events
    EventName.KLIPPY_ERROR: {
        "priority": EventPriority.P0_CRITICAL,
        "severity": EventSeverity.CRITICAL,
        "qos": 2,
    },
    EventName.KLIPPY_SHUTDOWN: {
        "priority": EventPriority.P0_CRITICAL,
        "severity": EventSeverity.CRITICAL,
        "qos": 2,
    },
    EventName.KLIPPY_DISCONNECTED: {
        "priority": EventPriority.P0_CRITICAL,
        "severity": EventSeverity.ERROR,
        "qos": 2,
    },
    # P1 - Print lifecycle events
    EventName.PRINT_STARTED: {
        "priority": EventPriority.P1_IMPORTANT,
        "severity": EventSeverity.INFO,
        "qos": 1,
    },
    EventName.PRINT_COMPLETED: {
        "priority": EventPriority.P1_IMPORTANT,
        "severity": EventSeverity.INFO,
        "qos": 1,
    },
    EventName.PRINT_FAILED: {
        "priority": EventPriority.P1_IMPORTANT,
        "severity": EventSeverity.ERROR,
        "qos": 1,
    },
    EventName.PRINT_CANCELLED: {
        "priority": EventPriority.P1_IMPORTANT,
        "severity": EventSeverity.WARNING,
        "qos": 1,
    },
    EventName.PRINT_PAUSED: {
        "priority": EventPriority.P1_IMPORTANT,
        "severity": EventSeverity.INFO,
        "qos": 1,
    },
    EventName.PRINT_RESUMED: {
        "priority": EventPriority.P1_IMPORTANT,
        "severity": EventSeverity.INFO,
        "qos": 1,
    },
}


# Print state transition mappings
# Maps (previous_state, current_state) tuples to EventName
# None as previous_state matches any unknown/initial state
PRINT_STATE_TRANSITIONS: Dict[tuple[Optional[str], str], EventName] = {
    # Starting a print (from various idle states)
    (None, "printing"): EventName.PRINT_STARTED,
    ("standby", "printing"): EventName.PRINT_STARTED,
    ("complete", "printing"): EventName.PRINT_STARTED,  # New print after completion
    ("completed", "printing"): EventName.PRINT_STARTED,  # Alternative spelling
    ("cancelled", "printing"): EventName.PRINT_STARTED,  # New print after cancel
    ("error", "printing"): EventName.PRINT_STARTED,  # New print after error
    # Resuming from pause
    ("paused", "printing"): EventName.PRINT_RESUMED,
    # Pausing
    ("printing", "paused"): EventName.PRINT_PAUSED,
    # Completing (Klipper uses "complete", some versions may use "completed")
    ("printing", "complete"): EventName.PRINT_COMPLETED,
    ("printing", "completed"): EventName.PRINT_COMPLETED,
    # Cancelling
    ("printing", "cancelled"): EventName.PRINT_CANCELLED,
    ("paused", "cancelled"): EventName.PRINT_CANCELLED,
    # Failing
    ("printing", "error"): EventName.PRINT_FAILED,
    ("paused", "error"): EventName.PRINT_FAILED,
}


def generate_event_id() -> str:
    """Generate a UUID v7 event ID.

    UUID v7 provides time-ordered UUIDs which enable:
    - Chronological sorting without additional timestamp field
    - Globally unique identification
    - Compatibility with standard UUID libraries

    Falls back to UUID v4 on Python < 3.12.
    """
    try:
        # Python 3.12+ has native uuid7 support
        return str(uuid.uuid7())  # type: ignore[attr-defined]
    except AttributeError:
        # Fallback for earlier Python versions
        # uuid4 is still globally unique, just not time-ordered
        return str(uuid.uuid4())


@dataclass
class Event:
    """Represents a discrete event to be published.

    Events are immutable records of significant state changes that occurred
    in the printer. They are published to the cloud for user notification
    and historical tracking.

    Attributes:
        event_name: The type of event (from EventName enum)
        message: Human-readable description of the event
        occurred_at: When the event occurred (UTC)
        event_id: Unique identifier (UUID v7)
        session_id: Associated print session ID, if any
        data: Additional event-specific data
    """

    event_name: EventName
    message: str
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_id: str = field(default_factory=generate_event_id)
    session_id: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)

    @property
    def severity(self) -> EventSeverity:
        """Get the severity level for this event type."""
        return EVENT_METADATA[self.event_name]["severity"]

    @property
    def priority(self) -> EventPriority:
        """Get the priority level for this event type."""
        return EVENT_METADATA[self.event_name]["priority"]

    @property
    def qos(self) -> int:
        """Get the MQTT QoS level for this event type."""
        return EVENT_METADATA[self.event_name]["qos"]

    @property
    def is_critical(self) -> bool:
        """Check if this is a P0 critical event."""
        return self.priority == EventPriority.P0_CRITICAL

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Returns a dictionary matching the telemetry events schema.
        """
        result: Dict[str, Any] = {
            "eventId": self.event_id,
            "eventName": self.event_name.value,
            "severity": self.severity.value,
            "message": self.message,
            "occurredAtUtc": self.occurred_at.isoformat().replace("+00:00", "Z"),
        }
        if self.session_id:
            result["sessionId"] = self.session_id
        if self.data:
            result["data"] = self.data
        return result

    def __repr__(self) -> str:
        return (
            f"Event(name={self.event_name.value}, "
            f"severity={self.severity.value}, "
            f"id={self.event_id[:8]}...)"
        )
