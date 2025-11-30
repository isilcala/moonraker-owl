"""Tests for the event system - event types, EventCollector, and orchestrator event detection."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

from moonraker_owl.telemetry.event_types import (
    Event,
    EventName,
    EventPriority,
    EventSeverity,
    PRINT_STATE_TRANSITIONS,
    generate_event_id,
)
from moonraker_owl.telemetry.events import EventCollector, RateLimitConfig
from moonraker_owl.telemetry.orchestrator import TelemetryOrchestrator


class FakeClock:
    """Test clock that returns a fixed or incrementing time."""

    def __init__(self, start: datetime, step: timedelta = timedelta(seconds=1)) -> None:
        self._current = start
        self._step = step

    def __call__(self) -> datetime:
        value = self._current
        self._current += self._step
        return value


# =============================================================================
# Event Type Tests
# =============================================================================


class TestEventTypes:
    """Tests for Event dataclass and related types."""

    def test_event_creation(self) -> None:
        """Event can be created with required fields."""
        event = Event(
            event_name=EventName.PRINT_STARTED,
            message="Print started: test.gcode",
        )

        assert event.event_name == EventName.PRINT_STARTED
        assert event.message == "Print started: test.gcode"
        assert event.event_id is not None
        assert len(event.event_id) == 36  # UUID format
        assert event.occurred_at is not None
        assert event.session_id is None
        assert event.data == {}

    def test_event_with_all_fields(self) -> None:
        """Event can be created with all optional fields."""
        occurred_at = datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc)
        event = Event(
            event_name=EventName.PRINT_COMPLETED,
            message="Print completed: benchy.gcode",
            occurred_at=occurred_at,
            event_id="custom-id-12345",
            session_id="history-1234",
            data={"filename": "benchy.gcode", "printDuration": 3600},
        )

        assert event.event_id == "custom-id-12345"
        assert event.occurred_at == occurred_at
        assert event.session_id == "history-1234"
        assert event.data["filename"] == "benchy.gcode"

    def test_event_priority_property(self) -> None:
        """Event.priority returns correct priority from metadata."""
        p0_event = Event(event_name=EventName.KLIPPY_SHUTDOWN, message="test")
        p1_event = Event(event_name=EventName.PRINT_STARTED, message="test")

        assert p0_event.priority == EventPriority.P0_CRITICAL
        assert p1_event.priority == EventPriority.P1_IMPORTANT

    def test_event_severity_property(self) -> None:
        """Event.severity returns correct severity from metadata."""
        critical = Event(event_name=EventName.KLIPPY_ERROR, message="test")
        error = Event(event_name=EventName.PRINT_FAILED, message="test")
        warning = Event(event_name=EventName.PRINT_CANCELLED, message="test")
        info = Event(event_name=EventName.PRINT_STARTED, message="test")

        assert critical.severity == EventSeverity.CRITICAL
        assert error.severity == EventSeverity.ERROR
        assert warning.severity == EventSeverity.WARNING
        assert info.severity == EventSeverity.INFO

    def test_event_qos_property(self) -> None:
        """Event.qos returns correct QoS from metadata."""
        p0_event = Event(event_name=EventName.KLIPPY_SHUTDOWN, message="test")
        p1_event = Event(event_name=EventName.PRINT_STARTED, message="test")

        assert p0_event.qos == 2  # P0 uses QoS 2
        assert p1_event.qos == 1  # P1 uses QoS 1

    def test_event_is_critical_property(self) -> None:
        """Event.is_critical returns True for P0 events."""
        p0_event = Event(event_name=EventName.KLIPPY_ERROR, message="test")
        p1_event = Event(event_name=EventName.PRINT_STARTED, message="test")

        assert p0_event.is_critical is True
        assert p1_event.is_critical is False

    def test_event_to_dict(self) -> None:
        """Event.to_dict() produces correct JSON-serializable dictionary."""
        occurred_at = datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc)
        event = Event(
            event_name=EventName.PRINT_STARTED,
            message="Print started: test.gcode",
            occurred_at=occurred_at,
            event_id="test-event-id",
            session_id="history-123",
            data={"filename": "test.gcode"},
        )

        result = event.to_dict()

        assert result["eventId"] == "test-event-id"
        assert result["eventName"] == "printStarted"
        assert result["severity"] == "info"
        assert result["message"] == "Print started: test.gcode"
        assert result["occurredAtUtc"] == "2025-11-30T10:00:00Z"
        assert result["sessionId"] == "history-123"
        assert result["data"]["filename"] == "test.gcode"

    def test_event_to_dict_without_optional_fields(self) -> None:
        """Event.to_dict() omits None sessionId and empty data."""
        event = Event(
            event_name=EventName.KLIPPY_SHUTDOWN,
            message="Klippy shutdown",
            event_id="test-id",
        )

        result = event.to_dict()

        assert "sessionId" not in result
        assert "data" not in result

    def test_generate_event_id_format(self) -> None:
        """generate_event_id() returns valid UUID format."""
        event_id = generate_event_id()

        # Should be 36 characters: 8-4-4-4-12
        assert len(event_id) == 36
        parts = event_id.split("-")
        assert len(parts) == 5
        assert len(parts[0]) == 8
        assert len(parts[1]) == 4
        assert len(parts[2]) == 4
        assert len(parts[3]) == 4
        assert len(parts[4]) == 12

    def test_print_state_transitions_mapping(self) -> None:
        """PRINT_STATE_TRANSITIONS maps state transitions to correct events."""
        assert PRINT_STATE_TRANSITIONS[("standby", "printing")] == EventName.PRINT_STARTED
        assert PRINT_STATE_TRANSITIONS[(None, "printing")] == EventName.PRINT_STARTED
        assert PRINT_STATE_TRANSITIONS[("paused", "printing")] == EventName.PRINT_RESUMED
        assert PRINT_STATE_TRANSITIONS[("printing", "paused")] == EventName.PRINT_PAUSED
        assert PRINT_STATE_TRANSITIONS[("printing", "complete")] == EventName.PRINT_COMPLETED
        assert PRINT_STATE_TRANSITIONS[("printing", "cancelled")] == EventName.PRINT_CANCELLED
        assert PRINT_STATE_TRANSITIONS[("printing", "error")] == EventName.PRINT_FAILED
        assert PRINT_STATE_TRANSITIONS[("paused", "cancelled")] == EventName.PRINT_CANCELLED
        assert PRINT_STATE_TRANSITIONS[("paused", "error")] == EventName.PRINT_FAILED


# =============================================================================
# EventCollector Tests
# =============================================================================


class TestEventCollector:
    """Tests for EventCollector priority queues and rate limiting."""

    def test_record_p0_event(self) -> None:
        """P0 events go to the critical queue."""
        collector = EventCollector()
        event = Event(event_name=EventName.KLIPPY_SHUTDOWN, message="test")

        collector.record(event)

        assert collector.p0_queue_size == 1
        assert collector.p1_queue_size == 0

    def test_record_p1_event(self) -> None:
        """P1 events go to the important queue."""
        collector = EventCollector()
        event = Event(event_name=EventName.PRINT_STARTED, message="test")

        collector.record(event)

        assert collector.p0_queue_size == 0
        assert collector.p1_queue_size == 1

    def test_harvest_p0_first(self) -> None:
        """P0 events are harvested before P1 events."""
        collector = EventCollector()

        # Add P1 first
        p1_event = Event(event_name=EventName.PRINT_STARTED, message="p1")
        collector.record(p1_event)

        # Add P0 second
        p0_event = Event(event_name=EventName.KLIPPY_SHUTDOWN, message="p0")
        collector.record(p0_event)

        events = collector.harvest()

        assert len(events) == 2
        assert events[0].event_name == EventName.KLIPPY_SHUTDOWN  # P0 first
        assert events[1].event_name == EventName.PRINT_STARTED  # P1 second

    def test_harvest_empties_queues(self) -> None:
        """harvest() removes events from queues."""
        collector = EventCollector()
        collector.record(Event(event_name=EventName.KLIPPY_SHUTDOWN, message="p0"))
        collector.record(Event(event_name=EventName.PRINT_STARTED, message="p1"))

        events = collector.harvest()

        assert len(events) == 2
        assert collector.p0_queue_size == 0
        assert collector.p1_queue_size == 0

    def test_harvest_rate_limits_p1(self) -> None:
        """P1 events are subject to rate limiting."""
        config = RateLimitConfig(max_per_second=1.0, max_per_minute=2, burst_size=1)
        collector = EventCollector(rate_config=config)

        # Add more events than burst allows
        for i in range(5):
            collector.record(Event(event_name=EventName.PRINT_STARTED, message=f"p1-{i}"))

        # First harvest should get burst_size events
        events = collector.harvest()
        assert len(events) == 1

        # Remaining events should be queued
        assert collector.p1_queue_size == 4

    def test_harvest_p0_bypasses_rate_limit(self) -> None:
        """P0 events bypass rate limiting."""
        config = RateLimitConfig(max_per_second=0.1, max_per_minute=1, burst_size=0)
        collector = EventCollector(rate_config=config)

        # Add multiple P0 events
        for i in range(10):
            collector.record(Event(event_name=EventName.KLIPPY_SHUTDOWN, message=f"p0-{i}"))

        # All P0 events should be harvested
        events = collector.harvest()
        assert len(events) == 10

    def test_harvest_mixed_events(self) -> None:
        """Mixed P0 and P1 events are harvested correctly."""
        config = RateLimitConfig(max_per_second=10.0, max_per_minute=100, burst_size=5)
        collector = EventCollector(rate_config=config)

        # Add mix of events
        collector.record(Event(event_name=EventName.PRINT_STARTED, message="p1-1"))
        collector.record(Event(event_name=EventName.KLIPPY_SHUTDOWN, message="p0-1"))
        collector.record(Event(event_name=EventName.PRINT_PAUSED, message="p1-2"))
        collector.record(Event(event_name=EventName.KLIPPY_ERROR, message="p0-2"))

        events = collector.harvest()

        assert len(events) == 4
        # P0 events first (in order)
        assert events[0].event_name == EventName.KLIPPY_SHUTDOWN
        assert events[1].event_name == EventName.KLIPPY_ERROR
        # P1 events second (in order)
        assert events[2].event_name == EventName.PRINT_STARTED
        assert events[3].event_name == EventName.PRINT_PAUSED

    def test_reset_clears_all(self) -> None:
        """reset() clears all queues and state."""
        collector = EventCollector()
        collector.record(Event(event_name=EventName.KLIPPY_SHUTDOWN, message="p0"))
        collector.record(Event(event_name=EventName.PRINT_STARTED, message="p1"))
        collector.record_command_state(
            command_id="cmd-1", command_type="pause", state="dispatched"
        )

        collector.reset()

        assert collector.p0_queue_size == 0
        assert collector.p1_queue_size == 0
        assert collector.pending_count == 0

    def test_legacy_record_command_state(self) -> None:
        """record_command_state() adds to legacy pending list."""
        collector = EventCollector()

        collector.record_command_state(
            command_id="cmd-123",
            command_type="pause",
            state="completed",
            session_id="history-456",
            details={"reason": "user_request"},
        )

        events = collector.drain()

        assert len(events) == 1
        assert events[0]["eventName"] == "commandStateChanged"
        assert events[0]["data"]["commandId"] == "cmd-123"
        assert events[0]["data"]["commandType"] == "pause"
        assert events[0]["data"]["state"] == "completed"
        assert events[0]["sessionId"] == "history-456"
        assert events[0]["data"]["reason"] == "user_request"

    def test_pending_count_includes_all_queues(self) -> None:
        """pending_count includes P0, P1, and legacy events."""
        collector = EventCollector()
        collector.record(Event(event_name=EventName.KLIPPY_SHUTDOWN, message="p0"))
        collector.record(Event(event_name=EventName.PRINT_STARTED, message="p1"))
        collector.record_command_state(
            command_id="cmd-1", command_type="pause", state="dispatched"
        )

        assert collector.pending_count == 3

    def test_max_queue_size(self) -> None:
        """Queues have a maximum size."""
        collector = EventCollector(max_queue_size=3)

        # Add more than max
        for i in range(5):
            collector.record(Event(event_name=EventName.PRINT_STARTED, message=f"p1-{i}"))

        # Only newest 3 should remain
        assert collector.p1_queue_size == 3


# =============================================================================
# Orchestrator Event Detection Tests
# =============================================================================


class TestOrchestratorEventDetection:
    """Tests for TelemetryOrchestrator state change detection."""

    def test_detect_klippy_error(self) -> None:
        """Klippy error state generates klippyError event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set ready state first
        orchestrator.ingest(
            {"result": {"status": {"webhooks": {"state": "ready"}}}}
        )
        orchestrator.events.harvest()  # Clear any initial events

        # Transition to error
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "webhooks": {
                            "state": "error",
                            "state_message": "MCU protocol error",
                        }
                    }
                }
            }
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.KLIPPY_ERROR
        assert events[0].is_critical
        assert "MCU protocol error" in events[0].message
        assert events[0].data["stateMessage"] == "MCU protocol error"

    def test_detect_klippy_shutdown(self) -> None:
        """Klippy shutdown state generates klippyShutdown event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set ready state first
        orchestrator.ingest(
            {"result": {"status": {"webhooks": {"state": "ready"}}}}
        )
        orchestrator.events.harvest()

        # Transition to shutdown
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "webhooks": {
                            "state": "shutdown",
                            "state_message": "Heater heater_bed not heating",
                        }
                    }
                }
            }
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.KLIPPY_SHUTDOWN
        assert "Heater heater_bed" in events[0].data["stateMessage"]

    def test_detect_klippy_disconnected(self) -> None:
        """Klippy disconnect generates klippyDisconnected event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set ready state first
        orchestrator.ingest(
            {"result": {"status": {"webhooks": {"state": "ready"}}}}
        )
        orchestrator.events.harvest()

        # Transition to startup (disconnected)
        orchestrator.ingest(
            {"result": {"status": {"webhooks": {"state": "startup"}}}}
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.KLIPPY_DISCONNECTED
        assert events[0].data["previousState"] == "ready"

    def test_detect_print_started(self) -> None:
        """Print standby -> printing generates printStarted event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set standby state
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "standby"}}}}
        )
        orchestrator.events.harvest()

        # Transition to printing
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "printing",
                            "filename": "benchy.gcode",
                        }
                    }
                }
            }
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_STARTED
        assert "benchy.gcode" in events[0].message
        assert events[0].data["filename"] == "benchy.gcode"

    def test_detect_print_paused(self) -> None:
        """Print printing -> paused generates printPaused event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set printing state
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "printing",
                            "filename": "test.gcode",
                        }
                    }
                }
            }
        )
        orchestrator.events.harvest()

        # Transition to paused
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "paused"}}}}
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_PAUSED

    def test_detect_print_resumed(self) -> None:
        """Print paused -> printing generates printResumed event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set paused state
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "paused",
                            "filename": "test.gcode",
                        }
                    }
                }
            }
        )
        orchestrator.events.harvest()

        # Transition to printing
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "printing"}}}}
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_RESUMED

    def test_detect_print_completed(self) -> None:
        """Print printing -> complete generates printCompleted event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set printing state
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "printing",
                            "filename": "test.gcode",
                            "print_duration": 3600,
                        }
                    }
                }
            }
        )
        orchestrator.events.harvest()

        # Transition to complete
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "complete",
                            "print_duration": 3650,
                            "filament_used": 1234.5,
                        }
                    }
                }
            }
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_COMPLETED

    def test_detect_print_completed_alternative_spelling(self) -> None:
        """Print printing -> completed (with 'ed') generates printCompleted event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set printing state
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "printing",
                            "filename": "test.gcode",
                        }
                    }
                }
            }
        )
        orchestrator.events.harvest()

        # Transition to completed (alternative spelling)
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "completed"}}}}
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_COMPLETED

    def test_detect_print_cancelled(self) -> None:
        """Print printing -> cancelled generates printCancelled event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set printing state
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "printing",
                            "filename": "test.gcode",
                        }
                    }
                }
            }
        )
        orchestrator.events.harvest()

        # Transition to cancelled
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "cancelled"}}}}
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_CANCELLED

    def test_detect_print_failed(self) -> None:
        """Print printing -> error generates printFailed event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set printing state
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "printing",
                            "filename": "test.gcode",
                        }
                    }
                }
            }
        )
        orchestrator.events.harvest()

        # Transition to error
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "error",
                            "message": "Thermal runaway",
                        }
                    }
                }
            }
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_FAILED

    def test_no_event_on_same_state(self) -> None:
        """No event when state doesn't change."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set printing state
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "printing"}}}}
        )
        orchestrator.events.harvest()

        # Same state again
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "printing"}}}}
        )

        events = orchestrator.events.harvest()
        assert len(events) == 0

    def test_no_event_for_unmapped_transition(self) -> None:
        """No event for state transitions not in mapping."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set complete state
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "complete"}}}}
        )
        orchestrator.events.harvest()

        # Transition to standby (not a significant event)
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "standby"}}}}
        )

        events = orchestrator.events.harvest()
        assert len(events) == 0

    def test_detect_print_started_from_complete(self) -> None:
        """Print complete -> printing generates printStarted event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set complete state (previous print finished)
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "complete"}}}}
        )
        orchestrator.events.harvest()

        # Start new print
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "printing",
                            "filename": "new_print.gcode",
                        }
                    }
                }
            }
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_STARTED
        assert "new_print.gcode" in events[0].message

    def test_detect_print_started_from_cancelled(self) -> None:
        """Print cancelled -> printing generates printStarted event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set cancelled state
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "cancelled"}}}}
        )
        orchestrator.events.harvest()

        # Start new print
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "printing",
                            "filename": "retry.gcode",
                        }
                    }
                }
            }
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_STARTED

    def test_detect_print_started_from_error(self) -> None:
        """Print error -> printing generates printStarted event."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set error state
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "error"}}}}
        )
        orchestrator.events.harvest()

        # Start new print after recovery
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "printing",
                            "filename": "recovered.gcode",
                        }
                    }
                }
            }
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_STARTED

    def test_detect_print_completed_from_paused(self) -> None:
        """Edge case: paused -> complete generates printCompleted event.

        This is an unusual transition but can happen if firmware auto-completes
        a paused print. We handle it defensively.
        """
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set paused state
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "paused",
                            "filename": "test.gcode",
                        }
                    }
                }
            }
        )
        orchestrator.events.harvest()

        # Directly complete from paused (edge case)
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "complete"}}}}
        )

        events = orchestrator.events.harvest()
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_COMPLETED

    def test_print_state_callback_invoked(self) -> None:
        """Print state callback is invoked on state change."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        callback_states: list[str] = []

        def on_state_change(state: str) -> None:
            callback_states.append(state)

        orchestrator.set_print_state_callback(on_state_change)

        # Set printing state
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "standby"}}}}
        )
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "printing"}}}}
        )
        orchestrator.ingest(
            {"result": {"status": {"print_stats": {"state": "paused"}}}}
        )

        assert callback_states == ["standby", "printing", "paused"]

    def test_reset_clears_state_tracking(self) -> None:
        """reset() clears state tracking fields."""
        clock = FakeClock(datetime(2025, 11, 30, 10, 0, 0, tzinfo=timezone.utc))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Set some state
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "webhooks": {"state": "ready"},
                        "print_stats": {
                            "state": "printing",
                            "filename": "test.gcode",
                        },
                    }
                }
            }
        )
        orchestrator.events.harvest()

        # Reset
        orchestrator.reset()

        # After reset, same state should generate new events
        orchestrator.ingest(
            {
                "result": {
                    "status": {
                        "print_stats": {
                            "state": "printing",
                            "filename": "test.gcode",
                        }
                    }
                }
            }
        )

        events = orchestrator.events.harvest()
        # Should get a printStarted because _last_print_state was reset to None
        assert len(events) == 1
        assert events[0].event_name == EventName.PRINT_STARTED
