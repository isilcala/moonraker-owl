"""Tests for the cadence control module.

These tests verify that the ChannelCadenceController correctly separates
the concerns of:
1. Force-publish (liveness guarantee) - forces publish without changing mode
2. Watch window expiration (mode control) - handled by TelemetryPublisher
"""

import pytest

from moonraker_owl.telemetry.cadence import (
    ChannelCadenceController,
    ChannelSchedule,
    TelemetryCadenceError,
)


class FakeHasher:
    """Hasher that returns predictable hashes for testing."""

    def __init__(self, default_hash: str = "hash-1"):
        self.default_hash = default_hash
        self.next_hash: str | None = None

    def hash_payload(self, payload: dict) -> str:
        if self.next_hash:
            result = self.next_hash
            self.next_hash = None
            return result
        return self.default_hash


class TestChannelCadenceController:
    """Tests for ChannelCadenceController."""

    def test_force_publish_triggers_on_elapsed_time(self):
        """Force-publish should trigger when force_publish_seconds elapses."""
        current_time = 1000.0
        controller = ChannelCadenceController(
            monotonic=lambda: current_time,
            hasher=FakeHasher(),
        )
        controller.configure(
            "sensors",
            interval=1.0,
            forced_interval=1.0,
            force_publish_seconds=300.0,
        )

        # First publish - should always publish
        payload = {"value": 1}
        decision = controller.evaluate(
            "sensors",
            payload,
            allow_force_publish=True,
        )
        assert decision.should_publish is True

        # Second publish with same payload after interval - should be deduped
        current_time = 1002.0  # Past the 1s interval
        controller._monotonic = lambda: current_time
        decision = controller.evaluate(
            "sensors",
            payload,
            allow_force_publish=True,
        )
        assert decision.should_publish is False
        assert decision.reason == "dedup"

        # Fast forward past force_publish_seconds
        current_time = 1000.0 + 301.0
        controller._monotonic = lambda: current_time

        # Same payload but force_publish should trigger
        decision = controller.evaluate(
            "sensors",
            payload,
            allow_force_publish=True,
        )
        assert decision.should_publish is True
        assert decision.reason == "ready"  # Published successfully

    def test_force_publish_does_not_change_schedule(self):
        """Force-publish should NOT modify the schedule configuration."""
        current_time = 1000.0
        controller = ChannelCadenceController(
            monotonic=lambda: current_time,
            hasher=FakeHasher(),
        )
        controller.configure(
            "sensors",
            interval=30.0,
            forced_interval=1.0,
            force_publish_seconds=300.0,
        )

        # Get original schedule
        original_schedule = controller.get_schedule("sensors")
        assert original_schedule.interval == 30.0
        assert original_schedule.force_publish_seconds == 300.0

        # Trigger force-publish
        payload = {"value": 1}
        controller.evaluate("sensors", payload, allow_force_publish=True)

        current_time = 1000.0 + 301.0
        controller._monotonic = lambda: current_time
        controller.evaluate("sensors", payload, allow_force_publish=True)

        # Schedule should remain unchanged
        schedule_after = controller.get_schedule("sensors")
        assert schedule_after.interval == 30.0
        assert schedule_after.force_publish_seconds == 300.0
        assert schedule_after == original_schedule

    def test_force_publish_independent_of_interval(self):
        """Force-publish timer is independent of regular interval enforcement."""
        current_time = 1000.0
        hasher = FakeHasher()
        controller = ChannelCadenceController(
            monotonic=lambda: current_time,
            hasher=hasher,
        )
        controller.configure(
            "sensors",
            interval=10.0,  # Regular publish every 10s
            forced_interval=None,
            force_publish_seconds=300.0,  # Force publish every 300s
        )

        # First publish
        hasher.next_hash = "hash-1"
        decision = controller.evaluate("sensors", {"v": 1}, allow_force_publish=True)
        assert decision.should_publish is True

        # After 5 seconds with different payload - blocked by interval
        current_time = 1005.0
        controller._monotonic = lambda: current_time
        hasher.next_hash = "hash-2"
        decision = controller.evaluate("sensors", {"v": 2}, allow_force_publish=True)
        assert decision.should_publish is False
        assert decision.reason == "cadence"

        # After 10 seconds with different payload - allowed
        current_time = 1010.0
        controller._monotonic = lambda: current_time
        hasher.next_hash = "hash-3"
        decision = controller.evaluate("sensors", {"v": 3}, allow_force_publish=True)
        assert decision.should_publish is True

    def test_force_publish_respects_allow_flag(self):
        """Force-publish only triggers when allow_force_publish=True."""
        current_time = 1000.0
        controller = ChannelCadenceController(
            monotonic=lambda: current_time,
            hasher=FakeHasher(),
        )
        controller.configure(
            "sensors",
            interval=1.0,
            force_publish_seconds=10.0,
        )

        payload = {"value": 1}
        controller.evaluate("sensors", payload, allow_force_publish=True)

        # Fast forward past force_publish_seconds
        current_time = 1011.0
        controller._monotonic = lambda: current_time

        # Without allow_force_publish, should be deduped
        decision = controller.evaluate(
            "sensors",
            payload,
            allow_force_publish=False,
        )
        assert decision.should_publish is False
        assert decision.reason == "dedup"

        # With allow_force_publish, should publish
        decision = controller.evaluate(
            "sensors",
            payload,
            allow_force_publish=True,
        )
        assert decision.should_publish is True

    def test_reset_channel_clears_force_publish_timer(self):
        """Resetting a channel should clear its force-publish timing state."""
        current_time = 1000.0
        controller = ChannelCadenceController(
            monotonic=lambda: current_time,
            hasher=FakeHasher(),
        )
        controller.configure(
            "sensors",
            interval=1.0,
            force_publish_seconds=300.0,
        )

        # First publish
        payload = {"value": 1}
        decision = controller.evaluate("sensors", payload, allow_force_publish=True)
        assert decision.should_publish is True

        # Reset the channel
        controller.reset_channel("sensors")

        # Same payload should publish again (no last_publish time)
        decision = controller.evaluate("sensors", payload, allow_force_publish=True)
        assert decision.should_publish is True

    def test_unconfigured_channel_raises_error(self):
        """Evaluating an unconfigured channel should raise TelemetryCadenceError."""
        controller = ChannelCadenceController()

        with pytest.raises(TelemetryCadenceError, match="not configured"):
            controller.evaluate("unknown", {"value": 1})

        with pytest.raises(TelemetryCadenceError, match="not configured"):
            controller.get_schedule("unknown")


class TestChannelSchedule:
    """Tests for ChannelSchedule frozen dataclass."""

    def test_schedule_is_immutable(self):
        """ChannelSchedule should be immutable (frozen dataclass)."""
        schedule = ChannelSchedule(
            interval=1.0,
            forced_interval=0.5,
            force_publish_seconds=300.0,
        )

        with pytest.raises(AttributeError):
            schedule.interval = 2.0  # type: ignore[misc]

    def test_schedule_equality(self):
        """Two schedules with same values should be equal."""
        schedule1 = ChannelSchedule(
            interval=1.0,
            forced_interval=0.5,
            force_publish_seconds=300.0,
        )
        schedule2 = ChannelSchedule(
            interval=1.0,
            forced_interval=0.5,
            force_publish_seconds=300.0,
        )
        assert schedule1 == schedule2
