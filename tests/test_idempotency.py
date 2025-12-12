"""Tests for CommandIdempotencyGuard (ADR-0013 Appendix D)."""

from datetime import datetime, timedelta, timezone

import pytest

from moonraker_owl.core.idempotency import CommandIdempotencyGuard, ProcessedCommand


class TestCommandIdempotencyGuard:
    """Unit tests for the idempotency guard."""

    def test_should_process_new_command(self):
        """New commands should be processed."""
        guard = CommandIdempotencyGuard(ttl_hours=24)

        result = guard.should_process("cmd-001")

        assert result is True

    def test_should_not_process_duplicate(self):
        """Already processed commands should not be reprocessed."""
        guard = CommandIdempotencyGuard(ttl_hours=24)

        # First: mark as processed
        guard.mark_processed("cmd-001", status="completed", stage="execution")

        # Second: should detect duplicate
        result = guard.should_process("cmd-001")

        assert result is False

    def test_get_cached_result_returns_entry(self):
        """Cached results should be retrievable."""
        guard = CommandIdempotencyGuard(ttl_hours=24)

        guard.mark_processed(
            "cmd-001",
            status="completed",
            stage="execution",
            error_code=None,
            error_message=None,
        )

        cached = guard.get_cached_result("cmd-001")

        assert cached is not None
        assert cached.command_id == "cmd-001"
        assert cached.status == "completed"
        assert cached.stage == "execution"

    def test_get_cached_result_returns_none_for_unknown(self):
        """Unknown commands should return None."""
        guard = CommandIdempotencyGuard(ttl_hours=24)

        cached = guard.get_cached_result("unknown-cmd")

        assert cached is None

    def test_failed_command_is_tracked(self):
        """Failed commands should also be tracked for idempotency."""
        guard = CommandIdempotencyGuard(ttl_hours=24)

        guard.mark_processed(
            "cmd-fail",
            status="failed",
            stage="execution",
            error_code="moonraker_error",
            error_message="Printer disconnected",
        )

        cached = guard.get_cached_result("cmd-fail")

        assert cached is not None
        assert cached.status == "failed"
        assert cached.error_code == "moonraker_error"
        assert cached.error_message == "Printer disconnected"

    def test_entry_count(self):
        """Entry count should reflect tracked commands."""
        guard = CommandIdempotencyGuard(ttl_hours=24)

        assert guard.entry_count == 0

        guard.mark_processed("cmd-001", status="completed", stage="execution")
        guard.mark_processed("cmd-002", status="completed", stage="execution")

        assert guard.entry_count == 2

    def test_clear_removes_all_entries(self):
        """Clear should remove all tracked commands."""
        guard = CommandIdempotencyGuard(ttl_hours=24)

        guard.mark_processed("cmd-001", status="completed", stage="execution")
        guard.mark_processed("cmd-002", status="completed", stage="execution")

        guard.clear()

        assert guard.entry_count == 0
        assert guard.should_process("cmd-001") is True


class TestIdempotencyExpiration:
    """Tests for TTL-based expiration."""

    def test_expired_entry_allows_reprocessing(self):
        """Expired entries should allow command reprocessing."""
        # Use a very short TTL for testing
        guard = CommandIdempotencyGuard(ttl_hours=0)  # 0 hours = immediate expiry

        # Manually create an expired entry
        guard._processed["cmd-old"] = ProcessedCommand(
            command_id="cmd-old",
            status="completed",
            stage="execution",
            processed_at=datetime.now(timezone.utc) - timedelta(hours=25),
        )

        # Should be allowed to reprocess
        result = guard.should_process("cmd-old")

        assert result is True

    def test_non_expired_entry_blocks_reprocessing(self):
        """Non-expired entries should block reprocessing."""
        guard = CommandIdempotencyGuard(ttl_hours=24)

        guard.mark_processed("cmd-recent", status="completed", stage="execution")

        result = guard.should_process("cmd-recent")

        assert result is False


class TestIdempotencyCleanup:
    """Tests for cleanup behavior."""

    def test_cleanup_removes_expired_entries(self):
        """Cleanup should remove expired entries."""
        guard = CommandIdempotencyGuard(ttl_hours=24)

        # Add an old entry manually
        guard._processed["cmd-old"] = ProcessedCommand(
            command_id="cmd-old",
            status="completed",
            stage="execution",
            processed_at=datetime.now(timezone.utc) - timedelta(hours=25),
        )

        # Add a recent entry
        guard.mark_processed("cmd-recent", status="completed", stage="execution")

        # Trigger cleanup
        guard._cleanup_expired()

        # Old entry should be gone
        assert "cmd-old" not in guard._processed
        # Recent entry should remain
        assert "cmd-recent" in guard._processed

    def test_max_entries_forces_cleanup(self):
        """Exceeding max entries should trigger forced cleanup."""
        guard = CommandIdempotencyGuard(ttl_hours=24, max_entries=5, cleanup_interval=1)

        # Add entries up to max
        for i in range(10):
            guard.mark_processed(f"cmd-{i:03d}", status="completed", stage="execution")

        # Should have been cleaned up
        assert guard.entry_count <= 5
