"""Unit tests for PrintJobRegistry and job:registered command handling."""

from datetime import datetime, timedelta, timezone

import pytest

from moonraker_owl.core.job_registry import PrintJobRegistry, PrintJobMapping


class TestPrintJobRegistry:
    """Tests for PrintJobRegistry class."""

    def test_register_stores_mapping(self) -> None:
        """Verify registration stores all mapping fields correctly."""
        registry = PrintJobRegistry()
        registry.register(
            moonraker_job_id="0002DD",
            print_job_id="job-123",
            filename="test.gcode",
        )

        mapping = registry.find_by_moonraker_job_id("0002DD")
        assert mapping is not None
        assert mapping.moonraker_job_id == "0002DD"
        assert mapping.print_job_id == "job-123"
        assert mapping.filename == "test.gcode"

    def test_find_by_filename_returns_matching(self) -> None:
        """Verify lookup by filename finds correct mapping."""
        registry = PrintJobRegistry()
        registry.register(
            moonraker_job_id="0001",
            print_job_id="job-A",
            filename="first.gcode",
        )
        registry.register(
            moonraker_job_id="0002",
            print_job_id="job-B",
            filename="second.gcode",
        )

        mapping = registry.find_by_filename("second.gcode")
        assert mapping is not None
        assert mapping.print_job_id == "job-B"

    def test_find_by_filename_returns_most_recent(self) -> None:
        """When multiple jobs have same filename, return most recent."""
        registry = PrintJobRegistry()
        # Register same filename twice
        registry.register(
            moonraker_job_id="0001",
            print_job_id="job-old",
            filename="same.gcode",
        )
        registry.register(
            moonraker_job_id="0002",
            print_job_id="job-new",
            filename="same.gcode",
        )

        mapping = registry.find_by_filename("same.gcode")
        assert mapping is not None
        # Should return the most recently registered one
        assert mapping.print_job_id == "job-new"

    def test_find_by_moonraker_job_id_returns_none_for_unknown(self) -> None:
        """Lookup for unknown moonraker_job_id returns None."""
        registry = PrintJobRegistry()
        registry.register(
            moonraker_job_id="0001",
            print_job_id="job-A",
            filename="test.gcode",
        )

        assert registry.find_by_moonraker_job_id("unknown") is None

    def test_find_by_filename_returns_none_for_unknown(self) -> None:
        """Lookup for unknown filename returns None."""
        registry = PrintJobRegistry()
        registry.register(
            moonraker_job_id="0001",
            print_job_id="job-A",
            filename="test.gcode",
        )

        assert registry.find_by_filename("unknown.gcode") is None

    def test_lru_eviction_removes_oldest_entries(self) -> None:
        """Verify oldest entries are evicted when exceeding max size."""
        registry = PrintJobRegistry(max_entries=3, max_age=timedelta(hours=24))

        # Register 4 entries
        for i in range(4):
            registry.register(
                moonraker_job_id=f"job-{i}",
                print_job_id=f"pj-{i}",
                filename=f"file-{i}.gcode",
            )

        # First entry should be evicted
        assert registry.find_by_moonraker_job_id("job-0") is None
        # Newer entries should remain
        assert registry.find_by_moonraker_job_id("job-1") is not None
        assert registry.find_by_moonraker_job_id("job-2") is not None
        assert registry.find_by_moonraker_job_id("job-3") is not None

    def test_none_filename_is_handled(self) -> None:
        """Registration with None filename should work."""
        registry = PrintJobRegistry()
        registry.register(
            moonraker_job_id="0001",
            print_job_id="job-A",
            filename=None,
        )

        mapping = registry.find_by_moonraker_job_id("0001")
        assert mapping is not None
        assert mapping.filename is None

        # Should not be findable by any filename
        assert registry.find_by_filename("anything.gcode") is None


class TestPrintJobMapping:
    """Tests for PrintJobMapping dataclass."""

    def test_dataclass_fields(self) -> None:
        """Verify dataclass stores all fields correctly."""
        now = datetime.now(timezone.utc)
        mapping = PrintJobMapping(
            moonraker_job_id="0001",
            print_job_id="job-A",
            filename="test.gcode",
            registered_at=now,
        )

        assert mapping.moonraker_job_id == "0001"
        assert mapping.print_job_id == "job-A"
        assert mapping.filename == "test.gcode"
        assert mapping.registered_at == now
