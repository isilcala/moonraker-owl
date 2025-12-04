from moonraker_owl.telemetry.state_store import MoonrakerStateStore
from moonraker_owl.telemetry.trackers import PrintSessionTracker


def test_has_active_job_when_raw_state_printing() -> None:
    """Test that printing state is detected as active job."""
    tracker = PrintSessionTracker()

    result = tracker._has_active_job(
        raw_state="printing",
        job_status=None,
    )

    assert result is True


def test_has_active_job_when_raw_state_paused() -> None:
    """Test that paused state is detected as active job."""
    tracker = PrintSessionTracker()

    result = tracker._has_active_job(
        raw_state="paused",
        job_status=None,
    )

    assert result is True


def test_no_active_job_when_terminal_state() -> None:
    """Test that cancelled state is not an active job."""
    tracker = PrintSessionTracker()

    result = tracker._has_active_job(
        raw_state="cancelled",
        job_status=None,
    )

    assert result is False


def test_no_active_job_when_job_status_terminal() -> None:
    """Test that terminal job_status takes precedence over raw_state."""
    tracker = PrintSessionTracker()

    # Even if raw_state is printing, terminal job_status takes precedence
    result = tracker._has_active_job(
        raw_state="printing",
        job_status="completed",
    )

    assert result is False


def test_history_event_nested_job_populates_session_identifiers() -> None:
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_print_stats_update",
            "params": [
                {
                    "print_stats": {
                        "state": "printing",
                    }
                }
            ],
        }
    )

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_history_changed",
            "params": [
                {
                    "action": "added",
                    "job": {
                        "job_id": "0002A5",
                        "filename": "sample.gcode",
                    },
                }
            ],
        }
    )

    session = tracker.compute(store)

    assert session.session_id == "job-0002A5"
    # job_id field removed - use session_id instead
    assert session.job_name == "sample.gcode"
    assert session.has_active_job is True


def test_session_cleared_when_print_ends() -> None:
    """Test that session_id becomes None when job ends (not latched)."""
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    # Start printing
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "printing", "filename": "test.gcode"},
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.session_id is not None
    assert session.has_active_job is True

    # Print completes
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "complete", "filename": "test.gcode"},
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.session_id is None  # Key change: session clears when job ends
    assert session.has_active_job is False


def test_file_relative_progress_calculation() -> None:
    """Test that progress is calculated using file-relative method when metadata available.
    
    This aligns with Mainsail's default 'file-relative' progress calculation:
    - Excludes slicer headers (before gcode_start_byte)
    - Only counts actual gcode bytes for progress
    - Falls back to virtual_sdcard.progress when metadata unavailable
    """
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    # Simulate a file with 1000 bytes of header, 9000 bytes of gcode
    # gcode_start_byte=1000, gcode_end_byte=10000 (9000 bytes of actual gcode)
    tracker.set_file_metadata(
        filename="test.gcode",
        gcode_start_byte=1000,
        gcode_end_byte=10000,
    )

    # Start printing at file position 1000 (just started gcode)
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "printing", "filename": "test.gcode"},
                    "virtual_sdcard": {"is_active": True, "file_position": 1000, "progress": 0.10},
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.progress_percent is not None
    # At position 1000 (gcode start), progress should be 0%
    assert session.progress_percent == 0.0

    # Move to file position 5500 (midway through gcode: 4500 bytes into 9000)
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "virtual_sdcard": {"is_active": True, "file_position": 5500, "progress": 0.55},
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.progress_percent is not None
    # File-relative: (5500-1000)/(10000-1000) = 4500/9000 = 50%
    assert abs(session.progress_percent - 50.0) < 0.1


def test_progress_fallback_without_metadata() -> None:
    """Test that progress falls back to virtual_sdcard.progress when no file metadata."""
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    # No file metadata set - should use virtual_sdcard.progress fallback
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "printing", "filename": "test.gcode"},
                    "virtual_sdcard": {"is_active": True, "file_position": 5000, "progress": 0.55},
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.progress_percent is not None
    # Without metadata, should use virtual_sdcard.progress (55%)
    assert abs(session.progress_percent - 55.0) < 0.1


def test_file_metadata_cleared_on_job_end() -> None:
    """Test that file metadata is cleared when job ends."""
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    # Set file metadata
    tracker.set_file_metadata(
        filename="test.gcode",
        gcode_start_byte=1000,
        gcode_end_byte=10000,
    )

    # Start printing
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "printing", "filename": "test.gcode"},
                    "virtual_sdcard": {"is_active": True, "file_position": 5500},
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.has_active_job is True

    # Job completes
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "complete", "filename": "test.gcode"},
                    "virtual_sdcard": {"is_active": False},
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.has_active_job is False
    # File metadata should be cleared
    assert tracker._current_filename is None
    assert tracker._gcode_start_byte is None
    assert tracker._gcode_end_byte is None