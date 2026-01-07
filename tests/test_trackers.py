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


def test_raw_state_printing_is_active_job_regardless_of_job_status() -> None:
    """Test that raw_state="printing" is active, regardless of stale job_status.
    
    Aligned with Mainsail/Obico: trust print_stats.state as authoritative.
    Even if job_status is stale (e.g., "cancelled" from previous print),
    we trust raw_state="printing" to indicate an active job.
    """
    tracker = PrintSessionTracker()

    # raw_state="printing" should be active, even with stale job_status
    result = tracker._has_active_job(
        raw_state="printing",
        job_status="completed",  # Stale from previous print
    )

    # With Mainsail-aligned logic: raw_state takes precedence
    assert result is True


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
    assert tracker._filament_total is None
    assert tracker._slicer_estimated_time is None


# ============================================================================
# Mainsail-compatible ETA Calculation Tests
# ============================================================================


def test_mainsail_eta_file_based_only() -> None:
    """Test ETA calculation using only file-based method.
    
    Mainsail formula: elapsed / progress - elapsed
    Example: 600s elapsed, 30% progress -> 600/0.3 - 600 = 1400s remaining
    """
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    # Set file metadata without filament info
    tracker.set_file_metadata(
        filename="test.gcode",
        gcode_start_byte=0,
        gcode_end_byte=10000,
    )

    # 30% progress, 600 seconds elapsed
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": "printing",
                        "filename": "test.gcode",
                        "print_duration": 600,
                    },
                    "virtual_sdcard": {
                        "is_active": True,
                        "file_position": 3000,  # 30% of 10000
                    },
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.remaining_seconds is not None
    # File-based: 600 / 0.3 - 600 = 1400
    assert 1350 <= session.remaining_seconds <= 1450


def test_mainsail_eta_filament_based_only() -> None:
    """Test ETA calculation using only filament-based method.
    
    Mainsail formula: elapsed / (filament_used / filament_total) - elapsed
    Example: 600s elapsed, 200mm used, 1000mm total -> 600/(200/1000) - 600 = 2400s remaining
    """
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    # Set file metadata with filament info but NO gcode byte range
    # This forces fallback to virtual_sdcard.progress which we set to 0 to disable file-based
    tracker.set_file_metadata(
        filename="test.gcode",
        gcode_start_byte=None,  # No byte range - disables file-relative progress
        gcode_end_byte=None,
        filament_total=1000.0,  # 1000mm total
    )

    # 20% filament used (200mm), 600 seconds elapsed
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": "printing",
                        "filename": "test.gcode",
                        "print_duration": 600,
                        "filament_used": 200.0,  # 20% of 1000
                    },
                    "virtual_sdcard": {
                        "is_active": True,
                        "progress": 0.0,  # Zero progress disables file-based
                    },
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.remaining_seconds is not None
    # Filament-based: 600 / (200/1000) - 600 = 600/0.2 - 600 = 2400
    assert 2350 <= session.remaining_seconds <= 2450


def test_mainsail_eta_combined_file_and_filament() -> None:
    """Test ETA calculation using average of file-based and filament-based.
    
    This is Mainsail's default behavior (calcEstimateTime: ['file', 'filament']).
    """
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    # Set file metadata with both byte range and filament info
    tracker.set_file_metadata(
        filename="test.gcode",
        gcode_start_byte=0,
        gcode_end_byte=10000,
        filament_total=1000.0,
    )

    # 30% file progress, 20% filament used, 600 seconds elapsed
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": "printing",
                        "filename": "test.gcode",
                        "print_duration": 600,
                        "filament_used": 200.0,  # 20%
                    },
                    "virtual_sdcard": {
                        "is_active": True,
                        "file_position": 3000,  # 30%
                    },
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.remaining_seconds is not None
    # File-based: 600/0.3 - 600 = 1400
    # Filament-based: 600/0.2 - 600 = 2400
    # Average: (1400 + 2400) / 2 = 1900
    assert 1850 <= session.remaining_seconds <= 1950


def test_mainsail_eta_slicer_fallback() -> None:
    """Test ETA falls back to slicer estimate when no other data available."""
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    # Set only slicer estimated time, no byte range or filament
    tracker.set_file_metadata(
        filename="test.gcode",
        gcode_start_byte=None,
        gcode_end_byte=None,
        filament_total=None,
        slicer_estimated_time=3600,  # 1 hour total
    )

    # 600 seconds elapsed, no progress or filament data
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": "printing",
                        "filename": "test.gcode",
                        "print_duration": 600,
                    },
                    "virtual_sdcard": {
                        "is_active": True,
                        "progress": 0.0,  # No progress
                    },
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.remaining_seconds is not None
    # Slicer fallback: 3600 - 600 = 3000
    assert session.remaining_seconds == 3000


def test_mainsail_eta_no_data_returns_none() -> None:
    """Test ETA returns None when no calculation method has valid data."""
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    # No metadata at all
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": "printing",
                        "filename": "test.gcode",
                        "print_duration": 600,
                    },
                    "virtual_sdcard": {
                        "is_active": True,
                        "progress": 0.0,  # No progress
                    },
                }
            ],
        }
    )

    session = tracker.compute(store)
    # No valid data for any method - should return None
    assert session.remaining_seconds is None


def test_mainsail_eta_zero_elapsed_returns_none() -> None:
    """Test ETA returns None when elapsed time is zero (just started)."""
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    tracker.set_file_metadata(
        filename="test.gcode",
        gcode_start_byte=0,
        gcode_end_byte=10000,
    )

    # Just started - 0 elapsed time
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": "printing",
                        "filename": "test.gcode",
                        "print_duration": 0,
                    },
                    "virtual_sdcard": {
                        "is_active": True,
                        "file_position": 100,
                    },
                }
            ],
        }
    )

    session = tracker.compute(store)
    # Zero elapsed time - can't calculate
    assert session.remaining_seconds is None


def test_session_includes_filament_data() -> None:
    """Test that SessionInfo includes filament data for downstream use."""
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    tracker.set_file_metadata(
        filename="test.gcode",
        gcode_start_byte=0,
        gcode_end_byte=10000,
        filament_total=1500.0,
        slicer_estimated_time=7200,
    )

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": "printing",
                        "filename": "test.gcode",
                        "print_duration": 300,
                        "filament_used": 150.0,
                    },
                    "virtual_sdcard": {
                        "is_active": True,
                        "file_position": 1000,
                    },
                }
            ],
        }
    )

    session = tracker.compute(store)
    assert session.filament_used == 150.0
    assert session.filament_total == 1500.0
    assert session.slicer_estimated_time == 7200