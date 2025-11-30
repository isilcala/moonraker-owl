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
