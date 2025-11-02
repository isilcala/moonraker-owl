from moonraker_owl.telemetry.state_store import MoonrakerStateStore
from moonraker_owl.telemetry.trackers import PrintSessionTracker


def test_active_job_without_session_when_raw_state_printing() -> None:
    tracker = PrintSessionTracker()

    result = tracker._is_active_job(
        raw_state="printing",
        idle_state=None,
        timelapse_paused=False,
    )

    assert result is True


def test_inactive_without_session_when_terminal_state() -> None:
    tracker = PrintSessionTracker()

    result = tracker._is_active_job(
        raw_state="cancelled",
        idle_state=None,
        timelapse_paused=False,
    )

    assert result is False


def test_idle_timeout_printing_marks_active() -> None:
    tracker = PrintSessionTracker()

    result = tracker._is_active_job(
        raw_state=None,
        idle_state="printing",
        timelapse_paused=False,
    )

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
    assert session.job_id == "0002A5"
    assert session.job_name == "sample.gcode"
    assert session.has_active_job is True


def test_history_in_progress_overrides_cancelled_state() -> None:
    tracker = PrintSessionTracker()
    store = MoonrakerStateStore()

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "cancelled"},
                    "virtual_sdcard": {"is_active": True},
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
                        "job_id": "0003AA",
                        "filename": "sample.gcode",
                        "status": "in_progress",
                    },
                }
            ],
        }
    )

    session = tracker.compute(store)

    assert session.raw_state == "cancelled"
    assert session.job_status == "in_progress"
    assert session.has_active_job is True
