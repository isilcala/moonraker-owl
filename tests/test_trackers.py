from moonraker_owl.telemetry.trackers import PrintSessionTracker


def test_active_job_without_session_when_raw_state_printing() -> None:
    tracker = PrintSessionTracker()

    result = tracker._is_active_job(
        session_id=None,
        raw_state="printing",
        idle_state=None,
        job_status=None,
        timelapse_paused=False,
        progress_value=0.0,
        progress_trend="steady",
        elapsed_seconds=0,
    )

    assert result is True


def test_inactive_without_session_when_terminal_state() -> None:
    tracker = PrintSessionTracker()

    result = tracker._is_active_job(
        session_id=None,
        raw_state="cancelled",
        idle_state=None,
        job_status="cancelled",
        timelapse_paused=False,
        progress_value=0.0,
        progress_trend="steady",
        elapsed_seconds=0,
    )

    assert result is False


def test_progress_signals_mark_active_without_session() -> None:
    tracker = PrintSessionTracker()

    result = tracker._is_active_job(
        session_id=None,
        raw_state=None,
        idle_state=None,
        job_status=None,
        timelapse_paused=False,
        progress_value=12.5,
        progress_trend="increasing",
        elapsed_seconds=30,
    )

    assert result is True
