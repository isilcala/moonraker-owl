from datetime import datetime, timezone

from moonraker_owl.telemetry.state_engine import PrinterContext, PrinterStateEngine


def _ctx(**overrides):
    base = dict(
        observed_at=datetime.now(tz=timezone.utc),
        has_active_job=True,
        is_heating=False,
        idle_state=None,
        timelapse_paused=False,
        progress_percent=0.0,
        progress_trend="unknown",
        job_status=None,
    )
    base.update(overrides)
    return PrinterContext(**base)


def test_cancelled_progress_reset_coerces_phase_cancelled() -> None:
    engine = PrinterStateEngine()
    context = _ctx(progress_percent=0.0, progress_trend="decreasing", idle_state="ready")

    result = engine.resolve("cancelled", context)

    assert result == "Cancelled"


def test_cancelled_with_active_progress_still_reports_cancelled() -> None:
    engine = PrinterStateEngine()
    context = _ctx(progress_percent=42.0, progress_trend="increasing")

    result = engine.resolve("cancelled", context)

    assert result == "Cancelled"


def test_cancelled_while_heating_transitions_to_heating() -> None:
    engine = PrinterStateEngine()
    context = _ctx(is_heating=True, progress_percent=0.0)

    result = engine.resolve("cancelled", context)

    assert result == "Heating"
