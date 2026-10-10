"""Tests for PollingScheduler failure backoff (P1-7)."""

import asyncio
import logging

import pytest

from moonraker_owl.telemetry.polling import PollingScheduler, _PollGroup


@pytest.mark.asyncio
async def test_poll_loop_backs_off_on_repeated_failures(monkeypatch):
    """A failing fetch must back off (grow the wait) instead of full-cadence."""
    stop_event = asyncio.Event()
    waits: list[float] = []

    async def failing_fetch(objects):
        raise RuntimeError("moonraker down")

    async def enqueue(payload):  # pragma: no cover - never reached on failure
        pass

    scheduler = PollingScheduler(
        fetch_state=failing_fetch,
        enqueue=enqueue,
        stop_event=stop_event,
    )

    group = _PollGroup(
        name="test",
        objects={"toolhead": None},
        interval=1.0,
        initial_delay=0.0,
    )

    real_wait_for = asyncio.wait_for

    async def spy_wait_for(awaitable, timeout):
        # Only record the inter-poll wait (waiting on the stop_event).
        waits.append(timeout)
        if len(waits) >= 4:
            stop_event.set()
        # Close the un-awaited coroutine to avoid a ResourceWarning, then
        # signal a timeout so the loop iterates quickly.
        close = getattr(awaitable, "close", None)
        if close is not None:
            close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(
        "moonraker_owl.telemetry.polling.asyncio.wait_for", spy_wait_for
    )

    # The loop breaks when stop_event triggers via TimeoutError path? We set the
    # event and the next wait_for raises TimeoutError -> continue; guard with a
    # bounded run.
    task = asyncio.create_task(scheduler._poll_loop(group))
    await asyncio.sleep(0)
    # Let it iterate a few times.
    for _ in range(20):
        await real_wait_for(asyncio.sleep(0), timeout=1)
        if len(waits) >= 4:
            break
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(waits) >= 2
    # Backoff must be strictly increasing across consecutive failures.
    assert waits[1] > waits[0]
    assert group.consecutive_failures >= 2


@pytest.mark.asyncio
async def test_poll_loop_resets_backoff_on_success(monkeypatch):
    """A successful fetch resets the failure counter."""
    stop_event = asyncio.Event()
    call = {"n": 0}
    enqueued: list[dict] = []

    async def flaky_fetch(objects):
        call["n"] += 1
        if call["n"] <= 2:
            raise RuntimeError("transient")
        return {"ok": True}

    async def enqueue(payload):
        enqueued.append(payload)
        stop_event.set()  # stop after first successful enqueue

    scheduler = PollingScheduler(
        fetch_state=flaky_fetch,
        enqueue=enqueue,
        stop_event=stop_event,
    )

    group = _PollGroup(
        name="test",
        objects={"toolhead": None},
        interval=0.01,
        initial_delay=0.0,
    )

    await asyncio.wait_for(scheduler._poll_loop(group), timeout=5)

    assert enqueued == [{"ok": True}]
    assert group.consecutive_failures == 0


@pytest.mark.asyncio
async def test_poll_failure_logs_warning_rate_limited(caplog):
    """Failures log a WARNING, rate-limited so an outage doesn't flood the log."""
    stop_event = asyncio.Event()

    async def failing_fetch(objects):  # pragma: no cover - not used here
        raise RuntimeError("moonraker down")

    async def enqueue(payload):  # pragma: no cover - never reached on failure
        pass

    scheduler = PollingScheduler(
        fetch_state=failing_fetch,
        enqueue=enqueue,
        stop_event=stop_event,
    )

    group = _PollGroup(
        name="test",
        objects={"toolhead": None},
        interval=1.0,
        initial_delay=0.0,
    )

    with caplog.at_level(logging.WARNING, logger="moonraker_owl.telemetry.polling"):
        for _ in range(3):
            group.consecutive_failures += 1
            scheduler._log_poll_failure(group, RuntimeError("moonraker down"))

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "test" in warnings[0].getMessage()
    assert "1 consecutive" in warnings[0].getMessage()


@pytest.mark.asyncio
async def test_poll_loop_logs_recovery(caplog):
    """A successful fetch after failures logs a recovery message."""
    stop_event = asyncio.Event()
    call = {"n": 0}

    async def flaky_fetch(objects):
        call["n"] += 1
        if call["n"] <= 2:
            raise RuntimeError("transient")
        return {"ok": True}

    async def enqueue(payload):
        stop_event.set()  # stop after first successful enqueue

    scheduler = PollingScheduler(
        fetch_state=flaky_fetch,
        enqueue=enqueue,
        stop_event=stop_event,
    )

    group = _PollGroup(
        name="test",
        objects={"toolhead": None},
        interval=0.01,
        initial_delay=0.0,
    )

    with caplog.at_level(logging.INFO, logger="moonraker_owl.telemetry.polling"):
        await asyncio.wait_for(scheduler._poll_loop(group), timeout=5)

    infos = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any("recovered after 2 failure(s)" in r.getMessage() for r in infos)
