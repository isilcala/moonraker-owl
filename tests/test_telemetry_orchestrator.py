"""Tests for the telemetry pipeline scaffolding."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from moonraker_owl.telemetry import TelemetryOrchestrator
from moonraker_owl.telemetry.state_store import MoonrakerStateStore


class FakeClock:
    def __init__(self, *timestamps: datetime) -> None:
        if not timestamps:
            raise ValueError("At least one timestamp is required")
        self._timestamps = list(timestamps)
        self._index = 0

    def __call__(self) -> datetime:
        if self._index < len(self._timestamps):
            value = self._timestamps[self._index]
            self._index += 1
            return value
        return self._timestamps[-1]


class IncrementingClock:
    def __init__(self, start: datetime, step: timedelta = timedelta(seconds=1)) -> None:
        self._current = start
        self._step = step

    def __call__(self) -> datetime:
        value = self._current
        self._current += self._step
        return value


def _iter_stream_payloads(limit: int = 200) -> list[dict]:
    stream_path = (
        Path(__file__).resolve().parent / "fixtures" / "moonraker-stream-20251102T151954Z.jsonl"
    )
    payloads: list[dict] = []
    with stream_path.open("r", encoding="utf-8") as handle:
        for idx, line in enumerate(handle):
            if limit and idx >= limit:
                break
            line = line.strip()
            if not line:
                continue
            payloads.append(json.loads(line)["payload"])
    return payloads


@pytest.fixture()
def baseline_snapshot() -> dict:
    return {
        "result": {
            "status": {
                "print_stats": {
                    "state": "printing",
                    "history_id": 4062,
                    "filename": "benchy.gcode",
                    "print_duration": 96,
                    "print_duration_remaining": 642,
                    "message": "Heating nozzle to 215°C",
                },
                "virtual_sdcard": {
                    "progress": 0.12,
                    "layer": {"current": 5, "total": 42},
                },
                "display_status": {
                    "message": "Heating nozzle to 215°C",
                },
                "extruder": {
                    "temperature": 204.3,
                    "target": 215.0,
                },
                "heater_bed": {
                    "temperature": 59.6,
                    "target": 60.0,
                },
                "fan": {
                    "value": 72.0,
                },
            }
        }
    }


def test_state_store_tracks_sections(baseline_snapshot: dict) -> None:
    clock = FakeClock(
        datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 4, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 5, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 6, tzinfo=timezone.utc),
    )
    store = MoonrakerStateStore(clock=clock)

    store.ingest(baseline_snapshot)

    print_stats = store.get("print_stats")
    assert print_stats is not None
    assert print_stats.data["state"] == "printing"

    update = {
        "method": "notify_status_update",
        "params": [{"extruder": {"temperature": 210.0}}],
    }

    store.ingest(update)

    extruder = store.get("extruder")
    assert extruder is not None
    assert extruder.data["temperature"] == 210.0

    duplicate = {
        "method": "notify_status_update",
        "params": [{"extruder": {"temperature": 210.0}}],
    }

    store.ingest(duplicate)

    extruder_after_duplicate = store.get("extruder")
    assert extruder_after_duplicate is not None
    assert extruder_after_duplicate.observed_at == extruder.observed_at
    assert store.latest_observed_at() == extruder.observed_at

    changed = {
        "method": "notify_status_update",
        "params": [{"extruder": {"temperature": 211.0}}],
    }

    store.ingest(changed)

    extruder_changed = store.get("extruder")
    assert extruder_changed is not None
    assert extruder_changed.data["temperature"] == 211.0
    assert extruder_changed.observed_at > extruder.observed_at
    assert store.latest_observed_at() == extruder_changed.observed_at


def test_orchestrator_builds_channel_payloads(baseline_snapshot: dict) -> None:
    clock = FakeClock(
        datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc),
    )

    orchestrator = TelemetryOrchestrator(clock=clock)
    orchestrator.ingest(baseline_snapshot)
    orchestrator.set_sensors_mode(
        mode="watch",
        max_hz=1.0,
        watch_window_expires=datetime(2025, 10, 10, 16, 44, 3, tzinfo=timezone.utc),
    )

    orchestrator.events.record_command_state(
        command_id="cmd-123",
        command_type="sensors:set-rate",
        state="completed",
        session_id="history-4062",
    )

    frames = orchestrator.build_payloads()

    assert set(frames.keys()) == {"status", "sensors", "events"}

    status_frame = frames["status"]
    assert status_frame.channel == "status"
    assert status_frame.session_id.startswith("history-")
    status_body = status_frame.payload
    assert status_body["printerStatus"] == "Printing"
    assert status_body["flags"]["watchWindowActive"] is True
    assert status_body["job"]["progressPercent"] == pytest.approx(12.0, rel=1e-3)

    sensors_frame = frames["sensors"]
    sensors = sensors_frame.payload
    assert sensors["cadence"]["mode"] == "watch"
    assert sensors["cadence"]["maxHz"] == 1.0
    assert len(sensors["sensors"]) == 3

    events_frame = frames["events"]
    events = events_frame.payload
    # Events include both the command state change and a print:started event
    # (triggered by the print_stats.state=printing in the baseline snapshot)
    assert len(events["items"]) >= 1
    command_events = [e for e in events["items"] if e["eventName"] == "system:command-state"]
    assert len(command_events) == 1
    event = command_events[0]
    assert event["eventName"] == "system:command-state"
    assert event["data"]["state"] == "completed"

    frames_second = orchestrator.build_payloads()
    assert frames_second == {}


def test_status_last_updated_remains_stable_without_state_changes(
    baseline_snapshot: dict,
) -> None:
    clock = FakeClock(
        datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 4, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 5, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 6, tzinfo=timezone.utc),
    )

    orchestrator = TelemetryOrchestrator(clock=clock, heartbeat_seconds=1)
    orchestrator.ingest(baseline_snapshot)

    first_frames = orchestrator.build_payloads()
    first_status = first_frames["status"].payload
    first_last_updated = first_status["lastUpdatedUtc"]

    orchestrator.ingest(baseline_snapshot)

    second_frames = orchestrator.build_payloads()
    second_status = second_frames["status"].payload
    assert second_status["lastUpdatedUtc"] == first_last_updated


def test_sensor_last_updated_ignores_rounding_noise(baseline_snapshot: dict) -> None:
    clock = FakeClock(
        datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 4, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 5, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 6, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 7, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 8, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 9, tzinfo=timezone.utc),
    )

    orchestrator = TelemetryOrchestrator(clock=clock)
    orchestrator.ingest(baseline_snapshot)

    first_frames = orchestrator.build_payloads()
    sensors_first = first_frames["sensors"].payload["sensors"]
    extruder_first = next(sensor for sensor in sensors_first if sensor["channel"] == "extruder")
    first_last_updated = extruder_first["lastUpdatedUtc"]

    noise_update = {
        "method": "notify_status_update",
        "params": [{"extruder": {"temperature": 204.4, "target": 215.0}}],
    }

    orchestrator.ingest(noise_update)
    second_frames = orchestrator.build_payloads(forced_channels=["sensors"])
    sensors_second = second_frames["sensors"].payload["sensors"]
    extruder_second = next(sensor for sensor in sensors_second if sensor["channel"] == "extruder")
    assert extruder_second["lastUpdatedUtc"] == first_last_updated

    change_update = {
        "method": "notify_status_update",
        "params": [{"extruder": {"temperature": 205.6, "target": 216.0}}],
    }

    orchestrator.ingest(change_update)
    third_frames = orchestrator.build_payloads()
    sensors_third = third_frames["sensors"].payload["sensors"]
    extruder_third = next(sensor for sensor in sensors_third if sensor["channel"] == "extruder")
    assert extruder_third["lastUpdatedUtc"] != first_last_updated


def test_watch_window_flag_reflects_idle_mode(baseline_snapshot: dict) -> None:
    now = datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc)
    clock = FakeClock(now, now, now, now)
    orchestrator = TelemetryOrchestrator(clock=clock)
    orchestrator.ingest(baseline_snapshot)
    orchestrator.set_sensors_mode(
        mode="idle", max_hz=0.033, watch_window_expires=None
    )

    frames = orchestrator.build_payloads()

    status_flags = frames["status"].payload["flags"]
    assert status_flags["watchWindowActive"] is False


def test_replay_stream_produces_progress_while_printing() -> None:
    """Test that job info is populated while printing is active.

    This test replays the first 200 frames of the stream (while printing)
    to verify progress tracking works during an active print.
    """
    clock = IncrementingClock(datetime(2025, 11, 2, 15, 19, tzinfo=timezone.utc))
    orchestrator = TelemetryOrchestrator(clock=clock)

    # Only process first 200 frames (while printing is active)
    for payload in _iter_stream_payloads(limit=200):
        orchestrator.ingest(payload)

    frames = orchestrator.build_payloads()
    assert "status" in frames
    status_payload = frames["status"].payload

    job_info = status_payload.get("job", {})
    assert job_info.get("name"), "Job name should be populated during active print"
    assert "progress" in job_info, "Progress section should exist during active print"

    sensors_payload = frames["sensors"].payload
    assert sensors_payload["cadence"]["mode"] == "idle"


def test_replay_stream_clears_job_after_completion() -> None:
    """Test that job info is cleared when print ends (not latched).

    This verifies the new behavior where session_id becomes None after
    the print job completes, causing job info to be cleared from status.
    """
    clock = IncrementingClock(datetime(2025, 11, 2, 15, 19, tzinfo=timezone.utc))
    orchestrator = TelemetryOrchestrator(clock=clock)

    # Process entire stream (ends with cancelled state)
    for payload in _iter_stream_payloads(limit=0):
        orchestrator.ingest(payload)

    frames = orchestrator.build_payloads()
    assert "status" in frames
    status_payload = frames["status"].payload

    # Job info should be cleared when print ends
    job_info = status_payload.get("job")
    assert job_info is None or job_info == {}, "Job info should be cleared after print ends"


def test_status_selector_emits_error_phase_when_klippy_shutdown() -> None:
    """Test that StatusSelector outputs Error phase when klippy is shutdown.

    This ensures the UI correctly displays error state when klippy shuts down,
    even if Moonraker sends stale print_stats updates with 'standby' state.
    """
    clock = IncrementingClock(datetime(2025, 11, 30, 12, 0, tzinfo=timezone.utc))
    orchestrator = TelemetryOrchestrator(clock=clock)

    # Start with a normal idle state
    orchestrator.ingest({
        "result": {
            "status": {
                "print_stats": {"state": "standby", "message": ""},
                "webhooks": {"state": "ready"},
            }
        }
    })

    frames = orchestrator.build_payloads()
    assert frames["status"].payload["printerStatus"] == "Idle"
    assert frames["status"].payload["flags"]["isShutdown"] is False

    # Simulate klippy shutdown
    orchestrator.ingest({
        "jsonrpc": "2.0",
        "method": "notify_klippy_shutdown",
        "params": [{"message": "MCU shutdown: Heater heater_bed exceeded maximum temp"}],
    })

    # Moonraker might send a stale print_stats update
    orchestrator.ingest({
        "jsonrpc": "2.0",
        "method": "notify_print_stats_update",
        "params": [{"state": "standby", "message": ""}],
    })

    frames = orchestrator.build_payloads()
    status = frames["status"].payload

    # Critical: phase should be Error, not Idle
    assert status["printerStatus"] == "Error", (
        "printerStatus should be Error when klippy is shutdown"
    )
    assert status["lifecycle"]["phase"] == "Error"
    assert status["flags"]["isShutdown"] is True

    # The shutdown message should be available
    assert "MCU shutdown" in status.get("subStatus", "")
    assert "MCU shutdown" in status["lifecycle"].get("reason", "")

    # Verify events channel emits klippy:shutdown
    events = frames.get("events")
    if events:
        shutdown_events = [
            e for e in events.payload.get("items", [])
            if e.get("eventName") == "klippy:shutdown"
        ]
        assert len(shutdown_events) == 1, "Should emit klippy:shutdown event"


def test_sensors_dedup_ignores_watch_window_expiry_changes(baseline_snapshot: dict) -> None:
    """Verify that changes to watchWindowExpiresUtc don't cause sensor republishing.
    
    This is a regression test for a bug where the sensors contract_hash included
    cadence metadata, causing ~1Hz publish rate in watch mode even when sensor
    values hadn't changed (because watchWindowExpiresUtc was ticking down).
    """
    now = datetime(2025, 10, 10, 16, 42, 0, tzinfo=timezone.utc)
    clock = FakeClock(now, now, now, now, now, now)
    orchestrator = TelemetryOrchestrator(clock=clock)
    orchestrator.ingest(baseline_snapshot)
    
    # Set watch mode with initial window expiring in 30 seconds
    orchestrator.set_sensors_mode(
        mode="watch", 
        max_hz=1.0, 
        watch_window_expires=now + timedelta(seconds=30)
    )
    
    # First build should emit sensors
    frames1 = orchestrator.build_payloads()
    assert "sensors" in frames1, "First build should emit sensors"
    
    # Simulate time passing - window now expires in 29 seconds (1 second elapsed)
    orchestrator.set_sensors_mode(
        mode="watch",
        max_hz=1.0,
        watch_window_expires=now + timedelta(seconds=29)
    )
    
    # Second build should NOT emit sensors (values unchanged, only cadence changed)
    frames2 = orchestrator.build_payloads()
    assert "sensors" not in frames2, (
        "Sensors should be deduplicated when only watchWindowExpiresUtc changes"
    )
    
    # Simulate watch window being extended (user opened sensors panel again)
    orchestrator.set_sensors_mode(
        mode="watch",
        max_hz=1.0,
        watch_window_expires=now + timedelta(seconds=45)
    )
    
    # Third build should still NOT emit sensors
    frames3 = orchestrator.build_payloads()
    assert "sensors" not in frames3, (
        "Sensors should be deduplicated when watch window is extended"
    )
    
    # Now change an actual sensor value
    value_update = {
        "method": "notify_status_update",
        "params": [{"extruder": {"temperature": 210.0}}],  # Changed from 204
    }
    orchestrator.ingest(value_update)
    
    # Fourth build SHOULD emit sensors (value actually changed)
    frames4 = orchestrator.build_payloads()
    assert "sensors" in frames4, (
        "Sensors should be emitted when sensor values change"
    )


# Note: test_set_thumbnail_url_delegates_to_session_tracker has been removed.
# Thumbnail URLs are now pushed via SignalR after upload ACK processing,
# so set_thumbnail_url is no longer part of the TelemetryOrchestrator API.


def test_timelapse_render_started_enables_polling_fallback(baseline_snapshot: dict) -> None:
    """Timelapse polling should be enabled when render status is 'started'.
    
    Some moonraker-timelapse versions skip 'running' and go directly from
    'started' to completion without sending 'success' notification.
    We need to start polling early to catch the new file.
    """
    now = datetime(2025, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(now, now + timedelta(seconds=1))
    orchestrator = TelemetryOrchestrator(clock=clock)
    
    # Initialize with printing state
    orchestrator.ingest(baseline_snapshot)
    
    # Initially, polling should not be requested
    assert not orchestrator.should_poll_timelapse()
    
    # Simulate timelapse render started event
    render_started = {
        "method": "notify_timelapse_event",
        "params": [{"action": "render", "status": "started"}],
    }
    orchestrator.ingest(render_started)
    
    # Polling should now be enabled
    assert orchestrator.should_poll_timelapse(), (
        "Polling should be enabled when render status is 'started'"
    )


def test_timelapse_render_running_enables_polling_fallback(baseline_snapshot: dict) -> None:
    """Timelapse polling should be enabled when render status is 'running'."""
    now = datetime(2025, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    clock = FakeClock(now, now + timedelta(seconds=1))
    orchestrator = TelemetryOrchestrator(clock=clock)
    
    # Initialize with printing state
    orchestrator.ingest(baseline_snapshot)
    
    # Initially, polling should not be requested
    assert not orchestrator.should_poll_timelapse()
    
    # Simulate timelapse render running event
    render_running = {
        "method": "notify_timelapse_event",
        "params": [{"action": "render", "status": "running"}],
    }
    orchestrator.ingest(render_running)
    
    # Polling should now be enabled
    assert orchestrator.should_poll_timelapse(), (
        "Polling should be enabled when render status is 'running'"
    )


def test_timelapse_polling_abandoned_after_timeout_not_re_enabled(baseline_snapshot: dict) -> None:
    """Once timelapse polling times out, it should stay abandoned until render:success.
    
    This test verifies that after polling times out (5 minutes), subsequent
    render:running events do NOT re-enable polling. Only render:success or
    reset() should clear the abandoned state.
    """
    now = datetime(2025, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    # Use incrementing clock with 1 second steps for initial setup
    clock = IncrementingClock(now, step=timedelta(seconds=1))
    orchestrator = TelemetryOrchestrator(clock=clock)
    
    # Initialize with printing state
    orchestrator.ingest(baseline_snapshot)
    
    # Simulate timelapse render running event
    render_running = {
        "method": "notify_timelapse_event",
        "params": [{"action": "render", "status": "running"}],
    }
    orchestrator.ingest(render_running)
    
    # Polling should be enabled initially
    assert orchestrator.should_poll_timelapse(), "Polling should be enabled initially"
    
    # Advance clock past the 5-minute timeout
    clock._current = now + timedelta(minutes=6)
    
    # Now polling should time out
    assert not orchestrator.should_poll_timelapse(), "Polling should time out after 5 minutes"
    
    # Another render:running event comes in - should NOT re-enable polling
    orchestrator.ingest(render_running)
    
    # Polling should remain abandoned
    assert not orchestrator.should_poll_timelapse(), (
        "Polling should remain abandoned after timeout, even with new render:running event"
    )


def test_timelapse_polling_abandoned_cleared_by_render_success(baseline_snapshot: dict) -> None:
    """render:success should clear the abandoned state and allow re-enabling."""
    now = datetime(2025, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    clock = IncrementingClock(now, step=timedelta(seconds=1))
    orchestrator = TelemetryOrchestrator(clock=clock)
    
    # Initialize with printing state
    orchestrator.ingest(baseline_snapshot)
    
    # Start polling
    render_running = {
        "method": "notify_timelapse_event",
        "params": [{"action": "render", "status": "running"}],
    }
    orchestrator.ingest(render_running)
    assert orchestrator.should_poll_timelapse()
    
    # Advance past timeout
    clock._current = now + timedelta(minutes=6)
    
    # Time out
    assert not orchestrator.should_poll_timelapse()
    
    # render:success comes in (from notification or fallback detection)
    render_success = {
        "method": "notify_timelapse_event",
        "params": [{"action": "render", "status": "success"}],
    }
    orchestrator.ingest(render_success)
    
    # A new timelapse starts rendering
    orchestrator.ingest(render_running)
    
    # Polling should now be enabled again
    assert orchestrator.should_poll_timelapse(), (
        "Polling should be re-enabled after render:success clears abandoned state"
    )


def test_timelapse_polling_abandoned_cleared_by_polling_success(baseline_snapshot: dict) -> None:
    """emit_timelapse_from_polling should clear the abandoned state for next timelapse."""
    now = datetime(2025, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    clock = IncrementingClock(now, step=timedelta(seconds=1))
    orchestrator = TelemetryOrchestrator(clock=clock)
    
    # Initialize with printing state
    orchestrator.ingest(baseline_snapshot)
    
    # Start polling
    render_running = {
        "method": "notify_timelapse_event",
        "params": [{"action": "render", "status": "running"}],
    }
    orchestrator.ingest(render_running)
    assert orchestrator.should_poll_timelapse()
    
    # Advance past timeout - polling gets abandoned
    clock._current = now + timedelta(minutes=6)
    assert not orchestrator.should_poll_timelapse()
    assert orchestrator._timelapse_poll_abandoned
    
    # Polling finds the file anyway (success path)
    orchestrator.emit_timelapse_from_polling("timelapse_20250501_120600.mp4")
    
    # Abandoned flag should be cleared
    assert not orchestrator._timelapse_poll_abandoned, (
        "emit_timelapse_from_polling should clear abandoned state"
    )
    
    # A new timelapse starts rendering
    orchestrator.ingest(render_running)
    
    # Polling should now be enabled again
    assert orchestrator.should_poll_timelapse(), (
        "Polling should be re-enabled after successful polling detection"
    )


def test_timelapse_event_dedup_by_observed_at(baseline_snapshot: dict) -> None:
    """Same timelapse event should not be reprocessed on every ingest.

    This tests that the observed_at timestamp deduplication prevents the
    same timelapse event from being processed multiple times. Without this
    fix, every ingest() call would re-read the stored event and cause log spam.
    """
    now = datetime(2025, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    clock = IncrementingClock(now, step=timedelta(milliseconds=60))
    orchestrator = TelemetryOrchestrator(clock=clock)

    # Initialize with printing state
    orchestrator.ingest(baseline_snapshot)

    # Send render:running event
    render_running = {
        "method": "notify_timelapse_event",
        "params": [{"action": "render", "status": "running"}],
    }
    orchestrator.ingest(render_running)

    # First ingest enables polling
    assert orchestrator.should_poll_timelapse()
    poll_started_at = orchestrator._timelapse_poll_started_at

    # Multiple subsequent ingests should NOT re-enable or modify polling state
    # because the same event (same observed_at) should be skipped
    for _ in range(10):
        # Ingest another status update (not a new timelapse event)
        orchestrator.ingest({
            "result": {
                "status": {
                    "print_stats": {"state": "printing"},
                }
            }
        })

    # Poll state should be unchanged - not re-triggered
    assert orchestrator.should_poll_timelapse()
    assert orchestrator._timelapse_poll_started_at == poll_started_at, (
        "Polling start time should not change from repeated ingests of same event"
    )

    # Now simulate timeout and verify no log spam scenario
    # Advance past timeout
    clock._current = now + timedelta(minutes=6)
    assert not orchestrator.should_poll_timelapse()
    assert orchestrator._timelapse_poll_abandoned

    # Multiple ingests should NOT process the same render:running event again
    # (this is what was causing the log spam bug)
    for _ in range(5):
        orchestrator.ingest({
            "result": {
                "status": {
                    "print_stats": {"state": "printing"},
                }
            }
        })

    # Abandoned flag should remain True - not being reset by stale events
    assert orchestrator._timelapse_poll_abandoned, (
        "Abandoned flag should not be affected by repeated ingests"
    )


class TestPrintEventPayloads:
    """Tests for print event payload generation ensuring all statistics fields are populated."""

    def _setup_active_job(self) -> TelemetryOrchestrator:
        """Create an orchestrator with an active print job following the production flow."""
        now = datetime(2025, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
        clock = IncrementingClock(now, step=timedelta(seconds=1))
        orchestrator = TelemetryOrchestrator(clock=clock)

        # Step 1: Job starts - action:added (like production)
        orchestrator.ingest({
            "jsonrpc": "2.0",
            "method": "notify_history_changed",
            "params": [{
                "action": "added",
                "job": {
                    "job_id": "0000ABCD",
                    "filename": "test_model.gcode",
                },
            }],
        })

        # Step 2: print_stats shows printing state with statistics
        orchestrator.ingest({
            "result": {
                "status": {
                    "print_stats": {
                        "state": "printing",
                        "filename": "test_model.gcode",
                        "print_duration": 3600.0,  # 1 hour
                        "filament_used": 1234.56,  # mm
                        "message": "",
                    },
                    "virtual_sdcard": {
                        "progress": 0.75,
                    },
                },
            },
        })

        # Harvest initial events (print:started)
        orchestrator.events.harvest()
        return orchestrator

    def test_print_completed_event_has_all_statistics(self) -> None:
        """print:completed event should include printDuration, filamentUsedMm, progressPercent."""
        orchestrator = self._setup_active_job()

        # Update stats to final values
        orchestrator.ingest({
            "result": {
                "status": {
                    "print_stats": {
                        "state": "complete",
                        "filename": "test_model.gcode",
                        "print_duration": 4200.0,
                        "filament_used": 1600.0,
                        "message": "",
                    },
                    "virtual_sdcard": {
                        "progress": 1.0,
                    },
                },
            },
        })

        # Job finishes with status = completed
        orchestrator.ingest({
            "jsonrpc": "2.0",
            "method": "notify_history_changed",
            "params": [{
                "action": "finished",
                "job": {
                    "job_id": "0000ABCD",
                    "filename": "test_model.gcode",
                    "status": "completed",
                },
            }],
        })

        events = orchestrator.events.harvest()
        completed_events = [e for e in events if e.event_name.value == "print:completed"]
        
        assert len(completed_events) >= 1, f"Expected completed event, got: {[e.event_name.value for e in events]}"
        event = completed_events[0]

        # Verify all statistics fields
        assert "printDuration" in event.data, f"printDuration missing. Got: {event.data}"
        assert "filamentUsedMm" in event.data, f"filamentUsedMm missing. Got: {event.data}"
        assert "progressPercent" in event.data, f"progressPercent missing. Got: {event.data}"

        assert event.data["printDuration"] > 0
        assert event.data["filamentUsedMm"] > 0
        assert 0 <= event.data["progressPercent"] <= 100

    def test_print_cancelled_event_has_all_statistics(self) -> None:
        """print:cancelled event should include printDuration, filamentUsedMm, progressPercent."""
        orchestrator = self._setup_active_job()

        # Update stats at cancellation point
        orchestrator.ingest({
            "result": {
                "status": {
                    "print_stats": {
                        "state": "cancelled",
                        "filename": "test_model.gcode",
                        "print_duration": 1800.0,
                        "filament_used": 600.0,
                        "message": "",
                    },
                    "virtual_sdcard": {
                        "progress": 0.5,
                    },
                },
            },
        })

        # Job finishes with status = cancelled
        orchestrator.ingest({
            "jsonrpc": "2.0",
            "method": "notify_history_changed",
            "params": [{
                "action": "finished",
                "job": {
                    "job_id": "0000ABCD",
                    "filename": "test_model.gcode",
                    "status": "cancelled",
                },
            }],
        })

        events = orchestrator.events.harvest()
        cancelled_events = [e for e in events if e.event_name.value == "print:cancelled"]
        
        assert len(cancelled_events) >= 1, f"Expected cancelled event, got: {[e.event_name.value for e in events]}"
        event = cancelled_events[0]

        assert "printDuration" in event.data, f"printDuration missing. Got: {event.data}"
        assert "filamentUsedMm" in event.data, f"filamentUsedMm missing. Got: {event.data}"
        assert "progressPercent" in event.data, f"progressPercent missing. Got: {event.data}"

    def test_print_failed_event_has_all_statistics(self) -> None:
        """print:failed event should include printDuration, filamentUsedMm, progressPercent, errorMessage."""
        orchestrator = self._setup_active_job()

        # Update stats with error
        orchestrator.ingest({
            "result": {
                "status": {
                    "print_stats": {
                        "state": "error",
                        "filename": "test_model.gcode",
                        "print_duration": 900.0,
                        "filament_used": 300.0,
                        "message": "Heater timeout on extruder",
                    },
                    "virtual_sdcard": {
                        "progress": 0.25,
                    },
                },
            },
        })

        # Job finishes with status = error
        orchestrator.ingest({
            "jsonrpc": "2.0",
            "method": "notify_history_changed",
            "params": [{
                "action": "finished",
                "job": {
                    "job_id": "0000ABCD",
                    "filename": "test_model.gcode",
                    "status": "error",
                },
            }],
        })

        events = orchestrator.events.harvest()
        failed_events = [e for e in events if e.event_name.value == "print:failed"]
        
        assert len(failed_events) >= 1, f"Expected failed event, got: {[e.event_name.value for e in events]}"
        event = failed_events[0]

        assert "printDuration" in event.data, f"printDuration missing. Got: {event.data}"
        assert "filamentUsedMm" in event.data, f"filamentUsedMm missing. Got: {event.data}"
        assert "progressPercent" in event.data, f"progressPercent missing. Got: {event.data}"
        assert "errorMessage" in event.data, f"errorMessage missing. Got: {event.data}"
        assert event.data["errorMessage"] == "Heater timeout on extruder"

