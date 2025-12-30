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
