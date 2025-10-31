"""Tests for the telemetry pipeline scaffolding."""

from __future__ import annotations

from datetime import datetime, timezone

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
    orchestrator.set_telemetry_mode(
        mode="watch",
        max_hz=1.0,
        watch_window_expires=datetime(2025, 10, 10, 16, 44, 3, tzinfo=timezone.utc),
    )

    orchestrator.events.record_command_state(
        command_id="cmd-123",
        command_type="telemetry:set-rate",
        state="completed",
        session_id="history-4062",
    )

    frames = orchestrator.build_payloads()

    assert set(frames.keys()) == {"overview", "telemetry", "events"}

    overview_frame = frames["overview"]
    assert overview_frame.channel == "overview"
    assert overview_frame.session_id.startswith("history-")
    overview_body = overview_frame.payload
    assert overview_body["printerStatus"] == "Printing"
    assert overview_body["flags"]["watchWindowActive"] is True
    assert overview_body["job"]["progressPercent"] == pytest.approx(12.0, rel=1e-3)

    telemetry_frame = frames["telemetry"]
    telemetry = telemetry_frame.payload
    assert telemetry["cadence"]["mode"] == "watch"
    assert telemetry["cadence"]["maxHz"] == 1.0
    assert len(telemetry["sensors"]) == 3

    events_frame = frames["events"]
    events = events_frame.payload
    assert len(events["items"]) == 1
    event = events["items"][0]
    assert event["eventName"] == "commandStateChanged"
    assert event["data"]["state"] == "completed"

    frames_second = orchestrator.build_payloads()
    assert frames_second["overview"].observed_at >= overview_frame.observed_at
    assert frames_second["telemetry"].observed_at >= telemetry_frame.observed_at


def test_overview_last_updated_remains_stable_without_state_changes(
    baseline_snapshot: dict,
) -> None:
    clock = FakeClock(
        datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 4, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 5, tzinfo=timezone.utc),
        datetime(2025, 10, 10, 16, 42, 6, tzinfo=timezone.utc),
    )

    orchestrator = TelemetryOrchestrator(clock=clock)
    orchestrator.ingest(baseline_snapshot)

    first_frames = orchestrator.build_payloads()
    first_overview = first_frames["overview"].payload
    first_last_updated = first_overview["lastUpdatedUtc"]

    orchestrator.ingest(baseline_snapshot)

    second_frames = orchestrator.build_payloads()
    second_overview = second_frames["overview"].payload
    assert second_overview["lastUpdatedUtc"] == first_last_updated


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
    sensors_first = first_frames["telemetry"].payload["sensors"]
    extruder_first = next(sensor for sensor in sensors_first if sensor["channel"] == "extruder")
    first_last_updated = extruder_first["lastUpdatedUtc"]

    noise_update = {
        "method": "notify_status_update",
        "params": [{"extruder": {"temperature": 204.4, "target": 215.0}}],
    }

    orchestrator.ingest(noise_update)
    second_frames = orchestrator.build_payloads()
    sensors_second = second_frames["telemetry"].payload["sensors"]
    extruder_second = next(sensor for sensor in sensors_second if sensor["channel"] == "extruder")
    assert extruder_second["lastUpdatedUtc"] == first_last_updated

    change_update = {
        "method": "notify_status_update",
        "params": [{"extruder": {"temperature": 205.6, "target": 216.0}}],
    }

    orchestrator.ingest(change_update)
    third_frames = orchestrator.build_payloads()
    sensors_third = third_frames["telemetry"].payload["sensors"]
    extruder_third = next(sensor for sensor in sensors_third if sensor["channel"] == "extruder")
    assert extruder_third["lastUpdatedUtc"] != first_last_updated


def test_watch_window_flag_reflects_idle_mode(baseline_snapshot: dict) -> None:
    now = datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc)
    clock = FakeClock(now, now, now, now)
    orchestrator = TelemetryOrchestrator(clock=clock)
    orchestrator.ingest(baseline_snapshot)
    orchestrator.set_telemetry_mode(
        mode="idle", max_hz=0.033, watch_window_expires=None
    )

    frames = orchestrator.build_payloads()

    overview_flags = frames["overview"].payload["flags"]
    assert overview_flags["watchWindowActive"] is False
