"""Tests for the telemetry publisher."""

import asyncio
import copy
import gzip
import json
import time
from contextlib import suppress
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from moonraker_owl.config import (
    CameraConfig,
    CloudConfig,
    CommandConfig,
    CompressionConfig,
    LoggingConfig,
    MoonrakerConfig,
    OwlConfig,
    ResilienceConfig,
    TelemetryConfig,
    TelemetryCadenceConfig,
    DEFAULT_TELEMETRY_FIELDS,
    DEFAULT_TELEMETRY_EXCLUDE_FIELDS,
)
from moonraker_owl import constants
from moonraker_owl.telemetry import (
    TelemetryConfigurationError,
    TelemetryPublisher,
    PollSpec,
    build_subscription_manifest,
)
from moonraker_owl.telemetry.selectors import StatusSelector
from moonraker_owl.telemetry.state_store import MoonrakerStateStore
from moonraker_owl.telemetry.trackers import HeaterMonitor, PrintSessionTracker
from moonraker_owl.version import __version__ as PLUGIN_VERSION

EXPECTED_ORIGIN = f"moonraker-owl@{PLUGIN_VERSION}"


class FakeMoonrakerClient:
    def __init__(self, initial_state: Dict[str, Any]) -> None:
        self._callback = None
        self._initial_state = initial_state
        self.subscription_objects = None
        self.last_query_objects = None
        self.query_log: list[Optional[Dict[str, Optional[list[str]]]]] = []
        self.resubscribe_calls = 0
        self._available_heaters: Dict[str, list[str]] = {
            "available_heaters": [],
            "available_sensors": [],
        }

    async def start(self, callback):
        self._callback = callback

    def remove_callback(self, callback):
        if self._callback is callback:
            self._callback = None

    def set_subscription_objects(self, objects):
        self.subscription_objects = objects

    def set_available_heaters(self, heaters: list[str], sensors: list[str]) -> None:
        """Configure available heaters and sensors for testing."""
        self._available_heaters = {
            "available_heaters": heaters,
            "available_sensors": sensors,
        }

    async def fetch_available_heaters(self, timeout: float = 5.0) -> Dict[str, list[str]]:
        """Return configured available heaters and sensors."""
        return self._available_heaters

    async def fetch_printer_state(self, objects=None, timeout=5.0):
        self.last_query_objects = objects
        self.query_log.append(objects)
        return self._initial_state

    async def resubscribe(self) -> None:
        self.resubscribe_calls += 1

    async def execute_print_action(self, action: str) -> None:
        raise NotImplementedError

    async def aclose(self) -> None:
        pass

    async def emit(self, payload: Dict[str, Any]) -> None:
        if self._callback is None:
            return
        await self._callback(payload)

    def update_state(self, state: Dict[str, Any]) -> None:
        self._initial_state = state


class FakeMQTTClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 1,
        retain: bool = False,
        *,
        properties=None,
    ) -> None:
        self.messages.append(
            {
                "topic": topic,
                "payload": payload,
                "qos": qos,
                "retain": retain,
                "properties": properties,
            }
        )

    def by_topic(self) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for message in self.messages:
            grouped.setdefault(message["topic"], []).append(message)
        return grouped


def build_config(
    *,
    include_fields: Optional[list[str]] = None,
    rate_hz: float = 5.0,
    include_raw_payload: bool = False,
) -> OwlConfig:
    parser = ConfigParser()
    parser.add_section("cloud")
    parser.set("cloud", "device_id", "device-123")
    parser.set("cloud", "tenant_id", "tenant-42")
    parser.set("cloud", "printer_id", "printer-99")
    parser.set("cloud", "username", "tenant-42:device-123")

    config = OwlConfig(
        cloud=CloudConfig(
            base_url="https://api.owl.dev",
            broker_host="broker.owl.dev",
            broker_port=1883,
            username="tenant-42:device-123",
            password="token",
        ),
        moonraker=MoonrakerConfig(url="http://localhost:7125"),
        telemetry=TelemetryConfig(
            rate_hz=rate_hz,
            include_raw_payload=include_raw_payload,
            include_fields=include_fields or list(DEFAULT_TELEMETRY_FIELDS),
        ),
        telemetry_cadence=TelemetryCadenceConfig(),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        resilience=ResilienceConfig(),
        compression=CompressionConfig(),
        camera=CameraConfig(),
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )

    return config


def _load_sample(name: str) -> Dict[str, Any]:
    sample_path = Path(__file__).resolve().parents[2] / "docs" / "examples" / name
    return json.loads(sample_path.read_text(encoding="utf-8"))


def _decode(message: dict[str, Any]) -> Dict[str, Any]:
    """Decode a telemetry message payload, handling gzip compression if present."""
    payload = message["payload"]
    props = message.get("properties")
    content_type = getattr(props, "ContentType", None) if props else None

    if content_type == "application/gzip":
        payload = gzip.decompress(payload)

    return json.loads(payload.decode("utf-8"))


@pytest.mark.asyncio
async def test_publisher_emits_initial_full_snapshots() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=20.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.1)
    await publisher.stop()

    messages = mqtt.by_topic()
    assert messages, "Expected telemetry messages"

    props = mqtt.messages[0]["properties"]
    assert props is not None
    # Verify traceparent is set for distributed tracing
    user_props = getattr(props, "UserProperty", None)
    assert user_props is not None, "Expected traceparent in UserProperty"
    assert len(user_props) == 1
    assert user_props[0][0] == "traceparent"
    # Verify W3C trace context format: 00-{trace-id}-{span-id}-{flags}
    traceparent = user_props[0][1]
    parts = traceparent.split("-")
    assert len(parts) == 4
    assert parts[0] == "00"  # version
    assert len(parts[1]) == 32  # trace-id (32 hex chars)
    assert len(parts[2]) == 16  # span-id (16 hex chars)
    assert parts[3] in ("00", "01")  # trace-flags

    expected_qos = {"status": 1, "sensors": 0}
    for channel in ("status", "sensors"):
        topic = f"owl/printers/device-123/{channel}"
        assert topic in messages, f"Missing channel {channel}"
        first_message = messages[topic][0]
        assert first_message["qos"] == expected_qos[channel]
        document = _decode(first_message)
        assert document["kind"] == "full"
        assert document["_schema"] == 1
        assert document["deviceId"] == "device-123"
        assert document.get("tenantId") == "tenant-42"
        assert document.get("printerId") == "printer-99"
        assert document.get("_origin") == EXPECTED_ORIGIN
        assert document.get("_ts"), "Expected contract timestamp"
        assert document.get("_seq") is not None
        assert "raw" not in document, "Raw field should be excluded by default"

    sensors_doc = _decode(messages["owl/printers/device-123/sensors"][0])
    sensors_body = sensors_doc.get("sensors")
    assert isinstance(sensors_body, dict)
    sensors = sensors_body.get("sensors")
    assert isinstance(sensors, list) and sensors, "Expected sensors payload"
    extruder_sensor = next(
        (sensor for sensor in sensors if sensor.get("channel") == "extruder"),
        None,
    )
    assert extruder_sensor is not None
    assert extruder_sensor.get("type") == "heater"
    assert extruder_sensor.get("value") is not None
    assert extruder_sensor.get("target") is not None

    status_doc = _decode(messages["owl/printers/device-123/status"][0])
    status_contract = status_doc.get("status")
    assert isinstance(status_contract, dict)
    assert status_contract.get("printerStatus") == "Printing"
    assert status_contract.get("elapsedSeconds") is not None
    assert status_contract.get("estimatedTimeRemainingSeconds") is not None
    assert status_contract.get("subStatus")
    flags = status_contract.get("flags")
    assert isinstance(flags, dict)
    assert flags.get("hasActiveJob") is True
    assert flags.get("watchWindowActive") is False

    job_contract = status_contract.get("job")
    assert isinstance(job_contract, dict)
    assert job_contract.get("name")
    assert job_contract.get("progressPercent") is not None


@pytest.mark.asyncio
async def test_pipeline_emits_schema_envelopes() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=20.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.1)
    await publisher.stop()

    messages = mqtt.by_topic()
    sensors_topic = "owl/printers/device-123/sensors"
    status_topic = "owl/printers/device-123/status"

    assert sensors_topic in messages, "Expected sensors channel"
    assert status_topic in messages, "Expected status channel"

    sensors_envelope = _decode(messages[sensors_topic][0])
    assert sensors_envelope["_schema"] == 1
    assert sensors_envelope["kind"] == "full"
    assert sensors_envelope["_origin"] == EXPECTED_ORIGIN
    assert sensors_envelope.get("sensors")
    cadence = sensors_envelope["sensors"].get("cadence")
    assert isinstance(cadence, dict)
    assert cadence.get("mode") == "idle"

    sensors = sensors_envelope["sensors"].get("sensors")
    assert isinstance(sensors, list) and sensors

    status_envelope = _decode(messages[status_topic][0])
    assert status_envelope["_schema"] == 1
    assert status_envelope["kind"] == "full"
    status_payload = status_envelope.get("status")
    assert isinstance(status_payload, dict)
    assert status_payload.get("printerStatus")


def test_cancelled_print_drops_redundant_printing_message() -> None:
    store = MoonrakerStateStore()
    tracker = PrintSessionTracker()
    heater = HeaterMonitor()
    selector = StatusSelector()

    observed_at = datetime.now(timezone.utc)

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "printing"},
                    "display_status": {"message": "Printing"},
                    "virtual_sdcard": {"is_active": True, "progress": 0.25},
                    "idle_timeout": {"state": "Printing"},
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    status = selector.build(store, session, heater, observed_at)
    assert status is not None
    assert status.get("printerStatus") == "Printing"
    assert status.get("subStatus") == "Printing"

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "cancelled"},
                    "display_status": {},
                    "idle_timeout": {"state": "Printing"},
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    status = selector.build(store, session, heater, observed_at + timedelta(seconds=1))
    assert status is not None
    assert status.get("printerStatus") == "Cancelled"
    assert status.get("subStatus") is None
    assert session.message is None
    assert session.has_active_job is False


def test_pause_then_cancel_clears_paused_state() -> None:
    store = MoonrakerStateStore()
    tracker = PrintSessionTracker()
    heater = HeaterMonitor()
    selector = StatusSelector()

    observed_at = datetime.now(timezone.utc)

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "printing"},
                    "display_status": {"message": "Printing"},
                    "virtual_sdcard": {"is_active": True, "progress": 0.4},
                    "idle_timeout": {"state": "Printing"},
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
                    "job": {"job_id": "0002DD", "filename": "sample.gcode"},
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    status = selector.build(store, session, heater, observed_at)
    assert status is not None
    assert status.get("printerStatus") == "Printing"
    assert status.get("subStatus") == "Printing"
    assert session.has_active_job is True

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "paused"},
                    "display_status": {"message": "Paused"},
                    "idle_timeout": {"state": "Printing"},
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    status = selector.build(store, session, heater, observed_at + timedelta(seconds=1))
    assert status is not None
    assert status.get("printerStatus") == "Paused"
    assert status.get("subStatus") == "Paused"
    assert session.has_active_job is True

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_history_changed",
            "params": [
                {
                    "action": "finished",
                    "job": {
                        "job_id": "0002DD",
                        "filename": "sample.gcode",
                        "status": "cancelled",
                    },
                }
            ],
        }
    )
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "cancelled"},
                    "display_status": {"message": "Paused"},
                    "idle_timeout": {"state": "Printing"},
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    status = selector.build(store, session, heater, observed_at + timedelta(seconds=2))
    assert status is not None
    assert status.get("printerStatus") == "Cancelled"
    assert status.get("subStatus") is None
    assert session.message is None
    assert session.has_active_job is False


def test_completed_job_status_updates_via_query() -> None:
    """Test that print_stats.state query after history event provides correct state.

    Following Mainsail's pattern, we trust print_stats.state as authoritative.
    The TelemetryPublisher queries print_stats on action:finished to get the
    actual terminal state. This test simulates that flow.
    """
    store = MoonrakerStateStore()
    tracker = PrintSessionTracker()
    heater = HeaterMonitor()
    selector = StatusSelector()

    observed_at = datetime.now(timezone.utc)

    # Initial printing state
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "printing"},
                    "display_status": {"progress": 0.99},
                    "virtual_sdcard": {"is_active": True, "progress": 0.99},
                    "idle_timeout": {"state": "Printing"},
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
                    "job": {"job_id": "0003EE", "filename": "benchy.gcode"},
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    status = selector.build(store, session, heater, observed_at)
    assert status is not None
    assert status.get("printerStatus") == "Printing"
    assert session.has_active_job is True

    # Job finishes - simulate the query-on-notification pattern:
    # TelemetryPublisher receives action:finished and queries print_stats,
    # which returns the updated state
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_history_changed",
            "params": [
                {
                    "action": "finished",
                    "job": {
                        "job_id": "0003EE",
                        "filename": "benchy.gcode",
                        "status": "completed",
                    },
                }
            ],
        }
    )
    # Simulate the query result: print_stats.state is now "complete"
    # (This is what TelemetryPublisher._query_print_stats_on_job_start does)
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "query_result",  # Simulated query response
            "params": [{"print_stats": {"state": "complete"}}],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    # Now raw_state is "complete" from the query
    assert session.raw_state == "complete"
    assert session.has_active_job is False

    status = selector.build(store, session, heater, observed_at + timedelta(seconds=1))
    assert status is not None
    assert status.get("printerStatus") == "Completed"


def test_cancelled_job_status_updates_via_query() -> None:
    """Test that print_stats.state query after cancel provides correct state."""
    store = MoonrakerStateStore()
    tracker = PrintSessionTracker()
    heater = HeaterMonitor()
    selector = StatusSelector()

    observed_at = datetime.now(timezone.utc)

    # Initial printing state
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "printing"},
                    "virtual_sdcard": {"is_active": True, "progress": 0.5},
                    "idle_timeout": {"state": "Printing"},
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
                    "job": {"job_id": "0004FF", "filename": "test.gcode"},
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    assert session.has_active_job is True

    # Job cancelled - simulate query-on-notification pattern
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_history_changed",
            "params": [
                {
                    "action": "finished",
                    "job": {
                        "job_id": "0004FF",
                        "filename": "test.gcode",
                        "status": "cancelled",
                    },
                }
            ],
        }
    )
    # Simulate the query result: print_stats.state is now "cancelled"
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "query_result",
            "params": [{"print_stats": {"state": "cancelled"}}],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    assert session.raw_state == "cancelled"
    assert session.has_active_job is False

    status = selector.build(store, session, heater, observed_at + timedelta(seconds=1))
    assert status is not None
    assert status.get("printerStatus") == "Cancelled"


def test_new_print_after_cancel_shows_printing_not_cancelled() -> None:
    """Test that starting a new print immediately after cancellation shows Printing.

    Aligned with Mainsail/Obico: trust print_stats.state as authoritative.
    
    Timeline (race condition scenario):
    1. Previous print cancelled (action:finished, job_status=cancelled)
    2. User starts new print (Reprint Job in Mainsail)
    3. print_stats.state changes to "printing" (via WebSocket)
    4. Agent sends status message (should show "Printing" because we trust print_stats.state)
    5. notify_history_changed action:added arrives LATER → triggers query → confirms state

    With Mainsail-aligned approach, we simply trust print_stats.state="printing"
    regardless of stale job_status from history. No override logic needed.
    """
    store = MoonrakerStateStore()
    tracker = PrintSessionTracker()
    heater = HeaterMonitor()
    selector = StatusSelector()

    observed_at = datetime.now(timezone.utc)

    # Step 1: Previous print was cancelled
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "cancelled"},
                    "virtual_sdcard": {"is_active": False, "progress": 0.0},
                    "idle_timeout": {"state": "Idle"},
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
                    "action": "finished",
                    "job": {
                        "job_id": "0005GG",
                        "filename": "test.gcode",
                        "status": "cancelled",
                    },
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    assert session.job_status == "cancelled"
    assert session.history_action == "finished"

    # Verify it shows Cancelled initially (raw_state is "cancelled")
    status = selector.build(store, session, heater, observed_at)
    assert status is not None
    assert status.get("printerStatus") == "Cancelled"

    # Step 2: User starts a new print - print_stats.state changes to "printing"
    # With Mainsail-aligned logic, we trust this state immediately
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {"state": "printing"},  # New print started!
                    "virtual_sdcard": {"is_active": True, "progress": 0.01},
                    "idle_timeout": {"state": "Printing"},
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    # raw_state is now "printing" - this is what Mainsail trusts
    assert session.raw_state == "printing"
    # job_status may still be stale, but we don't use it for override
    assert session.job_status == "cancelled"  # Stale from previous print
    # has_active_job is True because raw_state="printing" (Mainsail-aligned)
    assert session.has_active_job is True
    assert session.progress_percent == 1.0

    # With Mainsail-aligned logic: printerStatus = "Printing" because
    # we trust print_stats.state="printing" directly, no override from job_status
    status = selector.build(store, session, heater, observed_at + timedelta(seconds=1))
    assert status is not None
    assert status.get("printerStatus") == "Printing"


@pytest.mark.asyncio
async def test_publisher_emits_status_full_update() -> None:
    sample = _load_sample("moonraker-sample-printing.json")
    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=50.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()

    # Update both display_status and virtual_sdcard to trigger progress change
    # (display_status.progress is the primary source for progress calculation)
    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "display_status": {"progress": 0.55},
                    "virtual_sdcard": {"progress": 0.55},
                }
            ],
        }
    )
    await asyncio.sleep(0.05)
    await publisher.stop()

    status_messages = mqtt.by_topic().get("owl/printers/device-123/status")
    assert status_messages, "Expected status updates"
    document = _decode(status_messages[-1])
    assert document["kind"] == "full"
    status = document.get("status")
    assert isinstance(status, dict)

    job = status.get("job")
    assert isinstance(job, dict)
    assert job.get("progressPercent") == pytest.approx(55.0, rel=1e-3)


@pytest.mark.asyncio
async def test_publisher_emits_events_channel() -> None:
    moonraker = FakeMoonrakerClient({"result": {}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=50.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()

    # Manually queue an event to ensure the publisher forwards orchestrator events.
    publisher._orchestrator.events.record_command_state(
        command_id="cmd-123",
        command_type="sensors:set-rate",
        state="completed",
        session_id="history-1",
    )

    await moonraker.emit({"result": {"status": {}}})
    await asyncio.sleep(0.05)
    await publisher.stop()

    events_messages = mqtt.by_topic().get("owl/printers/device-123/events")
    assert events_messages, "Expected events payload"
    last_event_message = events_messages[-1]
    assert last_event_message["qos"] == 2
    document = _decode(last_event_message)
    assert document["kind"] == "full"
    assert document["_schema"] == 1
    events_body = document.get("events")
    assert isinstance(events_body, dict)
    items = events_body.get("items")
    assert isinstance(items, list) and items, "Expected contract events"
    entry = items[0]
    assert entry.get("eventName") == "system:command-state"
    data = entry.get("data")
    assert isinstance(data, dict)
    assert data.get("state") == "completed"


@pytest.mark.asyncio
async def test_subscription_skips_excluded_objects() -> None:
    moonraker = FakeMoonrakerClient({"result": {}})
    mqtt = FakeMQTTClient()
    config = build_config()
    config.telemetry.exclude_fields = list(DEFAULT_TELEMETRY_EXCLUDE_FIELDS)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    expected_objects = build_subscription_manifest(
        DEFAULT_TELEMETRY_FIELDS, DEFAULT_TELEMETRY_EXCLUDE_FIELDS
    )

    assert moonraker.subscription_objects == expected_objects


@pytest.mark.asyncio
async def test_subscription_handles_attribute_selection() -> None:
    moonraker = FakeMoonrakerClient({"result": {}})
    mqtt = FakeMQTTClient()
    config = build_config(
        include_fields=[
            "toolhead.position",
            "toolhead.velocity",
            "virtual_sdcard",
        ]
    )
    config.telemetry.exclude_fields = []

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    assert moonraker.subscription_objects == {
        "fan": ["speed"],  # Part cooling fan always subscribed
        "toolhead": ["position", "velocity"],
        "virtual_sdcard": None,
    }


def test_state_store_marks_klippy_shutdown() -> None:
    store = MoonrakerStateStore()

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "webhooks": {"state": "ready"},
                    "print_stats": {"state": "standby", "message": ""},
                    "printer": {"state": "ready", "is_shutdown": False},
                }
            ],
        }
    )

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_klippy_disconnected",
            "params": [{"message": "USB cable removed"}],
        }
    )

    snapshot = store.as_dict()
    assert snapshot.get("webhooks", {}).get("state") == "shutdown"
    assert snapshot.get("webhooks", {}).get("state_message") == "USB cable removed"
    assert snapshot.get("print_stats", {}).get("state") == "error"
    assert snapshot.get("print_stats", {}).get("message") == "USB cable removed"
    assert snapshot.get("printer", {}).get("is_shutdown") is True


def test_state_store_marks_klippy_ready_after_shutdown() -> None:
    """Test that klippy ready clears shutdown but does NOT reset print_stats.

    After klippy ready, print_stats retains its state from the shutdown phase.
    The TelemetryPublisher is responsible for querying Moonraker for the actual
    print_stats state (ADR-0003 pattern). This avoids spurious printStarted
    events when recovering from emergency stop.
    """
    store = MoonrakerStateStore()

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_klippy_shutdown",
            "params": [{"message": "Emergency stop"}],
        }
    )

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_klippy_ready",
            "params": None,
        }
    )

    snapshot = store.as_dict()
    # Klippy state is correctly marked as ready
    assert snapshot.get("webhooks", {}).get("state") == "ready"
    assert snapshot.get("printer", {}).get("state") == "ready"
    assert snapshot.get("printer", {}).get("is_shutdown") is False
    # print_stats is NOT reset - it retains the shutdown-phase state.
    # TelemetryPublisher queries Moonraker for actual state on klippy ready.
    assert snapshot.get("print_stats", {}).get("state") == "error"
    assert snapshot.get("print_stats", {}).get("message") == "Emergency stop"


def test_state_store_retains_shutdown_until_ready_signal() -> None:
    """Test that shutdown state is retained until explicit klippy_ready.

    The simplified approach:
    - webhooks.state and printer.is_shutdown track klippy connectivity
    - print_stats reflects actual Moonraker data (no inference/override)
    - Consumers should check printer.is_shutdown to determine if device is usable
    """
    store = MoonrakerStateStore()

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_klippy_shutdown",
            "params": [{"message": "Firmware restart"}],
        }
    )

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_print_stats_update",
            "params": [{"state": "standby", "message": ""}],
        }
    )

    snapshot = store.as_dict()
    # Klippy state correctly reflects shutdown
    assert snapshot.get("webhooks", {}).get("state") == "shutdown"
    assert snapshot.get("printer", {}).get("is_shutdown") is True
    # print_stats reflects actual Moonraker data, no override
    # (consumers check printer.is_shutdown for device usability)
    assert snapshot.get("print_stats", {}).get("state") == "standby"
    assert snapshot.get("print_stats", {}).get("message") == ""

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_klippy_ready",
            "params": None,
        }
    )

    snapshot = store.as_dict()
    assert snapshot.get("webhooks", {}).get("state") == "ready"
    assert snapshot.get("printer", {}).get("is_shutdown") is False
    assert snapshot.get("print_stats", {}).get("state") == "standby"
    assert snapshot.get("print_stats", {}).get("message") == ""


def test_state_store_handles_notify_klippy_state_ready() -> None:
    """Test that notify_klippy_state ready does NOT reset print_stats.

    When klippy becomes ready via notify_klippy_state, we update webhooks and
    printer sections but do NOT reset print_stats. The TelemetryPublisher
    queries Moonraker for actual print_stats state (ADR-0003 pattern).
    """
    store = MoonrakerStateStore()

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_klippy_state",
            "params": ["ready", {"message": "Firmware restart complete"}],
        }
    )

    snapshot = store.as_dict()
    assert snapshot.get("webhooks", {}).get("state") == "ready"
    assert (
        snapshot.get("webhooks", {}).get("state_message") == "Firmware restart complete"
    )
    assert snapshot.get("printer", {}).get("state") == "ready"
    assert snapshot.get("printer", {}).get("is_shutdown") is False
    # print_stats is NOT set - TelemetryPublisher queries for actual state
    assert snapshot.get("print_stats", {}).get("state") is None


def test_state_store_handles_notify_klippy_state_error() -> None:
    store = MoonrakerStateStore()

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_klippy_state",
            "params": {"state": "error", "message": "Emergency stop"},
        }
    )

    snapshot = store.as_dict()
    assert snapshot.get("webhooks", {}).get("state") == "shutdown"
    assert snapshot.get("printer", {}).get("is_shutdown") is True
    assert snapshot.get("print_stats", {}).get("state") == "error"


def test_state_store_export_restore_preserves_shutdown_state() -> None:
    """Test that export/restore preserves klippy shutdown tracking.

    The simplified approach tracks shutdown via printer.is_shutdown,
    not by overriding print_stats.
    """
    store = MoonrakerStateStore()

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_klippy_shutdown",
            "params": [{"message": "Emergency stop"}],
        }
    )

    snapshot = store.export_state()

    recovered = MoonrakerStateStore()
    recovered.restore_state(snapshot)

    recovered.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_print_stats_update",
            "params": [{"state": "standby", "message": ""}],
        }
    )

    state = recovered.as_dict()
    # Shutdown state is preserved via printer.is_shutdown
    assert state.get("printer", {}).get("is_shutdown") is True
    assert state.get("webhooks", {}).get("state") == "shutdown"
    # print_stats reflects actual Moonraker data, no override
    assert state.get("print_stats", {}).get("state") == "standby"
    assert state.get("print_stats", {}).get("message") == ""


# NOTE: test_state_store_prefers_gcode_shutdown_hint was removed.
# We no longer extract shutdown hints from gcode_response - we trust
# explicit klippy notifications directly (moonraker-obico pattern).


def test_state_store_print_state_normalizes_to_lowercase() -> None:
    """Test that print_state property normalizes state to lowercase.
    
    This ensures consistent matching with PRINT_STATE_TRANSITIONS lookups,
    which use lowercase keys like ('standby', 'printing').
    """
    store = MoonrakerStateStore()

    # Test with various case variations that Moonraker might return
    test_cases = [
        ("standby", "standby"),
        ("PRINTING", "printing"),
        ("Paused", "paused"),
        ("Complete", "complete"),
        ("  Cancelled  ", "cancelled"),  # With whitespace
        ("ERROR", "error"),
    ]

    for raw_state, expected in test_cases:
        store.ingest(
            {
                "jsonrpc": "2.0",
                "method": "notify_status_update",
                "params": [{"print_stats": {"state": raw_state}}],
            }
        )
        assert store.print_state == expected, f"Expected {expected!r} for input {raw_state!r}"

    # Test None handling
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [{"print_stats": {"state": None}}],
        }
    )
    assert store.print_state is None


def test_state_store_handles_notify_klippy_state_mapping_sequence() -> None:
    store = MoonrakerStateStore()

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_klippy_state",
            "params": [
                {"state": "ready", "state_message": "Restart complete"},
                {"message": "Restart complete"},
            ],
        }
    )

    snapshot = store.as_dict()
    assert snapshot.get("webhooks", {}).get("state") == "ready"
    assert snapshot.get("webhooks", {}).get("state_message") == "Restart complete"
    assert snapshot.get("printer", {}).get("state") == "ready"
    assert snapshot.get("printer", {}).get("is_shutdown") is False


@pytest.mark.asyncio
async def test_subscription_normalizes_field_names() -> None:
    moonraker = FakeMoonrakerClient({"result": {}})
    mqtt = FakeMQTTClient()
    config = build_config(
        include_fields=[
            '"print_stats"',
            " 'toolhead.position' ",
            ' "temperature_sensor ambient" ',
        ]
    )
    config.telemetry.exclude_fields = [' "moonraker_stats" ']

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    assert moonraker.subscription_objects == {
        "fan": ["speed"],  # Part cooling fan always subscribed
        "print_stats": None,
        "temperature_sensor ambient": ["temperature"],  # Sensors only have temperature
        "toolhead": ["position"],
    }


@pytest.mark.asyncio
async def test_polling_fetches_unsubscribed_objects() -> None:
    initial_state = {"result": {"status": {}}}
    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(include_fields=["print_stats"])

    poll_spec = PollSpec(
        name="test-poll",
        fields=("temperature_sensor ambient",),
        interval_seconds=0.05,
        initial_delay_seconds=0.0,
    )

    publisher = TelemetryPublisher(
        config,
        moonraker,
        mqtt,
        poll_specs=(poll_spec,),
    )

    await publisher.start()

    moonraker.update_state(
        {
            "result": {
                "status": {
                    "temperature_sensor ambient": {"temperature": 27.3},
                }
            }
        }
    )

    await asyncio.sleep(0.1)

    topics = mqtt.by_topic()
    assert topics, f"Expected sensors replay topics, found: {topics}"

    replay_messages = topics.get("owl/printers/device-123/sensors")
    polled_objects = next(
        (
            entry
            for entry in reversed(moonraker.query_log)
            if isinstance(entry, dict) and "temperature_sensor ambient" in entry
        ),
        None,
    )
    assert polled_objects is not None, "Expected poller to query ambient sensor"

    snapshot = publisher._orchestrator.store.get("temperature_sensor ambient")
    assert snapshot is not None
    assert snapshot.data.get("temperature") == 27.3


@pytest.mark.asyncio
async def test_heater_merge_forces_telemetry_publish() -> None:
    initial_state = {
        "result": {
            "status": {
                "extruder": {"temperature": 41.0, "target": 0.0},
                "heater_bed": {"temperature": 39.0, "target": 0.0},
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=20.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()
    await asyncio.sleep(0.15)

    moonraker.update_state(
        {
            "result": {
                "status": {
                    "extruder": {"temperature": 72.5, "target": 100.0},
                    "heater_bed": {"temperature": 39.0, "target": 0.0},
                }
            }
        }
    )

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "extruder": {"temperature": 72.5},
                    "heater_bed": {"temperature": 39.0},
                }
            ],
        }
    )

    await asyncio.sleep(0.1)
    await publisher.stop()

    sensors_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert sensors_messages, "Expected sensors publish after heater ramp"
    assert len(sensors_messages) == 1

    document = _decode(sensors_messages[0])
    assert document.get("_seq", 0) > 1

    sensors_body = document.get("sensors")
    assert isinstance(sensors_body, dict)
    sensors = sensors_body.get("sensors")
    assert isinstance(sensors, list)
    extruder_sensor = next(
        sensor for sensor in sensors if sensor.get("channel") == "extruder"
    )
    assert extruder_sensor.get("value") == pytest.approx(72.0, rel=1e-3)
    assert extruder_sensor.get("target") == pytest.approx(0.0, abs=1e-6)


@pytest.mark.asyncio
async def test_publish_system_status_publishes_error_snapshot() -> None:
    """Test that publish_system_status sends an error status snapshot."""
    initial_state = {
        "result": {
            "status": {
                "print_stats": {
                    "state": "standby",
                    "message": "",
                }
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config()

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()

    await publisher.publish_system_status(
        printer_state="error", message="Emergency stop"
    )

    await asyncio.sleep(0.05)
    await publisher.stop()

    status_messages = [
        message for message in mqtt.messages if message["topic"].endswith("/status")
    ]

    assert status_messages, "Expected status publication"
    error_message = status_messages[-1]
    assert error_message["retain"] is False
    document = _decode(error_message)
    status = document.get("status")
    assert isinstance(status, dict)
    assert status.get("printerStatus") == "Error"
    assert status.get("subStatus") == "Emergency stop"


def test_telemetry_configuration_requires_device_id():
    parser = ConfigParser()
    parser.add_section("cloud")

    config = OwlConfig(
        cloud=CloudConfig(),
        moonraker=MoonrakerConfig(),
        telemetry=TelemetryConfig(),
        telemetry_cadence=TelemetryCadenceConfig(),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        resilience=ResilienceConfig(),
        compression=CompressionConfig(),
        camera=CameraConfig(),
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )

    with pytest.raises(TelemetryConfigurationError):
        TelemetryPublisher(
            config, FakeMoonrakerClient({}), FakeMQTTClient(), poll_specs=()
        )


def test_telemetry_configuration_requires_device_id_v2():
    """Test that TelemetryPublisher raises error when device_id is missing."""
    parser = ConfigParser()
    parser.add_section("cloud")
    # No device_id set

    config = OwlConfig(
        cloud=CloudConfig(),
        moonraker=MoonrakerConfig(),
        telemetry=TelemetryConfig(),
        telemetry_cadence=TelemetryCadenceConfig(),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        resilience=ResilienceConfig(),
        compression=CompressionConfig(),
        camera=CameraConfig(),
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )

    with pytest.raises(TelemetryConfigurationError, match="Device ID is required"):
        TelemetryPublisher(
            config, FakeMoonrakerClient({}), FakeMQTTClient(), poll_specs=()
        )


@pytest.mark.asyncio
async def test_heater_subscriptions_include_temperature_and_target_fields() -> None:
    """Test that heater objects are subscribed with explicit temperature and target fields."""
    moonraker = FakeMoonrakerClient({"result": {}})
    mqtt = FakeMQTTClient()
    config = build_config(
        include_fields=[
            "extruder",
            "heater_bed",
            "temperature_sensor ambient",
        ]
    )
    config.telemetry.exclude_fields = []

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    # Verify heater objects are subscribed with temperature and target fields
    # Temperature sensors only subscribe to temperature (no target to set)
    assert moonraker.subscription_objects == {
        "extruder": ["temperature", "target"],
        "fan": ["speed"],  # Part cooling fan always subscribed
        "heater_bed": ["temperature", "target"],
        "temperature_sensor ambient": ["temperature"],  # Sensors only have temperature
    }


@pytest.mark.asyncio
async def test_temperature_target_preserved_across_updates() -> None:
    """Test that target temperature is preserved when omitted in subsequent updates."""
    initial_state = {
        "result": {
            "status": {
                "extruder": {
                    "temperature": 25.0,
                    "target": 210.0,
                },
                "heater_bed": {
                    "temperature": 22.0,
                    "target": 60.0,
                },
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=50.0, include_fields=["extruder", "heater_bed"])

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    # Initial message should have both temperature and target
    mqtt.messages.clear()
    await asyncio.sleep(0.15)

    # Emit update with only temperature (target omitted, simulating Moonraker behavior)
    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "extruder": {
                        "temperature": 196.3,  # floors to 196
                        # target intentionally omitted
                    },
                    "heater_bed": {
                        "temperature": 58.9,  # floors to 58
                        # target intentionally omitted
                    },
                }
            ],
        }
    )

    await asyncio.sleep(0.05)
    await publisher.stop()

    # Verify the target was preserved
    sensors_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert sensors_messages, "Expected sensors updates"

    document = _decode(sensors_messages[-1])
    sensors_body = document.get("sensors")
    assert isinstance(sensors_body, dict)
    sensors = sensors_body.get("sensors")
    assert isinstance(sensors, list) and sensors

    def _find(channel: str) -> Dict[str, Any]:
        for sensor in sensors:
            if sensor.get("channel") == channel:
                return sensor
        raise AssertionError(f"Missing sensor {channel}")

    extruder_sensor = _find("extruder")
    assert extruder_sensor.get("value") == pytest.approx(196.0, rel=1e-3)  # 196.3 floors to 196
    assert extruder_sensor.get("target") == pytest.approx(210.0, rel=1e-3)

    bed_sensor = _find("heater_bed")
    assert bed_sensor.get("value") == pytest.approx(58.0, rel=1e-3)  # 58.9 floors to 58
    assert bed_sensor.get("target") == pytest.approx(60.0, rel=1e-3)


@pytest.mark.asyncio
async def test_fan_sensor_emits_speed_as_percent() -> None:
    """Test that fan sensor emits speed as percentage (0-100) not raw value (0.0-1.0)."""
    initial_state = {
        "result": {
            "status": {
                "extruder": {
                    "temperature": 200.0,
                    "target": 200.0,
                },
                "fan": {
                    "speed": 0.75,  # 75% fan speed
                },
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=50.0, include_fields=["extruder", "fan"])

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    sensors_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert sensors_messages, "Expected sensors updates"

    document = _decode(sensors_messages[-1])
    sensors_body = document.get("sensors")
    assert isinstance(sensors_body, dict)
    sensors = sensors_body.get("sensors")
    assert isinstance(sensors, list) and sensors

    # Find the fan sensor
    fan_sensor = None
    for sensor in sensors:
        if sensor.get("channel") == "fan":
            fan_sensor = sensor
            break

    assert fan_sensor is not None, "Expected fan sensor"
    assert fan_sensor.get("type") == "fan"
    assert fan_sensor.get("unit") == "percent"
    assert fan_sensor.get("value") == pytest.approx(75.0, rel=1e-3)  # 0.75 * 100 = 75%
    assert fan_sensor.get("sourceObject") == "fan"
    assert "target" not in fan_sensor  # Fans don't have target
    assert "canSetTarget" not in fan_sensor  # Derived from type, not sent explicitly


@pytest.mark.asyncio
async def test_fractional_temperature_changes_dont_emit_after_floor_rounding() -> None:
    initial_state = {
        "result": {
            "status": {
                "extruder": {
                    "temperature": 195.0,
                    "target": 200.0,
                }
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=50.0, include_fields=["extruder"])

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()
    await asyncio.sleep(0.15)

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "extruder": {
                        "temperature": 195.4,
                    }
                }
            ],
        }
    )

    await asyncio.sleep(0.1)

    first_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert not first_messages, "Expected fractional change to be suppressed by rounding"

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "extruder": {
                        "temperature": 196.2,
                    }
                }
            ],
        }
    )

    await asyncio.sleep(0.1)
    await publisher.stop()

    sensors_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert sensors_messages, "Expected integer-scale change to publish sensors"

    document = _decode(sensors_messages[-1])
    sensors_body = document.get("sensors")
    assert isinstance(sensors_body, dict)
    sensors = sensors_body.get("sensors")
    assert isinstance(sensors, list)
    extruder_sensor = next(
        sensor for sensor in sensors if sensor.get("channel") == "extruder"
    )
    assert extruder_sensor.get("value") == pytest.approx(196.0, rel=1e-3)


@pytest.mark.asyncio
async def test_idle_cadence_flushes_latest_payload() -> None:
    initial_state = {
        "result": {
            "status": {
                "extruder": {
                    "temperature": 40.0,
                    "target": 60.0,
                }
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=0.5, include_fields=["extruder"])

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "extruder": {
                        "temperature": 50.0,
                    }
                }
            ],
        }
    )

    await asyncio.sleep(0.1)

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "extruder": {
                        "temperature": 60.0,
                    }
                }
            ],
        }
    )

    await asyncio.sleep(2.5)
    await publisher.stop()

    sensors_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert sensors_messages, "Expected deferred payload to flush"

    document = _decode(sensors_messages[-1])
    sensors_body = document.get("sensors")
    assert isinstance(sensors_body, dict)
    sensors = sensors_body.get("sensors")
    assert isinstance(sensors, list)

    extruder_sensor = next(
        sensor for sensor in sensors if sensor.get("channel") == "extruder"
    )
    assert extruder_sensor.get("value") == pytest.approx(60.0, rel=1e-3)


@pytest.mark.asyncio
async def test_restart_fetches_fresh_status_state() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    initial_messages = mqtt.by_topic().get("owl/printers/device-123/status")
    assert initial_messages, "Expected initial status publish"

    await publisher.stop()
    mqtt.messages.clear()

    moonraker.update_state(_load_sample("moonraker-sample-complete.json"))

    await publisher.start()
    await asyncio.sleep(0.05)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 0.3
    latest_status: Optional[str] = None

    while loop.time() < deadline:
        resumed_messages = mqtt.by_topic().get("owl/printers/device-123/status") or []
        if resumed_messages:
            last_message = _decode(resumed_messages[-1])
            status_body = last_message.get("status")
            if isinstance(status_body, dict):
                latest_status = status_body.get("printerStatus")
                if latest_status == "Completed":
                    break
        await asyncio.sleep(0.01)

    assert (
        latest_status == "Completed"
    ), f"Expected status Completed but saw {latest_status!r}"

    resumed_messages = mqtt.by_topic().get("owl/printers/device-123/status") or []
    assert resumed_messages, "Expected status publish after restart"
    # Agent no longer uses retained messages, all messages are retain=false
    assert all(not message["retain"] for message in resumed_messages)

    await publisher.stop()


@pytest.mark.asyncio
async def test_start_recovers_after_loop_termination() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    assert publisher._worker is not None  # type: ignore[attr-defined]
    publisher._worker.cancel()  # type: ignore[attr-defined]
    with suppress(asyncio.CancelledError):
        await publisher._worker  # type: ignore[attr-defined]

    await publisher.start()
    await asyncio.sleep(0.05)

    status_messages = mqtt.by_topic().get("owl/printers/device-123/status")
    assert status_messages, "Expected status publish after recovering start"

    await publisher.stop()


@pytest.mark.asyncio
async def test_restart_emits_current_sensors_after_start() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    initial_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert initial_messages, "Expected sensors publish on first start"
    initial_payload = _decode(initial_messages[-1])
    initial_sensors = initial_payload.get("sensors", {})
    initial_extruder = next(
        (
            sensor
            for sensor in initial_sensors.get("sensors", [])
            if sensor.get("channel") == "extruder"
        ),
        None,
    )
    assert initial_extruder is not None
    assert initial_extruder.get("target") == pytest.approx(255.0, rel=1e-3)

    await publisher.stop()
    mqtt.messages.clear()
    moonraker.update_state(_load_sample("moonraker-sample-complete.json"))

    await publisher.start()
    await asyncio.sleep(0.05)
    assert publisher._worker is not None
    assert not publisher._worker.done()

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 1.5
    extruder_sensor = None

    while loop.time() < deadline:
        replay_messages = mqtt.by_topic().get("owl/printers/device-123/sensors") or []
        if replay_messages:
            latest_payload = _decode(replay_messages[-1])
            sensors_body = latest_payload.get("sensors", {})
            sensors = sensors_body.get("sensors", [])
            candidate = next(
                (sensor for sensor in sensors if sensor.get("channel") == "extruder"),
                None,
            )
            if candidate is not None:
                extruder_sensor = candidate
                target = extruder_sensor.get("target")
                if target is not None and abs(target - 0.0) <= 1e-6:
                    break
        await asyncio.sleep(0.01)

    assert extruder_sensor is not None, "Expected extruder sensor sensors after restart"
    assert extruder_sensor.get("target") == pytest.approx(0.0, abs=1e-6)
    assert extruder_sensor.get("value") == pytest.approx(45.04, rel=1e-3)


@pytest.mark.asyncio
async def test_restart_replays_full_status_when_state_unchanged() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    await publisher.stop()
    mqtt.messages.clear()

    await publisher.start()
    await asyncio.sleep(0.05)

    topics = mqtt.by_topic()
    status_messages = topics.get("owl/printers/device-123/status")
    sensors_messages = topics.get("owl/printers/device-123/sensors")

    assert status_messages, "Expected status publish after restart"
    assert sensors_messages, "Expected sensors publish after restart"

    status_document = _decode(status_messages[-1])
    sensors_document = _decode(sensors_messages[-1])

    assert status_document.get("kind") == "full"
    assert sensors_document.get("kind") == "full"
    assert isinstance(status_document.get("status"), dict)
    assert isinstance(sensors_document.get("sensors"), dict)

    assert not publisher._force_full_channels_after_reset  # type: ignore[attr-defined]

    await publisher.stop()


@pytest.mark.asyncio
async def test_publish_system_status_emits_error_snapshot() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    mqtt.messages.clear()

    await publisher.publish_system_status(
        printer_state="error",
        message="Moonraker unavailable",
    )

    status_messages = mqtt.by_topic().get("owl/printers/device-123/status")
    assert status_messages, "Expected status publish for system status"

    snapshot = json.loads(status_messages[-1]["payload"].decode("utf-8"))
    status_body = snapshot.get("status")
    assert status_body is not None
    assert status_body.get("printerStatus") == "Error"
    assert status_body.get("subStatus") == "Moonraker unavailable"
    assert status_messages[-1]["retain"] is False

    await publisher.stop()


@pytest.mark.asyncio
async def test_status_listener_invoked_for_system_status() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()

    captured: list[Dict[str, Any]] = []

    def _listener(payload: Dict[str, Any]) -> None:
        captured.append(payload)

    publisher.register_status_listener(_listener)

    await publisher.publish_system_status(
        printer_state="error",
        message="Moonraker unavailable",
    )

    assert captured, "Expected listener to receive status payload"
    snapshot = captured[-1]
    status = snapshot.get("result", {}).get("status", {})
    assert status.get("print_stats", {}).get("state") == "error"

    await publisher.stop()


def test_watch_window_expiration_reverts_to_idle_rate() -> None:
    request_at = datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc)

    moonraker = FakeMoonrakerClient({"result": {"status": {}}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    expires_at = publisher.apply_sensors_rate(
        mode="watch",
        max_hz=2.0,
        duration_seconds=90,
        requested_at=request_at,
    )

    assert publisher._current_mode == "watch"
    assert expires_at == request_at + timedelta(seconds=90)

    publisher._maybe_expire_watch_window(now=expires_at + timedelta(seconds=1))

    assert publisher._current_mode == "idle"
    assert publisher._watch_window_expires is None
    schedule = publisher._cadence_controller.get_schedule("sensors")  # type: ignore[attr-defined]
    assert schedule.interval == pytest.approx(
        publisher._idle_interval,
        rel=1e-6,
    )
    assert schedule.forced_interval == pytest.approx(
        publisher._idle_interval,
        rel=1e-6,
    )


def test_forced_interval_tracks_watch_mode() -> None:
    moonraker = FakeMoonrakerClient({"result": {"status": {}}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    idle_interval = publisher._idle_interval
    schedule = publisher._cadence_controller.get_schedule("sensors")  # type: ignore[attr-defined]
    assert schedule.interval == pytest.approx(idle_interval, rel=1e-6)
    assert schedule.forced_interval == pytest.approx(idle_interval, rel=1e-6)

    watch_expires = publisher.apply_sensors_rate(
        mode="watch",
        max_hz=1.0,
        duration_seconds=120,
        requested_at=datetime.now(timezone.utc),
    )
    assert watch_expires is not None

    watch_schedule = publisher._cadence_controller.get_schedule("sensors")  # type: ignore[attr-defined]
    assert watch_schedule.interval == pytest.approx(1.0, rel=1e-6)
    assert watch_schedule.forced_interval == pytest.approx(1.0, rel=1e-6)

    publisher.apply_sensors_rate(
        mode="idle",
        max_hz=1.0 / idle_interval if idle_interval > 0 else 0.0,
        duration_seconds=None,
        requested_at=datetime.now(timezone.utc),
    )

    reverted_schedule = publisher._cadence_controller.get_schedule("sensors")  # type: ignore[attr-defined]
    assert reverted_schedule.interval == pytest.approx(idle_interval, rel=1e-6)
    assert reverted_schedule.forced_interval == pytest.approx(idle_interval, rel=1e-6)


def test_reset_runtime_state_requeues_previous_snapshot() -> None:
    moonraker = FakeMoonrakerClient({"result": {"status": {}}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    snapshot = {
        "result": {
            "status": {
                "print_stats": {"state": "standby"},
                "temperature_sensor extruder": {
                    "temperature": 25.0,
                    "target": 0.0,
                },
            }
        }
    }

    publisher._last_payload_snapshot = copy.deepcopy(snapshot)
    publisher._current_mode = "watch"
    publisher._pending_payload = None
    publisher._pending_channels.clear()  # type: ignore[attrdefined]

    publisher._reset_runtime_state()

    assert publisher._pending_payload == snapshot
    pending_channels = publisher._pending_channels  # type: ignore[attrdefined]
    assert {"status", "sensors"}.issubset(pending_channels.keys())
    for channel in ("status", "sensors"):
        entry = pending_channels[channel]
        assert entry.forced is True
        assert entry.respect_cadence is False
    assert publisher._last_payload_snapshot == snapshot


def test_reset_runtime_state_preserves_print_state_when_requested() -> None:
    """Test that preserve_print_state=True retains StateStore across reset.
    
    This is critical for token renewal reconnections: we don't want to
    emit spurious print:started events when the print is still in progress.
    """
    moonraker = FakeMoonrakerClient({"result": {"status": {}}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    # Simulate an active print - set up state store with printing state
    printing_payload = {
        "result": {
            "status": {
                "print_stats": {
                    "state": "printing",
                    "filename": "test.gcode",
                },
            }
        }
    }
    publisher._orchestrator.store.ingest(printing_payload)
    
    # Verify initial state
    assert publisher._orchestrator.store.print_state == "printing"
    assert publisher._orchestrator.store.print_filename == "test.gcode"

    # Reset WITH preserve_print_state=True (simulating token renewal)
    publisher._reset_runtime_state(preserve_print_state=True)

    # State should be preserved
    assert publisher._orchestrator.store.print_state == "printing"
    assert publisher._orchestrator.store.print_filename == "test.gcode"

    # CRITICAL: Orchestrator tracking state must also be preserved to prevent
    # spurious print:started events when the same print_state is re-ingested
    assert publisher._orchestrator._last_print_state == "printing"
    assert publisher._orchestrator._last_filename == "test.gcode"


def test_reset_runtime_state_clears_print_state_by_default() -> None:
    """Test that default reset clears StateStore (existing behavior)."""
    moonraker = FakeMoonrakerClient({"result": {"status": {}}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    # Simulate an active print
    printing_payload = {
        "result": {
            "status": {
                "print_stats": {
                    "state": "printing",
                    "filename": "test.gcode",
                },
            }
        }
    }
    publisher._orchestrator.store.ingest(printing_payload)
    
    # Verify initial state
    assert publisher._orchestrator.store.print_state == "printing"

    # Reset WITHOUT preserve_print_state (default behavior)
    publisher._reset_runtime_state(preserve_print_state=False)

    # State should be cleared
    assert publisher._orchestrator.store.print_state is None


def test_preserve_print_state_prevents_spurious_print_started_event() -> None:
    """Test that no spurious print:started event is emitted after token renewal.
    
    This is a regression test for the bug where token renewal during a print
    would cause a new print:started event because the orchestrator's tracking
    state was reset even though the StateStore was preserved.
    
    The fix ensures that when preserve_print_state=True, the orchestrator's
    _last_print_state is initialized from the restored StateStore, preventing
    the (None -> printing) transition that would emit a spurious event.
    """
    moonraker = FakeMoonrakerClient({"result": {"status": {}}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    # 1. Start a print - this should emit one print:started event
    start_payload = {
        "result": {
            "status": {
                "print_stats": {
                    "state": "printing",
                    "filename": "test.gcode",
                },
            }
        }
    }
    publisher._orchestrator.ingest(start_payload)
    
    # Harvest the initial print:started event
    initial_events = publisher._orchestrator.events.harvest()
    started_events = [e for e in initial_events if e.event_name.value == "print:started"]
    assert len(started_events) == 1, "Expected exactly one print:started event"

    # 2. Simulate token renewal while printing
    publisher._reset_runtime_state(preserve_print_state=True)

    # 3. Re-ingest the same printing state (simulating _prime_initial_state)
    # This should NOT emit another print:started event
    publisher._orchestrator.ingest(start_payload)

    # Check for spurious events
    post_renewal_events = publisher._orchestrator.events.harvest()
    spurious_started = [e for e in post_renewal_events if e.event_name.value == "print:started"]
    assert len(spurious_started) == 0, (
        f"Spurious print:started event after token renewal! "
        f"_last_print_state={publisher._orchestrator._last_print_state}"
    )


@pytest.mark.asyncio
async def test_resubscribe_triggered_after_klippy_ready() -> None:
    initial_state = {
        "result": {
            "status": {
                "printer": {"state": "shutdown"},
                "print_stats": {"state": "error", "message": "Restarting"},
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    await moonraker.emit({"method": "notify_klippy_ready", "params": None})
    await asyncio.sleep(0.1)

    assert (
        moonraker.resubscribe_calls >= 1
    ), "Expected Moonraker resubscribe after Klippy ready notification"

    await publisher.stop()


@pytest.mark.asyncio
async def test_resubscribe_triggered_after_notify_klippy_state_ready() -> None:
    initial_state = {
        "result": {
            "status": {
                "printer": {"state": "startup"},
                "print_stats": {"state": "startup"},
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    await moonraker.emit(
        {
            "method": "notify_klippy_state",
            "params": [{"state": "ready"}],
        }
    )
    await asyncio.sleep(0.1)

    first_calls = moonraker.resubscribe_calls
    assert first_calls >= 1, "Expected resubscribe after notify_klippy_state ready"

    await moonraker.emit(
        {
            "method": "notify_klippy_state",
            "params": {"state": "ready"},
        }
    )
    await asyncio.sleep(0.05)

    assert (
        moonraker.resubscribe_calls == first_calls
    ), "Duplicate ready notifications should not reschedule resubscribe"

    await publisher.stop()


@pytest.mark.asyncio
async def test_status_channel_forced_after_klippy_ready() -> None:
    """Status channel should be force-published after klippy ready.

    This ensures the UI updates from 'Offline' to operational state immediately
    without waiting for cadence controller throttle to expire.
    """
    initial_state = {
        "result": {
            "status": {
                "webhooks": {"state": "shutdown"},
                "printer": {"state": "shutdown", "is_shutdown": True},
                "print_stats": {"state": "error", "message": "Emergency stop"},
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    # Use slow rate to ensure force is needed
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    # Clear initial messages
    mqtt.messages.clear()

    # Update state to ready (Moonraker returns standby for print_stats after recovery)
    ready_state = {
        "result": {
            "status": {
                "webhooks": {"state": "ready"},
                "printer": {"state": "ready", "is_shutdown": False},
                "print_stats": {"state": "standby"},
            }
        }
    }
    moonraker.update_state(ready_state)

    # Emit klippy ready notification
    await moonraker.emit({"method": "notify_klippy_ready", "params": None})
    await asyncio.sleep(0.15)  # Wait for resubscribe and prime_initial_state

    # Find status channel messages
    status_messages = [
        msg for msg in mqtt.messages if "/status" in msg["topic"]
    ]

    # Should have at least one status message after klippy ready
    assert len(status_messages) >= 1, (
        f"Expected at least 1 status message after klippy ready, "
        f"got {len(status_messages)}. All messages: {[m['topic'] for m in mqtt.messages]}"
    )

    # The status message should reflect operational state (Idle, not Error)
    last_status_msg = status_messages[-1]
    last_status = json.loads(last_status_msg["payload"])
    status_body = last_status.get("status", {})
    printer_status = status_body.get("printerStatus", "")
    
    # After klippy ready with print_stats.state=standby, printerStatus should be "Idle"
    # (not "Error" which was the previous state)
    assert printer_status.lower() in {"idle", "ready"}, (
        f"Expected printerStatus='Idle' or 'Ready' after klippy recovery, got '{printer_status}'"
    )
    
    # Verify isShutdown flag is cleared
    flags = status_body.get("flags", {})
    assert flags.get("isShutdown") is False, (
        f"Expected isShutdown=False after recovery, got {flags.get('isShutdown')}"
    )

    await publisher.stop()


@pytest.mark.asyncio
async def test_watch_cadence_enforces_max_rate() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.1)

    publisher.apply_sensors_rate(
        mode="watch",
        max_hz=1.0,
        duration_seconds=None,
        requested_at=datetime.now(timezone.utc),
    )

    mqtt.messages.clear()

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "extruder": {"temperature": 205.0, "target": 210.0},
                }
            ],
        }
    )
    await asyncio.sleep(0.05)

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "extruder": {"temperature": 206.0, "target": 210.0},
                }
            ],
        }
    )
    await asyncio.sleep(0.2)

    sensors_topic = "owl/printers/device-123/sensors"
    early_messages = mqtt.by_topic().get(sensors_topic, [])
    assert (
        len(early_messages) <= 1
    ), "Sensors should respect the 1 Hz cadence and avoid bursts"

    await asyncio.sleep(1.0)

    later_messages = mqtt.by_topic().get(sensors_topic, [])
    assert (
        len(later_messages) >= 2
    ), "Sensors should continue publishing after the cadence interval"

    await publisher.stop()


def test_rate_request_reapplied_after_reset() -> None:
    request_at = datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc)
    moonraker = FakeMoonrakerClient({"result": {"status": {}}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    expires_at = publisher.apply_sensors_rate(
        mode="watch",
        max_hz=2.0,
        duration_seconds=120,
        requested_at=request_at,
    )

    assert expires_at == request_at + timedelta(seconds=120)

    # Simulate that some of the watch window elapsed during downtime.
    fake_now = request_at + timedelta(seconds=30)

    # Reset cadence back to idle to mirror _reset_runtime_state side effects.
    publisher._current_mode = "idle"
    publisher._sensors_interval = publisher._idle_interval
    publisher._watch_window_expires = None
    publisher._refresh_channel_schedules()

    publisher._reapply_rate_request(now=fake_now)

    expected_interval = max(0.1, 1.0 / 2.0)
    remaining_window = fake_now + timedelta(seconds=90)

    assert publisher._current_mode == "watch"
    schedule = publisher._cadence_controller.get_schedule("sensors")  # type: ignore[attr-defined]
    assert schedule.interval == pytest.approx(
        expected_interval,
        rel=1e-6,
    )
    assert publisher._watch_window_expires == remaining_window

    active_request = publisher._active_rate_request
    assert active_request is not None
    assert active_request.mode == "watch"
    assert active_request.max_hz == pytest.approx(2.0)
    assert active_request.duration_seconds == 90
    assert active_request.requested_at == fake_now
    assert active_request.expires_at == remaining_window


def test_forced_publish_respects_one_hz_cap() -> None:
    moonraker = FakeMoonrakerClient({"result": {"status": {}}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    publisher._current_mode = "watch"
    force_interval = publisher._watch_interval or 1.0
    if force_interval <= 0:
        force_interval = 1.0
    publisher._sensors_interval = force_interval
    publisher._sensors_force_publish_seconds = max(300.0, force_interval * 5)
    publisher._refresh_channel_schedules()

    controller = publisher._cadence_controller  # type: ignore[attrdefined]
    state = controller._state["sensors"]  # type: ignore[attrdefined]
    state.last_publish = 100.0
    payload = {"sensors": []}

    controller._monotonic = lambda: 100.2  # type: ignore[attr-defined]
    decision = controller.evaluate(
        "sensors",
        payload,
        explicit_force=True,
        respect_cadence=True,
        allow_force_publish=True,
    )

    assert decision.should_publish is False
    assert decision.delay_seconds is not None
    assert decision.delay_seconds == pytest.approx(force_interval - 0.2)

    controller._monotonic = lambda: 100.2 + force_interval  # type: ignore[attr-defined]
    decision = controller.evaluate(
        "sensors",
        payload,
        explicit_force=True,
        respect_cadence=True,
        allow_force_publish=True,
    )

    assert decision.should_publish is True
    assert decision.delay_seconds is None

    controller._monotonic = time.monotonic  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_notify_status_update_does_not_trigger_query():
    """Verify that heater updates rely solely on WebSocket notifications."""
    # Initial state with both temperature and target
    initial_state = {
        "result": {
            "status": {
                "extruder": {"temperature": 40.0, "target": 40.0},
                "heater_bed": {"temperature": 25.0, "target": 0.0},
            }
        }
    }

    # Create a fake client that can track queries
    class QueryTrackingFakeClient(FakeMoonrakerClient):
        def __init__(self, initial_state):
            super().__init__(initial_state)
            self.query_count = 0
            # State that will be returned by subsequent queries
            self.current_state = initial_state

        async def fetch_printer_state(self, objects=None, timeout=5.0):
            self.query_count += 1
            self.last_query_objects = objects
            self.query_log.append(objects)
            return self.current_state

    moonraker = QueryTrackingFakeClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config()

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())
    await publisher.start()
    await asyncio.sleep(0.1)

    # Clear initial messages
    mqtt.messages.clear()
    await asyncio.sleep(0.15)
    initial_query_count = moonraker.query_count
    initial_query_objects = moonraker.last_query_objects

    # Simulate user setting target to 50°C in Mainsail
    # Moonraker sends notify_status_update WITHOUT target field (the bug we're fixing)
    moonraker.current_state = {
        "result": {
            "status": {
                "extruder": {"temperature": 40.1, "target": 50.0},  # Target changed!
                "heater_bed": {"temperature": 25.0, "target": 0.0},
            }
        }
    }

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "extruder": {
                        "temperature": 40.1,
                        # NOTE: target field is MISSING (Moonraker's field omission)
                    }
                }
            ],
        }
    )

    await asyncio.sleep(0.5)

    # Verify that no HTTP query was executed
    assert (
        moonraker.query_count == initial_query_count
    ), "Expected no HTTP query when processing notify_status_update"
    assert moonraker.last_query_objects == initial_query_objects

    # No new sensors publish is expected when the contract does not change
    assert (
        mqtt.by_topic().get("owl/printers/device-123/sensors") is None
    ), "Expected no sensors publish for unchanged contract"

    await publisher.stop()


@pytest.mark.asyncio
async def test_status_listener_handles_future_return() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()

    loop = asyncio.get_running_loop()
    observed: list[Dict[str, Any]] = []

    def _listener(payload: Dict[str, Any]) -> asyncio.Future[Dict[str, Any]]:
        fut: asyncio.Future[Dict[str, Any]] = loop.create_future()

        def _complete() -> None:
            if not fut.done():
                fut.set_result(payload)

        fut.add_done_callback(lambda result: observed.append(result.result()))
        loop.call_soon(_complete)
        return fut

    publisher.register_status_listener(_listener)

    await publisher.publish_system_status(
        printer_state="error",
        message="Moonraker unavailable",
    )

    await asyncio.sleep(0.05)

    assert observed, "Expected future-based listener to complete"

    def _is_error(entry: Dict[str, Any]) -> bool:
        status = entry.get("result", {}).get("status", {})
        return status.get("webhooks", {}).get("state") == "error"

    assert any(_is_error(entry) for entry in observed), "Expected error snapshot"

    await publisher.stop()


@pytest.mark.asyncio
async def test_status_recovery_retained_after_error_snapshot() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    status_topic = "owl/printers/device-123/status"

    await publisher.publish_system_status(
        printer_state="error",
        message="Moonraker unavailable",
    )

    status_messages = mqtt.by_topic().get(status_topic)
    assert status_messages, "Expected status publish for error snapshot"
    assert status_messages[-1]["retain"] is False
    mqtt.messages.clear()

    await moonraker.emit(sample)
    await asyncio.sleep(0.05)

    recovery_messages = mqtt.by_topic().get(status_topic)
    assert recovery_messages, "Expected status after recovery"
    assert recovery_messages[-1]["retain"] is False
    snapshot = json.loads(recovery_messages[-1]["payload"].decode("utf-8"))
    assert snapshot.get("status", {}).get("printerStatus") != "Error"

    await publisher.stop()


@pytest.mark.asyncio
async def test_status_listener_receives_aggregated_status_updates() -> None:
    initial_state = {
        "result": {
            "status": {
                "printer": {"state": "ready"},
                "webhooks": {"state": "ready"},
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()

    captured: list[Dict[str, Any]] = []

    def _listener(snapshot: Dict[str, Any]) -> None:
        captured.append(snapshot)

    publisher.register_status_listener(_listener)

    await moonraker.emit(  # simulate Moonraker notify_status_update payload
        {
            "method": "notify_status_update",
            "params": [
                {
                    "webhooks": {"state": "shutdown"},
                    "print_stats": {"state": "error", "message": "Emergency"},
                }
            ],
        }
    )

    await asyncio.sleep(0.05)

    assert captured, "Expected status listener to receive aggregated snapshot"
    latest = captured[-1]
    status = latest.get("result", {}).get("status", {})
    assert status.get("webhooks", {}).get("state") == "shutdown"
    assert status.get("print_stats", {}).get("message") == "Emergency"

    await publisher.stop()


@pytest.mark.asyncio
async def test_raw_payload_excluded_by_default():
    """Verify that raw Moonraker payload is excluded by default to save bandwidth."""
    initial_state = {
        "result": {
            "status": {
                "extruder": {"temperature": 25.0, "target": 0.0},
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config()  # Default: include_raw_payload=False

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())
    await publisher.start()
    await asyncio.sleep(0.2)

    # Verify messages were published
    sensors_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert sensors_messages, "Expected sensors messages"

    # Check that raw field is NOT present
    document = _decode(sensors_messages[-1])
    assert (
        "raw" not in document
    ), "Raw field should be excluded by default to save bandwidth (~450 bytes)"

    # Verify normalized data is still present
    assert "deviceId" in document, "Expected device metadata"
    assert document.get("_origin") == EXPECTED_ORIGIN
    sensors_body = document.get("sensors")
    assert isinstance(sensors_body, dict)
    sensors = sensors_body.get("sensors")
    assert isinstance(sensors, list)
    assert any(sensor.get("channel") == "extruder" for sensor in sensors)

    await publisher.stop()


@pytest.mark.asyncio
async def test_raw_payload_included_when_configured():
    """Verify that raw Moonraker payload is included when explicitly configured."""
    initial_state = {
        "result": {
            "status": {
                "extruder": {"temperature": 25.0, "target": 0.0},
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(include_raw_payload=True)  # Explicitly enable

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())
    await publisher.start()
    await asyncio.sleep(0.2)

    # Verify messages were published
    sensors_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert sensors_messages, "Expected sensors messages"

    # Check that raw field IS present
    document = _decode(sensors_messages[-1])
    assert "raw" in document, "Raw field should be included when configured"
    assert isinstance(document["raw"], str), "Raw field should be a JSON string"

    # Verify normalized data is also present
    assert document.get("_origin") == EXPECTED_ORIGIN
    sensors_body = document.get("sensors")
    assert isinstance(sensors_body, dict)
    sensors = sensors_body.get("sensors")
    assert isinstance(sensors, list) and sensors
    assert any(sensor.get("channel") == "extruder" for sensor in sensors)

    await publisher.stop()


@pytest.mark.asyncio
async def test_query_print_stats_on_job_start_adr0003():
    """Verify that print_stats is queried when notify_history_changed with action:added arrives.

    This tests the ADR-0003 query-on-notification pattern for print_stats.
    Moonraker's WebSocket optimization may omit print_stats.state from notify_status_update,
    so we must query via HTTP when a job starts to capture the full state.
    """
    initial_state = {
        "result": {
            "status": {
                "extruder": {"temperature": 200.0, "target": 200.0},
                "print_stats": {"state": "printing", "filename": "test.gcode"},
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=10.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())
    await publisher.start()
    await asyncio.sleep(0.05)

    # Clear initial query log
    moonraker.query_log.clear()

    # Simulate notify_history_changed with action:added (job start)
    job_start_notification = {
        "method": "notify_history_changed",
        "params": [
            {
                "action": "added",
                "job": {
                    "job_id": "000123",
                    "filename": "test.gcode",
                    "status": "in_progress",
                },
            }
        ],
    }

    await moonraker.emit(job_start_notification)
    await asyncio.sleep(0.1)

    # Verify that print_stats was queried via HTTP (ADR-0003 pattern)
    print_stats_queries = [
        q for q in moonraker.query_log if q and "print_stats" in q
    ]
    assert print_stats_queries, (
        "Expected HTTP query for print_stats on job start (ADR-0003 pattern). "
        f"Query log: {moonraker.query_log}"
    )

    await publisher.stop()


@pytest.mark.asyncio
async def test_query_print_stats_on_job_finished():
    """Verify that print_stats is queried for finished jobs (Mainsail-aligned).
    
    Per ADR-0003 and Mainsail alignment: query print_stats on BOTH action:added
    and action:finished to ensure terminal states are captured, since Moonraker
    may not push the final print_stats.state via notify_status_update.
    """
    initial_state = {
        "result": {
            "status": {
                "extruder": {"temperature": 200.0, "target": 0.0},
                "print_stats": {"state": "printing", "filename": "test.gcode"},
            }
        }
    }

    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=10.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())
    await publisher.start()
    await asyncio.sleep(0.05)

    # Clear initial query log
    moonraker.query_log.clear()

    # Simulate notify_history_changed with action:finished (job complete)
    job_finished_notification = {
        "method": "notify_history_changed",
        "params": [
            {
                "action": "finished",
                "job": {
                    "job_id": "000123",
                    "filename": "test.gcode",
                    "status": "completed",
                },
            }
        ],
    }

    await moonraker.emit(job_finished_notification)
    await asyncio.sleep(0.1)

    # Verify that print_stats WAS queried (Mainsail-aligned: query on all history events)
    print_stats_queries = [
        q for q in moonraker.query_log if q and "print_stats" in q
    ]
    assert print_stats_queries, (
        "Expected HTTP query for print_stats on job finish (Mainsail-aligned). "
        f"Query log: {moonraker.query_log}"
    )

    await publisher.stop()


def test_status_payload_includes_moonraker_job_id() -> None:
    """Test that job payload includes moonrakerJobId field for backend matching.

    The moonrakerJobId (raw value like "0003BB") allows NexusService to match
    thumbnail updates using the same ID stored in the PrintJob database record,
    without requiring Agent-side mapping of cloud PrintJobIds.
    """
    store = MoonrakerStateStore()
    tracker = PrintSessionTracker()
    heater = HeaterMonitor()
    selector = StatusSelector()

    observed_at = datetime.now(timezone.utc)

    # Simulate a printing state with job_id from print_stats
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": "printing",
                        "filename": "benchy.gcode",
                        "info": {"total_layer": 100, "current_layer": 50},
                    },
                    "virtual_sdcard": {"is_active": True, "progress": 0.5},
                    "idle_timeout": {"state": "Printing"},
                }
            ],
        }
    )

    # Simulate history event with job_id
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_history_changed",
            "params": [
                {
                    "action": "added",
                    "job": {
                        "job_id": "0003BB",
                        "filename": "benchy.gcode",
                        "status": "in_progress",
                    },
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    status = selector.build(store, session, heater, observed_at)

    assert status is not None
    job = status.get("job")
    assert job is not None, "Expected job payload when printing"

    # Verify moonrakerJobId is included with raw value (no prefix)
    assert job.get("moonrakerJobId") == "0003BB", (
        f"Expected moonrakerJobId to be '0003BB', but got '{job.get('moonrakerJobId')}'"
    )

    # sessionId should have the prefix
    session_id = job.get("sessionId")
    assert session_id is not None
    assert "0003BB" in session_id, "sessionId should contain the job ID"


def test_status_payload_job_id_equals_session_id() -> None:
    """Test that jobId equals sessionId (both use Agent's session identifier).

    The jobId field uses session_id for backward compatibility. Backend matching
    should use moonrakerJobId instead.
    """
    store = MoonrakerStateStore()
    tracker = PrintSessionTracker()
    heater = HeaterMonitor()
    selector = StatusSelector()

    observed_at = datetime.now(timezone.utc)

    # Simulate a printing state
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": "printing",
                        "filename": "test.gcode",
                    },
                    "virtual_sdcard": {"is_active": True, "progress": 0.5},
                    "idle_timeout": {"state": "Printing"},
                }
            ],
        }
    )
    heater.refresh(store)

    session = tracker.compute(store)
    status = selector.build(store, session, heater, observed_at)

    assert status is not None
    job = status.get("job")
    assert job is not None, "Expected job payload when printing"

    # jobId should equal sessionId
    assert job.get("jobId") == job.get("sessionId"), (
        "jobId should equal sessionId for backward compatibility"
    )
