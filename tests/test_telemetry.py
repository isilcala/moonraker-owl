"""Tests for the telemetry publisher."""

import asyncio
import copy
import json
import time
from contextlib import suppress
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from moonraker_owl.config import (
    CloudConfig,
    CommandConfig,
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
from moonraker_owl.telemetry.selectors import OverviewSelector
from moonraker_owl.telemetry.state_store import MoonrakerStateStore
from moonraker_owl.telemetry.trackers import HeaterMonitor, PrintSessionTracker
from moonraker_owl.telemetry_normalizer import TelemetryNormalizer
import moonraker_owl.telemetry_normalizer as telemetry_normalizer
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

    async def start(self, callback):
        self._callback = callback

    def remove_callback(self, callback):
        if self._callback is callback:
            self._callback = None

    def set_subscription_objects(self, objects):
        self.subscription_objects = objects

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
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )

    return config


def _load_sample(name: str) -> Dict[str, Any]:
    sample_path = Path(__file__).resolve().parents[2] / "docs" / "examples" / name
    return json.loads(sample_path.read_text(encoding="utf-8"))


def _decode(message: dict[str, Any]) -> Dict[str, Any]:
    return json.loads(message["payload"].decode("utf-8"))


@pytest.mark.parametrize(
    ("raw_state", "expected_status"),
    [
        ("printing", "Printing"),
        ("paused", "Paused"),
        ("cancelling", "Cancelling"),
        ("cancelled", "Cancelled"),
    ],
)
def test_normalizer_maps_printer_states(raw_state: str, expected_status: str) -> None:
    normalizer = TelemetryNormalizer()

    payload = normalizer.ingest(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": raw_state,
                    }
                }
            ],
        }
    )

    overview = payload.overview
    assert overview is not None, "Expected overview payload"
    assert overview.get("printerStatus") == expected_status
    assert "rawStatus" not in overview


@pytest.mark.parametrize(
    ("raw_state", "expected_status"),
    [
        ("printing", "Printing"),
        ("paused", "Paused"),
        ("cancelled", "Cancelled"),
    ],
)
def test_normalizer_maps_print_stats_update_states(
    raw_state: str, expected_status: str
) -> None:
    normalizer = TelemetryNormalizer()

    payload = normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": [
                {
                    "state": raw_state,
                }
            ],
        }
    )

    overview = payload.overview
    assert overview is not None, "Expected overview payload"
    assert overview.get("printerStatus") == expected_status


def test_normalizer_handles_tuple_style_print_stats_payload() -> None:
    normalizer = TelemetryNormalizer()

    payload = normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": [["print_stats", {"state": "printing"}]],
        }
    )

    overview = payload.overview
    assert overview is not None
    assert overview.get("printerStatus") == "Printing"
    print_stats_state = normalizer._status_state.get("print_stats", {}).get("state")  # type: ignore[attr-defined]
    assert print_stats_state == "printing"


def test_normalizer_handles_print_stats_dict_params() -> None:
    normalizer = TelemetryNormalizer()

    payload = normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": {"print_stats": {"state": "printing"}},
        }
    )

    overview = payload.overview
    assert overview is not None
    assert overview.get("printerStatus") == "Printing"
    print_stats_state = normalizer._status_state.get("print_stats", {}).get("state")  # type: ignore[attr-defined]
    assert print_stats_state == "printing"


def test_cancelled_overrides_prior_paused_state() -> None:
    normalizer = TelemetryNormalizer()

    normalizer.ingest(
        {
            "result": {
                "status": {
                    "print_stats": {"state": "paused"},
                }
            }
        }
    )

    cancelled = normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": [{"state": "cancelled"}],
        }
    )

    overview = cancelled.overview
    assert overview is not None
    assert overview.get("printerStatus") == "Cancelled"


def test_status_update_reports_cancelled() -> None:
    normalizer = TelemetryNormalizer()

    payload = normalizer.ingest(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "print_stats": {
                        "state": "cancelled",
                        "message": "",
                    }
                }
            ],
        }
    )

    overview = payload.overview
    assert overview is not None
    assert overview.get("printerStatus") == "Cancelled"


def test_cancelled_state_persists_briefly(monkeypatch) -> None:
    normalizer = TelemetryNormalizer()

    base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    timeline = [
        base_time,
        base_time,
        base_time + timedelta(seconds=20),
        base_time + timedelta(seconds=20),
        base_time + timedelta(seconds=20),
    ]

    class FakeDateTime(datetime):  # type: ignore[misc]
        _index = -1

        @classmethod
        def now(cls, tz=None):
            cls._index += 1
            value = timeline[min(cls._index, len(timeline) - 1)]
            if tz is not None:
                return value if value.tzinfo else value.replace(tzinfo=tz)
            return value

    monkeypatch.setattr(telemetry_normalizer, "datetime", FakeDateTime)

    cancelled = normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": [{"state": "cancelled"}],
        }
    )
    assert cancelled.overview is not None
    assert cancelled.overview.get("printerStatus") == "Cancelled"

    standby_recent = normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": [{"state": "standby"}],
        }
    )
    assert standby_recent.overview is not None
    assert standby_recent.overview.get("printerStatus") == "Cancelled"

    standby_late = normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": [{"state": "standby"}],
        }
    )
    assert standby_late.overview is not None
    assert standby_late.overview.get("printerStatus") == "Idle"


def test_terminal_state_clears_on_printing(monkeypatch) -> None:
    normalizer = TelemetryNormalizer()

    base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    timeline = [
        base_time,
        base_time,
        base_time + timedelta(seconds=2),
        base_time + timedelta(seconds=2),
    ]

    class FakeDateTime(datetime):  # type: ignore[misc]
        _index = -1

        @classmethod
        def now(cls, tz=None):
            cls._index += 1
            value = timeline[min(cls._index, len(timeline) - 1)]
            if tz is not None:
                return value if value.tzinfo else value.replace(tzinfo=tz)
            return value

    monkeypatch.setattr(telemetry_normalizer, "datetime", FakeDateTime)

    normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": [{"state": "cancelled"}],
        }
    )

    printing = normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": [{"state": "printing"}],
        }
    )

    assert printing.overview is not None
    assert printing.overview.get("printerStatus") == "Printing"


def test_active_job_overrides_cancelled_latch(monkeypatch) -> None:
    normalizer = TelemetryNormalizer()

    base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    timeline = [
        base_time,
        base_time,
        base_time + timedelta(seconds=1),
        base_time + timedelta(seconds=1),
    ]

    class FakeDateTime(datetime):  # type: ignore[misc]
        _index = -1

        @classmethod
        def now(cls, tz=None):
            cls._index += 1
            value = timeline[min(cls._index, len(timeline) - 1)]
            if tz is not None:
                return value if value.tzinfo else value.replace(tzinfo=tz)
            return value

    monkeypatch.setattr(telemetry_normalizer, "datetime", FakeDateTime)

    normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": [{"state": "cancelled"}],
        }
    )

    job_loaded = normalizer.ingest(
        {
            "method": "notify_print_stats_update",
            "params": [
                {
                    "state": "standby",
                    "filename": "test.gcode",
                    "message": "Ready",
                }
            ],
        }
    )

    assert job_loaded.overview is not None
    assert job_loaded.overview.get("printerStatus") == "Printing"


def test_overview_last_updated_refreshes_without_changes(monkeypatch) -> None:
    normalizer = TelemetryNormalizer()

    payload = {
        "method": "notify_status_update",
        "params": [
            {
                "status": {
                    "print_stats": {
                        "state": "printing",
                    }
                }
            }
        ],
    }

    base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    later_time = base_time + timedelta(seconds=5)
    timeline = [base_time, later_time]

    class FakeDateTime(datetime):  # type: ignore[misc]
        _call_index = -1

        @classmethod
        def now(cls, tz=None):
            cls._call_index += 1
            value = timeline[min(cls._call_index, len(timeline) - 1)]
            if tz is not None:
                return value if value.tzinfo else value.replace(tzinfo=tz)
            return value

    monkeypatch.setattr(telemetry_normalizer, "datetime", FakeDateTime)

    first_overview = normalizer.ingest(payload).overview
    assert first_overview is not None
    second_overview = normalizer.ingest(payload).overview
    assert second_overview is not None

    assert first_overview.get("lastUpdatedUtc") is not None
    assert second_overview.get("lastUpdatedUtc") is not None
    assert second_overview["lastUpdatedUtc"] != first_overview["lastUpdatedUtc"]


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
    # Device authentication now handled via JWT (MQTT password) - no UserProperty needed
    assert getattr(props, "UserProperty", None) is None

    expected_qos = {"overview": 1, "metrics": 0}
    for channel in ("overview", "metrics"):
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

    metrics_doc = _decode(messages["owl/printers/device-123/metrics"][0])
    metrics_body = metrics_doc.get("metrics")
    assert isinstance(metrics_body, dict)
    sensors = metrics_body.get("sensors")
    assert isinstance(sensors, list) and sensors, "Expected metrics sensor payload"
    extruder_sensor = next(
        (sensor for sensor in sensors if sensor.get("channel") == "extruder"),
        None,
    )
    assert extruder_sensor is not None
    assert extruder_sensor.get("type") == "heater"
    assert extruder_sensor.get("value") is not None
    assert extruder_sensor.get("target") is not None

    overview_doc = _decode(messages["owl/printers/device-123/overview"][0])
    overview_contract = overview_doc.get("overview")
    assert isinstance(overview_contract, dict)
    assert overview_contract.get("printerStatus") == "Printing"
    assert overview_contract.get("elapsedSeconds") is not None
    assert overview_contract.get("estimatedTimeRemainingSeconds") is not None
    assert overview_contract.get("subStatus")
    flags = overview_contract.get("flags")
    assert isinstance(flags, dict)
    assert flags.get("hasActiveJob") is True
    assert flags.get("watchWindowActive") is False

    job_contract = overview_contract.get("job")
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
    metrics_topic = "owl/printers/device-123/metrics"
    overview_topic = "owl/printers/device-123/overview"

    assert metrics_topic in messages, "Expected metrics channel"
    assert overview_topic in messages, "Expected overview channel"
    assert "owl/printers/device-123/sensors" not in messages

    metrics_envelope = _decode(messages[metrics_topic][0])
    assert metrics_envelope["_schema"] == 1
    assert metrics_envelope["kind"] == "full"
    assert metrics_envelope["_origin"] == EXPECTED_ORIGIN
    assert metrics_envelope.get("metrics")
    cadence = metrics_envelope["metrics"].get("cadence")
    assert isinstance(cadence, dict)
    assert cadence.get("mode") == "idle"

    sensors = metrics_envelope["metrics"].get("sensors")
    assert isinstance(sensors, list) and sensors

    overview_envelope = _decode(messages[overview_topic][0])
    assert overview_envelope["_schema"] == 1
    assert overview_envelope["kind"] == "full"
    overview_payload = overview_envelope.get("overview")
    assert isinstance(overview_payload, dict)
    assert overview_payload.get("printerStatus")


def test_cancelled_print_drops_redundant_printing_message() -> None:
    store = MoonrakerStateStore()
    tracker = PrintSessionTracker()
    heater = HeaterMonitor()
    selector = OverviewSelector()

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
    overview = selector.build(store, session, heater, observed_at)
    assert overview is not None
    assert overview.get("printerStatus") == "Printing"
    assert overview.get("subStatus") == "Printing"

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
    overview = selector.build(store, session, heater, observed_at + timedelta(seconds=1))
    assert overview is not None
    assert overview.get("printerStatus") == "Cancelled"
    assert overview.get("subStatus") is None
    assert session.message is None
    assert session.has_active_job is False


def test_pause_then_cancel_clears_paused_state() -> None:
    store = MoonrakerStateStore()
    tracker = PrintSessionTracker()
    heater = HeaterMonitor()
    selector = OverviewSelector()

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
    overview = selector.build(store, session, heater, observed_at)
    assert overview is not None
    assert overview.get("printerStatus") == "Printing"
    assert overview.get("subStatus") == "Printing"
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
    overview = selector.build(store, session, heater, observed_at + timedelta(seconds=1))
    assert overview is not None
    assert overview.get("printerStatus") == "Paused"
    assert overview.get("subStatus") == "Paused"
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
    overview = selector.build(store, session, heater, observed_at + timedelta(seconds=2))
    assert overview is not None
    assert overview.get("printerStatus") == "Cancelled"
    assert overview.get("subStatus") is None
    assert session.message is None
    assert session.has_active_job is False


@pytest.mark.asyncio
async def test_publisher_emits_overview_full_update() -> None:
    sample = _load_sample("moonraker-sample-printing.json")
    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=50.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "virtual_sdcard": {"progress": 0.55},
                }
            ],
        }
    )
    await asyncio.sleep(0.05)
    await publisher.stop()



    overview_messages = mqtt.by_topic().get("owl/printers/device-123/overview")
    assert overview_messages, "Expected overview updates"
    document = _decode(overview_messages[-1])
    assert document["kind"] == "full"
    overview = document.get("overview")
    assert isinstance(overview, dict)

    job = overview.get("job")
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
    command_type="metrics:set-rate",
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
    assert entry.get("eventName") == "commandStateChanged"
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
    assert snapshot.get("webhooks", {}).get("state") == "ready"
    assert snapshot.get("printer", {}).get("state") == "ready"
    assert snapshot.get("printer", {}).get("is_shutdown") is False
    assert snapshot.get("print_stats", {}).get("state") == "standby"
    assert snapshot.get("print_stats", {}).get("message") == ""


def test_state_store_retains_shutdown_until_ready_signal() -> None:
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
    assert snapshot.get("webhooks", {}).get("state") == "shutdown"
    assert snapshot.get("printer", {}).get("is_shutdown") is True
    assert snapshot.get("print_stats", {}).get("state") == "error"
    assert snapshot.get("print_stats", {}).get("message") == "Firmware restart"

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
        snapshot.get("webhooks", {}).get("state_message")
        == "Firmware restart complete"
    )
    assert snapshot.get("printer", {}).get("state") == "ready"
    assert snapshot.get("printer", {}).get("is_shutdown") is False
    assert snapshot.get("print_stats", {}).get("state") == "standby"


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
    assert state.get("print_stats", {}).get("state") == "error"
    assert state.get("print_stats", {}).get("message") == "Emergency stop"


def test_state_store_prefers_gcode_shutdown_hint() -> None:
    store = MoonrakerStateStore()

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_gcode_response",
            "params": ["!! Emergency stop !!"],
        }
    )

    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_klippy_shutdown",
            "params": [{}],
        }
    )

    snapshot = store.as_dict()
    assert snapshot.get("print_stats", {}).get("state") == "error"
    assert snapshot.get("print_stats", {}).get("message") == "Emergency stop"


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
    assert (
        snapshot.get("webhooks", {}).get("state_message") == "Restart complete"
    )
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
        "print_stats": None,
        "temperature_sensor ambient": None,
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
    assert topics, f"Expected metrics replay topics, found: {topics}"

    replay_messages = topics.get("owl/printers/device-123/metrics")
    polled_objects = next(
        (
            entry
            for entry in reversed(moonraker.query_log)
            if isinstance(entry, dict)
            and "temperature_sensor ambient" in entry
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

    metrics_messages = mqtt.by_topic().get("owl/printers/device-123/metrics")
    assert metrics_messages, "Expected metrics publish after heater ramp"
    assert len(metrics_messages) == 1

    document = _decode(metrics_messages[0])
    assert document.get("_seq", 0) > 1

    metrics_body = document.get("metrics")
    assert isinstance(metrics_body, dict)
    sensors = metrics_body.get("sensors")
    assert isinstance(sensors, list)
    extruder_sensor = next(
        sensor for sensor in sensors if sensor.get("channel") == "extruder"
    )
    assert extruder_sensor.get("value") == pytest.approx(72.0, rel=1e-3)
    assert extruder_sensor.get("target") == pytest.approx(0.0, abs=1e-6)


@pytest.mark.asyncio
async def test_publish_system_status_clears_retained_overview() -> None:
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

    await moonraker.emit(initial_state)
    await asyncio.sleep(0.05)
    mqtt.messages.clear()

    await publisher.publish_system_status(
        printer_state="error", message="Emergency stop"
    )

    await asyncio.sleep(0.05)
    await publisher.stop()

    overview_messages = [
        message
        for message in mqtt.messages
        if message["topic"].endswith("/overview")
    ]

    assert overview_messages, "Expected overview publications"
    clear_message = overview_messages[0]
    assert clear_message["retain"] is True
    assert clear_message["payload"] == b""

    error_message = overview_messages[-1]
    assert error_message["retain"] is False
    document = _decode(error_message)
    overview = document.get("overview")
    assert isinstance(overview, dict)
    assert overview.get("printerStatus") == "Error"


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
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )

    with pytest.raises(TelemetryConfigurationError):
        TelemetryPublisher(
            config, FakeMoonrakerClient({}), FakeMQTTClient(), poll_specs=()
        )


def test_telemetry_configuration_requires_device_id():
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

    # Verify heater objects (not sensors) are subscribed with explicit fields
    # Temperature sensors subscribe to all fields (None) since they don't have target
    assert moonraker.subscription_objects == {
        "extruder": ["temperature", "target"],
        "heater_bed": ["temperature", "target"],
        "temperature_sensor ambient": None,  # Sensors don't have target
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
                        "temperature": 195.5,
                        # target intentionally omitted
                    },
                    "heater_bed": {
                        "temperature": 58.2,
                        # target intentionally omitted
                    },
                }
            ],
        }
    )

    await asyncio.sleep(0.05)
    await publisher.stop()

    # Verify the target was preserved
    metrics_messages = mqtt.by_topic().get("owl/printers/device-123/metrics")
    assert metrics_messages, "Expected metrics updates"

    document = _decode(metrics_messages[-1])
    metrics_body = document.get("metrics")
    assert isinstance(metrics_body, dict)
    sensors = metrics_body.get("sensors")
    assert isinstance(sensors, list)

    def _find(channel: str) -> Dict[str, Any]:
        for sensor in sensors:
            if sensor.get("channel") == channel:
                return sensor
        raise AssertionError(f"Missing sensor {channel}")

    extruder_sensor = _find("extruder")
    assert extruder_sensor.get("value") == pytest.approx(196.0, rel=1e-3)
    assert extruder_sensor.get("target") == pytest.approx(210.0, rel=1e-3)

    bed_sensor = _find("heaterBed")
    assert bed_sensor.get("value") == pytest.approx(58.0, rel=1e-3)
    assert bed_sensor.get("target") == pytest.approx(60.0, rel=1e-3)


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

    first_messages = mqtt.by_topic().get("owl/printers/device-123/metrics")
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

    metrics_messages = mqtt.by_topic().get("owl/printers/device-123/metrics")
    assert metrics_messages, "Expected integer-scale change to publish metrics"

    document = _decode(metrics_messages[-1])
    metrics_body = document.get("metrics")
    assert isinstance(metrics_body, dict)
    sensors = metrics_body.get("sensors")
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

    metrics_messages = mqtt.by_topic().get("owl/printers/device-123/metrics")
    assert metrics_messages, "Expected deferred payload to flush"

    document = _decode(metrics_messages[-1])
    metrics_body = document.get("metrics")
    assert isinstance(metrics_body, dict)
    sensors = metrics_body.get("sensors")
    assert isinstance(sensors, list)

    extruder_sensor = next(
        sensor for sensor in sensors if sensor.get("channel") == "extruder"
    )
    assert extruder_sensor.get("value") == pytest.approx(60.0, rel=1e-3)


@pytest.mark.asyncio
async def test_restart_fetches_fresh_overview_state() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    initial_messages = mqtt.by_topic().get("owl/printers/device-123/overview")
    assert initial_messages, "Expected initial overview publish"

    await publisher.stop()
    mqtt.messages.clear()

    moonraker.update_state(_load_sample("moonraker-sample-complete.json"))

    await publisher.start()
    await asyncio.sleep(0.05)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + 0.3
    latest_status: Optional[str] = None

    while loop.time() < deadline:
        resumed_messages = mqtt.by_topic().get("owl/printers/device-123/overview") or []
        if resumed_messages:
            last_message = _decode(resumed_messages[-1])
            overview_body = last_message.get("overview")
            if isinstance(overview_body, dict):
                latest_status = overview_body.get("printerStatus")
                if latest_status == "Completed":
                    break
        await asyncio.sleep(0.01)

    assert latest_status == "Completed", f"Expected overview status Completed but saw {latest_status!r}"

    resumed_messages = mqtt.by_topic().get("owl/printers/device-123/overview") or []
    assert resumed_messages, "Expected overview publish after restart"
    assert any(message["retain"] for message in resumed_messages)

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

    overview_messages = mqtt.by_topic().get("owl/printers/device-123/overview")
    assert overview_messages, "Expected overview publish after recovering start"

    await publisher.stop()


@pytest.mark.asyncio
async def test_restart_emits_current_metrics_after_start() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    initial_messages = mqtt.by_topic().get("owl/printers/device-123/metrics")
    assert initial_messages, "Expected metrics publish on first start"
    initial_payload = _decode(initial_messages[-1])
    initial_metrics = initial_payload.get("metrics", {})
    initial_extruder = next(
        (sensor for sensor in initial_metrics.get("sensors", []) if sensor.get("channel") == "extruder"),
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
        replay_messages = mqtt.by_topic().get("owl/printers/device-123/metrics") or []
        if replay_messages:
            latest_payload = _decode(replay_messages[-1])
            metrics_body = latest_payload.get("metrics", {})
            sensors = metrics_body.get("sensors", [])
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

    assert extruder_sensor is not None, "Expected extruder sensor metrics after restart"
    assert extruder_sensor.get("target") == pytest.approx(0.0, abs=1e-6)
    assert extruder_sensor.get("value") == pytest.approx(45.04, rel=1e-3)


@pytest.mark.asyncio
async def test_restart_replays_full_overview_when_state_unchanged() -> None:
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
    overview_messages = topics.get("owl/printers/device-123/overview")
    metrics_messages = topics.get("owl/printers/device-123/metrics")

    assert overview_messages, "Expected overview publish after restart"
    assert metrics_messages, "Expected metrics publish after restart"

    overview_document = _decode(overview_messages[-1])
    metrics_document = _decode(metrics_messages[-1])

    assert overview_document.get("kind") == "full"
    assert metrics_document.get("kind") == "full"
    assert isinstance(overview_document.get("overview"), dict)
    assert isinstance(metrics_document.get("metrics"), dict)

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

    overview_messages = mqtt.by_topic().get("owl/printers/device-123/overview")
    assert overview_messages, "Expected overview publish for system status"

    snapshot = json.loads(overview_messages[-1]["payload"].decode("utf-8"))
    overview_body = snapshot.get("overview")
    assert overview_body is not None
    assert overview_body.get("printerStatus") == "Error"
    assert overview_body.get("subStatus") == "Moonraker unavailable"
    assert overview_messages[-1]["retain"] is True

    await publisher.stop()

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

    expires_at = publisher.apply_metrics_rate(
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
    schedule = publisher._cadence_controller.get_schedule("metrics")  # type: ignore[attr-defined]
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
    schedule = publisher._cadence_controller.get_schedule("metrics")  # type: ignore[attr-defined]
    assert schedule.interval == pytest.approx(idle_interval, rel=1e-6)
    assert schedule.forced_interval == pytest.approx(idle_interval, rel=1e-6)

    watch_expires = publisher.apply_metrics_rate(
        mode="watch",
        max_hz=1.0,
        duration_seconds=120,
        requested_at=datetime.now(timezone.utc),
    )
    assert watch_expires is not None

    watch_schedule = publisher._cadence_controller.get_schedule("metrics")  # type: ignore[attr-defined]
    assert watch_schedule.interval == pytest.approx(1.0, rel=1e-6)
    assert watch_schedule.forced_interval == pytest.approx(1.0, rel=1e-6)

    publisher.apply_metrics_rate(
        mode="idle",
        max_hz=1.0 / idle_interval if idle_interval > 0 else 0.0,
        duration_seconds=None,
        requested_at=datetime.now(timezone.utc),
    )

    reverted_schedule = publisher._cadence_controller.get_schedule("metrics")  # type: ignore[attr-defined]
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
    publisher._pending_channels.clear()  # type: ignore[attr-defined]

    publisher._reset_runtime_state()

    assert publisher._pending_payload == snapshot
    pending_channels = publisher._pending_channels  # type: ignore[attr-defined]
    assert {"overview", "metrics"}.issubset(pending_channels.keys())
    for channel in ("overview", "metrics"):
        entry = pending_channels[channel]
        assert entry.forced is True
        assert entry.respect_cadence is False
    assert publisher._last_payload_snapshot == snapshot


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
async def test_watch_cadence_enforces_max_rate() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.1)

    publisher.apply_metrics_rate(
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

    metrics_topic = "owl/printers/device-123/metrics"
    early_messages = mqtt.by_topic().get(metrics_topic, [])
    assert (
        len(early_messages) <= 1
    ), "Metrics should respect the 1 Hz cadence and avoid bursts"

    await asyncio.sleep(1.0)

    later_messages = mqtt.by_topic().get(metrics_topic, [])
    assert len(later_messages) >= 2, "Metrics should continue publishing after the cadence interval"

    await publisher.stop()


def test_rate_request_reapplied_after_reset() -> None:
    request_at = datetime(2025, 10, 10, 16, 42, 3, tzinfo=timezone.utc)
    moonraker = FakeMoonrakerClient({"result": {"status": {}}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    expires_at = publisher.apply_metrics_rate(
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
    publisher._metrics_interval = publisher._idle_interval
    publisher._watch_window_expires = None
    publisher._refresh_channel_schedules()

    publisher._reapply_rate_request(now=fake_now)

    expected_interval = max(0.1, 1.0 / 2.0)
    remaining_window = fake_now + timedelta(seconds=90)

    assert publisher._current_mode == "watch"
    schedule = publisher._cadence_controller.get_schedule("metrics")  # type: ignore[attr-defined]
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
    publisher._metrics_interval = force_interval
    publisher._metrics_watchdog_seconds = max(300.0, force_interval * 5)
    publisher._refresh_channel_schedules()

    controller = publisher._cadence_controller  # type: ignore[attr-defined]
    state = controller._state["metrics"]  # type: ignore[attr-defined]
    state.last_publish = 100.0
    payload = {"sensors": []}

    controller._monotonic = lambda: 100.2  # type: ignore[attr-defined]
    decision = controller.evaluate(
    "metrics",
        payload,
        explicit_force=True,
        respect_cadence=True,
        allow_watchdog=True,
    )

    assert decision.should_publish is False
    assert decision.delay_seconds is not None
    assert decision.delay_seconds == pytest.approx(force_interval - 0.2)

    controller._monotonic = lambda: 100.2 + force_interval  # type: ignore[attr-defined]
    decision = controller.evaluate(
    "metrics",
        payload,
        explicit_force=True,
        respect_cadence=True,
        allow_watchdog=True,
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
    assert moonraker.query_count == initial_query_count, (
        "Expected no HTTP query when processing notify_status_update"
    )
    assert moonraker.last_query_objects == initial_query_objects

    # No new metrics publish is expected when the contract does not change
    assert (
        mqtt.by_topic().get("owl/printers/device-123/metrics") is None
    ), "Expected no metrics publish for unchanged contract"

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
async def test_overview_recovery_retained_after_error_snapshot() -> None:
    sample = _load_sample("moonraker-sample-printing.json")

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=1 / 30)

    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    await publisher.start()
    await asyncio.sleep(0.05)

    overview_topic = "owl/printers/device-123/overview"

    await publisher.publish_system_status(
        printer_state="error",
        message="Moonraker unavailable",
    )

    overview_messages = mqtt.by_topic().get(overview_topic)
    assert overview_messages, "Expected overview retain for error snapshot"
    assert overview_messages[-1]["retain"] is True
    mqtt.messages.clear()

    await moonraker.emit(sample)
    await asyncio.sleep(0.05)

    recovery_messages = mqtt.by_topic().get(overview_topic)
    assert recovery_messages, "Expected overview after recovery"
    assert recovery_messages[-1]["retain"] is True
    snapshot = json.loads(recovery_messages[-1]["payload"].decode("utf-8"))
    assert snapshot.get("overview", {}).get("printerStatus") != "Error"

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
    metrics_messages = mqtt.by_topic().get("owl/printers/device-123/metrics")
    assert metrics_messages, "Expected metrics messages"

    # Check that raw field is NOT present
    document = _decode(metrics_messages[-1])
    assert "raw" not in document, (
        "Raw field should be excluded by default to save bandwidth (~450 bytes)"
    )

    # Verify normalized data is still present
    assert "deviceId" in document, "Expected device metadata"
    assert document.get("_origin") == EXPECTED_ORIGIN
    metrics_body = document.get("metrics")
    assert isinstance(metrics_body, dict)
    sensors = metrics_body.get("sensors")
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
    metrics_messages = mqtt.by_topic().get("owl/printers/device-123/metrics")
    assert metrics_messages, "Expected metrics messages"

    # Check that raw field IS present
    document = _decode(metrics_messages[-1])
    assert "raw" in document, "Raw field should be included when configured"
    assert isinstance(document["raw"], str), "Raw field should be a JSON string"

    # Verify normalized data is also present
    assert document.get("_origin") == EXPECTED_ORIGIN
    metrics_body = document.get("metrics")
    assert isinstance(metrics_body, dict)
    sensors = metrics_body.get("sensors")
    assert isinstance(sensors, list) and sensors
    assert any(sensor.get("channel") == "extruder" for sensor in sensors)

    await publisher.stop()
