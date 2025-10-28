"""Tests for the telemetry publisher."""

import asyncio
import json
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
    DEFAULT_TELEMETRY_FIELDS,
    DEFAULT_TELEMETRY_EXCLUDE_FIELDS,
)
from moonraker_owl import constants
from moonraker_owl.telemetry import TelemetryConfigurationError, TelemetryPublisher
from moonraker_owl.telemetry_normalizer import TelemetryNormalizer
import moonraker_owl.telemetry_normalizer as telemetry_normalizer
from moonraker_owl.version import __version__ as PLUGIN_VERSION

EXPECTED_ORIGIN = f"moonraker-owl@{PLUGIN_VERSION}"


class FakeMoonrakerClient:
    def __init__(self, initial_state: Dict[str, Any]) -> None:
        self._callback = None
        self._initial_state = initial_state
        self.subscription_objects = None

    async def start(self, callback):
        self._callback = callback

    def remove_callback(self, callback):
        if self._callback is callback:
            self._callback = None

    def set_subscription_objects(self, objects):
        self.subscription_objects = objects

    async def fetch_printer_state(self, objects=None, timeout=5.0):
        self.subscription_objects = objects
        return self._initial_state

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

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    messages = mqtt.by_topic()
    assert messages, "Expected telemetry messages"

    props = mqtt.messages[0]["properties"]
    assert props is not None
    assert getattr(props, "UserProperty", None) == [
        (constants.DEVICE_TOKEN_MQTT_PROPERTY_NAME, "token")
    ]

    expected_qos = {"overview": 1, "telemetry": 0}
    for channel in ("overview", "telemetry"):
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

    telemetry_doc = _decode(messages["owl/printers/device-123/telemetry"][0])
    telemetry_body = telemetry_doc.get("telemetry")
    assert isinstance(telemetry_body, dict)
    sensors = telemetry_body.get("sensors")
    assert isinstance(sensors, list) and sensors, "Expected telemetry sensor payload"
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

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    messages = mqtt.by_topic()
    telemetry_topic = "owl/printers/device-123/telemetry"
    overview_topic = "owl/printers/device-123/overview"

    assert telemetry_topic in messages, "Expected telemetry channel"
    assert overview_topic in messages, "Expected overview channel"
    assert "owl/printers/device-123/sensors" not in messages

    telemetry_envelope = _decode(messages[telemetry_topic][0])
    assert telemetry_envelope["_schema"] == 1
    assert telemetry_envelope["kind"] == "full"
    assert telemetry_envelope["_origin"] == EXPECTED_ORIGIN
    assert telemetry_envelope.get("telemetry")
    cadence = telemetry_envelope["telemetry"].get("cadence")
    assert isinstance(cadence, dict)
    assert cadence.get("mode") == "idle"

    sensors = telemetry_envelope["telemetry"].get("sensors")
    assert isinstance(sensors, list) and sensors

    overview_envelope = _decode(messages[overview_topic][0])
    assert overview_envelope["_schema"] == 1
    assert overview_envelope["kind"] == "full"
    overview_payload = overview_envelope.get("overview")
    assert isinstance(overview_payload, dict)
    assert overview_payload.get("printerStatus")


@pytest.mark.asyncio
async def test_publisher_emits_overview_full_update() -> None:
    sample = _load_sample("moonraker-sample-printing.json")
    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=50.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt)

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

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()

    # Manually queue an event to ensure the publisher forwards orchestrator events.
    publisher._orchestrator.events.record_command_state(
        command_id="cmd-123",
        command_type="telemetry:set-rate",
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

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    expected_objects: dict[str, Optional[list[str]]] = {
        field: None
        for field in DEFAULT_TELEMETRY_FIELDS
        if field and field not in DEFAULT_TELEMETRY_EXCLUDE_FIELDS
    }

    # Heaters explicitly subscribe to temperature and target fields
    # (optimization to ensure both are always present)
    expected_objects["extruder"] = ["temperature", "target"]
    expected_objects["heater_bed"] = ["temperature", "target"]

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

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    assert moonraker.subscription_objects == {
        "toolhead": ["position", "velocity"],
        "virtual_sdcard": None,
    }


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

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)
    await publisher.stop()

    assert moonraker.subscription_objects == {
        "print_stats": None,
        "temperature_sensor ambient": None,
        "toolhead": ["position"],
    }


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

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()

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

    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/telemetry")
    assert telemetry_messages, "Expected telemetry publish after heater ramp"
    assert len(telemetry_messages) == 1

    document = _decode(telemetry_messages[0])
    assert document.get("_seq", 0) > 1

    telemetry_body = document.get("telemetry")
    assert isinstance(telemetry_body, dict)
    sensors = telemetry_body.get("sensors")
    assert isinstance(sensors, list)
    extruder_sensor = next(
        sensor for sensor in sensors if sensor.get("channel") == "extruder"
    )
    assert extruder_sensor.get("target") == pytest.approx(100.0, rel=1e-3)
    assert extruder_sensor.get("value") == pytest.approx(72.5, rel=1e-3)


def test_telemetry_configuration_requires_device_id():
    parser = ConfigParser()
    parser.add_section("cloud")

    config = OwlConfig(
        cloud=CloudConfig(),
        moonraker=MoonrakerConfig(),
        telemetry=TelemetryConfig(),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        resilience=ResilienceConfig(),
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )

    with pytest.raises(TelemetryConfigurationError):
        TelemetryPublisher(config, FakeMoonrakerClient({}), FakeMQTTClient())


def test_telemetry_configuration_requires_device_token():
    parser = ConfigParser()
    parser.add_section("cloud")
    parser.set("cloud", "device_id", "device-123")

    config = OwlConfig(
        cloud=CloudConfig(),
        moonraker=MoonrakerConfig(),
        telemetry=TelemetryConfig(),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        resilience=ResilienceConfig(),
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )

    with pytest.raises(TelemetryConfigurationError):
        TelemetryPublisher(config, FakeMoonrakerClient({}), FakeMQTTClient())


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

    publisher = TelemetryPublisher(config, moonraker, mqtt)

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

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)

    # Initial message should have both temperature and target
    mqtt.messages.clear()

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
    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/telemetry")
    assert telemetry_messages, "Expected telemetry updates"

    document = _decode(telemetry_messages[-1])
    telemetry_body = document.get("telemetry")
    assert isinstance(telemetry_body, dict)
    sensors = telemetry_body.get("sensors")
    assert isinstance(sensors, list)

    def _find(channel: str) -> Dict[str, Any]:
        for sensor in sensors:
            if sensor.get("channel") == channel:
                return sensor
        raise AssertionError(f"Missing sensor {channel}")

    extruder_sensor = _find("extruder")
    assert extruder_sensor.get("value") == pytest.approx(195.5, rel=1e-3)
    assert extruder_sensor.get("target") == pytest.approx(210.0, rel=1e-3)

    bed_sensor = _find("heaterBed")
    assert bed_sensor.get("value") == pytest.approx(58.2, rel=1e-3)
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

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()

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

    first_messages = mqtt.by_topic().get("owl/printers/device-123/telemetry")
    assert first_messages, "Expected telemetry publish for fractional change"
    first_document = _decode(first_messages[-1])
    first_body = first_document.get("telemetry")
    assert isinstance(first_body, dict)
    first_sensors = first_body.get("sensors")
    assert isinstance(first_sensors, list)
    first_extruder = next(
        sensor for sensor in first_sensors if sensor.get("channel") == "extruder"
    )
    assert first_extruder.get("value") == pytest.approx(195.4, rel=1e-3)

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

    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/telemetry")
    assert telemetry_messages, "Expected integer-scale change to publish telemetry"

    document = _decode(telemetry_messages[-1])
    telemetry_body = document.get("telemetry")
    assert isinstance(telemetry_body, dict)
    sensors = telemetry_body.get("sensors")
    assert isinstance(sensors, list)
    extruder_sensor = next(
        sensor for sensor in sensors if sensor.get("channel") == "extruder"
    )
    assert extruder_sensor.get("value") == pytest.approx(196.2, rel=1e-3)
    assert document["_seq"] > first_document["_seq"]


@pytest.mark.asyncio
async def test_query_on_notification_ensures_target_values():
    """Verify that query-on-notification strategy retrieves target values.

    This test validates the Obico-inspired fix: when Moonraker omits the target
    field in notify_status_update (which happens when setting target on a cold
    extruder), we query heater state via HTTP to ensure we always have current
    target values.
    """
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
            self.last_query_objects = None
            # State that will be returned by subsequent queries
            self.current_state = initial_state

        async def fetch_printer_state(self, objects=None, timeout=5.0):
            self.query_count += 1
            self.last_query_objects = objects
            self.subscription_objects = objects
            return self.current_state

    moonraker = QueryTrackingFakeClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config()

    publisher = TelemetryPublisher(config, moonraker, mqtt)
    await publisher.start()
    await asyncio.sleep(0.1)

    # Clear initial messages
    mqtt.messages.clear()
    initial_query_count = moonraker.query_count

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

    await asyncio.sleep(0.5)  # Wait for query + processing

    # Verify that an HTTP query was executed
    assert moonraker.query_count > initial_query_count, (
        "Expected HTTP query to be executed on notify_status_update"
    )

    # Verify that only heaters were queried (efficiency check)
    assert moonraker.last_query_objects is not None, "Expected query objects"
    queried = set(moonraker.last_query_objects.keys())
    assert "extruder" in queried, "Expected extruder to be queried"
    # Verify we use None to get all fields
    assert moonraker.last_query_objects["extruder"] is None, (
        "Expected to query all heater fields (None)"
    )

    # Verify MQTT message includes the correct target (from query, not notification)
    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/telemetry")
    assert telemetry_messages, "Expected telemetry updates"

    document = _decode(telemetry_messages[-1])
    telemetry_body = document.get("telemetry")
    assert isinstance(telemetry_body, dict)
    sensors = telemetry_body.get("sensors")
    assert isinstance(sensors, list)
    extruder_sensor = next(
        sensor for sensor in sensors if sensor.get("channel") == "extruder"
    )
    assert extruder_sensor.get("value") == pytest.approx(40.1, rel=1e-3)
    # This is the critical assertion: target should be 50.0 (from query),
    # not missing or stuck at old value
    assert extruder_sensor.get("target") == pytest.approx(50.0, rel=1e-3), (
        "Target should be retrieved via HTTP query when omitted from WebSocket notification"
    )

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

    publisher = TelemetryPublisher(config, moonraker, mqtt)
    await publisher.start()
    await asyncio.sleep(0.2)

    # Verify messages were published
    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/telemetry")
    assert telemetry_messages, "Expected telemetry messages"

    # Check that raw field is NOT present
    document = _decode(telemetry_messages[-1])
    assert "raw" not in document, (
        "Raw field should be excluded by default to save bandwidth (~450 bytes)"
    )

    # Verify normalized data is still present
    assert "deviceId" in document, "Expected device metadata"
    assert document.get("_origin") == EXPECTED_ORIGIN
    telemetry_body = document.get("telemetry")
    assert isinstance(telemetry_body, dict)
    sensors = telemetry_body.get("sensors")
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

    publisher = TelemetryPublisher(config, moonraker, mqtt)
    await publisher.start()
    await asyncio.sleep(0.2)

    # Verify messages were published
    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/telemetry")
    assert telemetry_messages, "Expected telemetry messages"

    # Check that raw field IS present
    document = _decode(telemetry_messages[-1])
    assert "raw" in document, "Raw field should be included when configured"
    assert isinstance(document["raw"], str), "Raw field should be a JSON string"

    # Verify normalized data is also present
    assert document.get("_origin") == EXPECTED_ORIGIN
    telemetry_body = document.get("telemetry")
    assert isinstance(telemetry_body, dict)
    sensors = telemetry_body.get("sensors")
    assert isinstance(sensors, list) and sensors
    assert any(sensor.get("channel") == "extruder" for sensor in sensors)

    await publisher.stop()
