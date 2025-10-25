"""Tests for the telemetry publisher."""

import asyncio
import json
from configparser import ConfigParser
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


def _get_contract_sensors(document: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    telemetry = document.get("sensors")
    assert isinstance(telemetry, dict), "Expected telemetry contract object"
    # Allow either top-level 'sensors' contract or nested {"sensors": {...}} payload
    sensors = (
        telemetry.get("sensors") if telemetry.get("sensors") is not None else telemetry
    )
    assert isinstance(sensors, dict), "Expected telemetry.sensors object"
    return sensors


def _get_sensor(document: Dict[str, Any], name: str) -> Dict[str, Any]:
    sensors = _get_contract_sensors(document)
    sensor = sensors.get(name)
    assert isinstance(sensor, dict), f"Expected sensor '{name}'"
    return sensor


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
    assert overview.get("rawStatus") == raw_state.capitalize()


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

    for channel in ("overview", "sensors"):
        topic = f"owl/printers/device-123/{channel}"
        assert topic in messages, f"Missing channel {channel}"
        first_message = messages[topic][0]
        assert (
            first_message["qos"]
            == {
                "overview": 1,
                "sensors": 0,
            }[channel]
        )
        document = _decode(first_message)
        assert document["kind"] == "full"
        assert document["schemaVersion"] == 1
        assert document["deviceId"] == "device-123"
        assert document.get("tenantId") == "tenant-42"
        assert document.get("printerId") == "printer-99"
        assert document.get("_origin") == EXPECTED_ORIGIN
        assert document.get("_ts"), "Expected contract timestamp"
        assert document.get("_seq") is not None
        assert "source" not in document
        assert "sequence" not in document
        assert "timestamp" not in document
        # Raw field is excluded by default (bandwidth optimization)
        assert "raw" not in document, "Raw field should be excluded by default"

    telemetry_doc = _decode(messages["owl/printers/device-123/sensors"][0])
    assert "temperatures" not in telemetry_doc
    contract_section = telemetry_doc.get("sensors")
    assert isinstance(contract_section, dict)
    sensors = (
        contract_section.get("sensors")
        if contract_section.get("sensors") is not None
        else contract_section
    )
    assert isinstance(sensors, dict) and sensors, "Expected sensor contract payload"
    extruder_sensor = sensors.get("extruder")
    assert isinstance(extruder_sensor, dict)
    assert extruder_sensor.get("type") == "heater"
    assert extruder_sensor.get("value") is not None

    overview_doc = _decode(messages["owl/printers/device-123/overview"][0])
    overview_contract = overview_doc.get("overview")
    assert isinstance(overview_contract, dict)
    assert overview_contract.get("printerStatus") == "Printing"
    assert overview_contract.get("progressPercent") is not None
    assert overview_contract.get("elapsedSeconds") is not None
    assert overview_contract.get("estimatedTimeRemainingSeconds") is not None
    assert overview_contract.get("subStatus")

    job_contract = overview_contract.get("job")
    assert isinstance(job_contract, dict)
    assert job_contract.get("name")
    assert job_contract.get("progressPercent") == overview_contract.get(
        "progressPercent"
    )

    thumbnail_contract = job_contract.get("thumbnail")
    assert isinstance(thumbnail_contract, dict)
    assert "cloudUrl" in thumbnail_contract


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
                    "status": {
                        "display_status": {"progress": 0.55},
                    }
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
    assert overview.get("progressPercent") == 55

    job = overview.get("job")
    assert isinstance(job, dict)
    assert job.get("progressPercent") == 55


@pytest.mark.asyncio
async def test_publisher_emits_events_channel() -> None:
    moonraker = FakeMoonrakerClient({"result": {}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=50.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)
    mqtt.messages.clear()

    await moonraker.emit(
        {
            "method": "notify_gcode_response",
            "params": ["File opened"],
        }
    )
    await asyncio.sleep(0.05)
    await publisher.stop()

    events_messages = mqtt.by_topic().get("owl/printers/device-123/events")
    assert events_messages, "Expected events payload"
    last_event_message = events_messages[-1]
    assert last_event_message["qos"] == 2
    document = _decode(last_event_message)
    assert document["kind"] == "delta"
    events = document.get("events")
    assert isinstance(events, list) and events, "Expected contract events"
    entry = events[0]
    assert entry.get("eventName") == "gcodeResponse"
    assert entry.get("severity") == "info"
    payload = entry.get("payload")
    assert isinstance(payload, dict)
    details = payload.get("details")
    assert isinstance(details, dict)
    assert details.get("message") == "File opened"
    assert details.get("code") == "gcode_response"


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

        moonraker._initial_state = {
            "result": {
                "status": {
                    "extruder": {"temperature": 72.5, "target": 100.0},
                    "heater_bed": {"temperature": 39.0, "target": 0.0},
                }
            }
        }

        await moonraker.emit(
            {
                "method": "notify_status_update",
                "params": [
                    {
                        "status": {
                            "extruder": {"temperature": 72.5},
                            "heater_bed": {"temperature": 39.0},
                        }
                    }
                ],
            }
        )

        await asyncio.sleep(0.1)
        await publisher.stop()

        telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
        assert telemetry_messages, "Expected telemetry publish after heater ramp"
        assert len(telemetry_messages) == 1

        document = _decode(telemetry_messages[0])
        assert document.get("_seq", 0) > 1

        extruder_sensor = _get_sensor(document, "extruder")
        assert extruder_sensor.get("target") == 100
        assert extruder_sensor.get("value") == 72


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
                    "status": {
                        "extruder": {
                            "temperature": 195.5,
                            # target intentionally omitted
                        },
                        "heater_bed": {
                            "temperature": 58.2,
                            # target intentionally omitted
                        },
                    }
                }
            ],
        }
    )

    await asyncio.sleep(0.05)
    await publisher.stop()

    # Verify the target was preserved
    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert telemetry_messages, "Expected telemetry updates"

    document = _decode(telemetry_messages[-1])
    extruder_sensor = _get_sensor(document, "extruder")
    assert extruder_sensor.get("value") == 195
    assert extruder_sensor.get("target") == 210, "Target should be preserved"

    bed_sensor = _get_sensor(document, "heaterBed")
    assert bed_sensor.get("value") == 58
    assert bed_sensor.get("target") == 60, "Target should be preserved"


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
                    "status": {
                        "extruder": {
                            "temperature": 195.4,
                        }
                    }
                }
            ],
        }
    )

    await asyncio.sleep(0.1)

    assert mqtt.by_topic().get("owl/printers/device-123/sensors") is None, (
        "Expected fractional change below 1°C to be deduplicated"
    )

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [
                {
                    "status": {
                        "extruder": {
                            "temperature": 196.2,
                        }
                    }
                }
            ],
        }
    )

    await asyncio.sleep(0.1)
    await publisher.stop()

    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert telemetry_messages, "Expected integer-scale change to publish telemetry"

    document = _decode(telemetry_messages[-1])
    extruder_sensor = _get_sensor(document, "extruder")
    assert extruder_sensor.get("value") == 196


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
    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert telemetry_messages, "Expected telemetry updates"

    document = _decode(telemetry_messages[-1])
    extruder_sensor = _get_sensor(document, "extruder")
    assert extruder_sensor.get("value") == 40
    # This is the critical assertion: target should be 50.0 (from query),
    # not missing or stuck at old value
    assert extruder_sensor.get("target") == 50, (
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
    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert telemetry_messages, "Expected telemetry messages"

    # Check that raw field is NOT present
    document = _decode(telemetry_messages[-1])
    assert "raw" not in document, (
        "Raw field should be excluded by default to save bandwidth (~450 bytes)"
    )

    # Verify normalized data is still present
    assert "deviceId" in document, "Expected device metadata"
    assert document.get("_origin") == EXPECTED_ORIGIN
    contract = document.get("sensors")
    assert isinstance(contract, dict) and ("sensors" in contract or contract), (
        "Expected sensors contract"
    )
    sensors = (
        contract.get("sensors") if contract.get("sensors") is not None else contract
    )
    assert isinstance(sensors, dict) and "extruder" in sensors

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
    telemetry_messages = mqtt.by_topic().get("owl/printers/device-123/sensors")
    assert telemetry_messages, "Expected telemetry messages"

    # Check that raw field IS present
    document = _decode(telemetry_messages[-1])
    assert "raw" in document, "Raw field should be included when configured"
    assert isinstance(document["raw"], str), "Raw field should be a JSON string"

    # Verify normalized data is also present
    assert document.get("_origin") == EXPECTED_ORIGIN
    contract = document.get("sensors")
    assert isinstance(contract, dict)
    sensors = (
        contract.get("sensors") if contract.get("sensors") is not None else contract
    )
    assert isinstance(sensors, dict) and "extruder" in sensors
    assert isinstance(sensors, dict) and "extruder" in sensors

    await publisher.stop()
