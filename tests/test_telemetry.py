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

    async def fetch_printer_state(self, objects=None):
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
    *, include_fields: Optional[list[str]] = None, rate_hz: float = 5.0
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

    for channel in ("status", "progress", "telemetry"):
        topic = f"owl/printers/device-123/{channel}"
        assert topic in messages, f"Missing channel {channel}"
        document = _decode(messages[topic][0])
        assert document["kind"] == "full"
        assert document["schemaVersion"] == 1
        assert document["deviceId"] == "device-123"
        assert document.get("tenantId") == "tenant-42"
        assert document.get("printerId") == "printer-99"
        assert document["source"] == "moonraker"
        assert document["raw"], "Expected raw Moonraker payload"

    telemetry_doc = _decode(messages["owl/printers/device-123/telemetry"][0])
    assert telemetry_doc["toolhead"]["position"]["x"] is not None
    assert telemetry_doc["temperatures"], "Expected temperature readings"

    progress_doc = _decode(messages["owl/printers/device-123/progress"][0])
    assert progress_doc["job"]["progress"]["percent"] > 0


@pytest.mark.asyncio
async def test_publisher_emits_progress_delta() -> None:
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

    progress_messages = mqtt.by_topic().get("owl/printers/device-123/progress")
    assert progress_messages, "Expected progress updates"
    document = _decode(progress_messages[-1])
    assert document["kind"] == "delta"
    assert document["job"]["progress"]["percent"] == pytest.approx(55.0, rel=0.01)


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
    document = _decode(events_messages[-1])
    assert document["kind"] == "delta"
    assert document["events"][0]["message"] == "File opened"


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

    expected_objects = {
        field: None
        for field in DEFAULT_TELEMETRY_FIELDS
        if field and field not in DEFAULT_TELEMETRY_EXCLUDE_FIELDS
    }

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
