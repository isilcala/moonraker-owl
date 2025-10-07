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
        self.messages: list[tuple[str, bytes, int, bool]] = []

    def publish(
        self, topic: str, payload: bytes, qos: int = 1, retain: bool = False
    ) -> None:
        self.messages.append((topic, payload, qos, retain))


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


@pytest.mark.asyncio
async def test_telemetry_publisher_emits_filtered_channels():
    sample_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "examples"
        / "moonraker-sample-printing.json"
    )
    sample = json.loads(sample_path.read_text(encoding="utf-8"))

    moonraker = FakeMoonrakerClient(sample)
    mqtt = FakeMQTTClient()
    config = build_config(include_fields=list(DEFAULT_TELEMETRY_FIELDS), rate_hz=20.0)
    config.telemetry.exclude_fields = list(DEFAULT_TELEMETRY_EXCLUDE_FIELDS)

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.1)
    await publisher.stop()

    assert mqtt.messages, "Expected at least one telemetry publish"
    topic, payload, qos, retain = mqtt.messages[0]
    assert topic == "owl/printers/device-123/telemetry"
    assert qos == 1
    assert retain is False

    document = json.loads(payload.decode("utf-8"))
    assert document["deviceId"] == "device-123"
    assert document.get("tenantId") == "tenant-42"
    assert document["printerId"] == "printer-99"
    assert "timestamp" in document
    expected_keys = {
        "print_stats",
        "toolhead",
        "gcode_move",
        "heater_bed",
        "extruder",
        "fan",
        "display_status",
        "virtual_sdcard",
        "temperature_sensor ambient",
        "temperature_sensor chamber",
    }

    assert set(document["channels"].keys()) == expected_keys
    assert document["channels"]["print_stats"]["state"] == "printing"
    assert document["channels"]["heater_bed"]["target"] == 80.0
    assert document["channels"]["temperature_sensor ambient"]["temperature"] is None
    for field in DEFAULT_TELEMETRY_EXCLUDE_FIELDS:
        assert field not in document["channels"]


@pytest.mark.asyncio
async def test_telemetry_publisher_tracks_latest_update():
    initial_state = {"result": {"status": {"print_stats": {"state": "idle"}}}}
    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(include_fields=["print_stats"], rate_hz=50.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [{"status": {"print_stats": {"state": "printing"}}}],
        }
    )
    await asyncio.sleep(0.05)

    await publisher.stop()

    assert len(mqtt.messages) >= 2
    _, payload, _, _ = mqtt.messages[-1]
    document = json.loads(payload.decode("utf-8"))
    assert document["channels"]["print_stats"]["state"] == "printing"
    assert document["moonrakerEvent"] == "notify_status_update"


@pytest.mark.asyncio
async def test_telemetry_publisher_captures_proc_stats():
    moonraker = FakeMoonrakerClient({"result": {}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=50.0)
    config.telemetry.exclude_fields = []

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)

    stream_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "examples"
        / "moonraker-stream-sample-trimmed.jsonl"
    )
    with stream_path.open("r", encoding="utf-8") as lines:
        for line in lines:
            payload = json.loads(line)
            await moonraker.emit(payload)

    await asyncio.sleep(0.05)
    await publisher.stop()

    assert mqtt.messages, "Expected telemetry publish from proc stats"
    _, payload, _, _ = mqtt.messages[-1]
    document = json.loads(payload.decode("utf-8"))
    channels = document["channels"]
    assert "moonraker_stats" in channels
    assert "system_cpu_usage" in channels
    assert "network" in channels
    assert channels["moonraker_stats"]["cpu_usage"] >= 0


@pytest.mark.asyncio
async def test_telemetry_publisher_exclude_fields() -> None:
    sample_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "examples"
        / "moonraker-sample-printing.json"
    )
    sample = json.loads(sample_path.read_text(encoding="utf-8"))

    moonraker = FakeMoonrakerClient({"result": {}})
    mqtt = FakeMQTTClient()
    config = build_config(rate_hz=20.0)
    config.telemetry.exclude_fields = [
        "print_stats.message",
        "virtual_sdcard.file_position",
        "network.*.bandwidth",
    ]

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)
    await moonraker.emit(sample)
    await asyncio.sleep(0.1)

    stream_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "examples"
        / "moonraker-stream-sample-trimmed.jsonl"
    )
    with stream_path.open("r", encoding="utf-8") as lines:
        for line in lines:
            payload = json.loads(line)
            await moonraker.emit(payload)

    await asyncio.sleep(0.1)
    await publisher.stop()

    assert mqtt.messages
    documents = [
        json.loads(payload.decode("utf-8")) for _, payload, _, _ in mqtt.messages
    ]

    first_with_stats = next(
        doc for doc in documents if "print_stats" in doc["channels"]
    )
    assert "message" not in first_with_stats["channels"]["print_stats"]
    assert "file_position" not in first_with_stats["channels"]["virtual_sdcard"]

    last = documents[-1]
    network = last["channels"].get("network")
    assert network is not None
    for iface in network.values():
        assert "bandwidth" not in iface


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
