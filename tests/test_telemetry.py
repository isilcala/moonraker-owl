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
    TelemetryConfig,
)
from moonraker_owl.telemetry import TelemetryConfigurationError, TelemetryPublisher


class FakeMoonrakerClient:
    def __init__(self, initial_state: Dict[str, Any]) -> None:
        self._callback = None
        self._initial_state = initial_state

    async def start(self, callback):
        self._callback = callback

    def remove_callback(self, callback):
        if self._callback is callback:
            self._callback = None

    async def fetch_printer_state(self):
        return self._initial_state

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
            include_fields=include_fields or ["status", "telemetry"],
        ),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )

    return config


@pytest.mark.asyncio
async def test_telemetry_publisher_emits_filtered_channels():
    initial_state = {
        "result": {
            "status": {"state": "idle", "bed_temp": 50},
            "telemetry": {"nozzle_temp": 215},
            "ignored": {"value": 1},
        }
    }
    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(include_fields=["status", "telemetry"], rate_hz=20.0)

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
    assert document["tenantId"] == "tenant-42"
    assert document["printerId"] == "printer-99"
    assert "timestamp" in document
    assert set(document["channels"].keys()) == {"status", "telemetry"}
    assert document["channels"]["status"]["state"] == "idle"


@pytest.mark.asyncio
async def test_telemetry_publisher_tracks_latest_update():
    initial_state = {"result": {"status": {"state": "idle"}}}
    moonraker = FakeMoonrakerClient(initial_state)
    mqtt = FakeMQTTClient()
    config = build_config(include_fields=["status"], rate_hz=50.0)

    publisher = TelemetryPublisher(config, moonraker, mqtt)

    await publisher.start()
    await asyncio.sleep(0.05)

    await moonraker.emit(
        {
            "method": "notify_status_update",
            "params": [{"status": {"state": "printing"}}],
        }
    )
    await asyncio.sleep(0.05)

    await publisher.stop()

    assert len(mqtt.messages) >= 2
    _, payload, _, _ = mqtt.messages[-1]
    document = json.loads(payload.decode("utf-8"))
    assert document["channels"]["status"]["state"] == "printing"
    assert document["moonrakerEvent"] == "notify_status_update"


def test_telemetry_configuration_requires_device_id():
    parser = ConfigParser()
    parser.add_section("cloud")

    config = OwlConfig(
        cloud=CloudConfig(),
        moonraker=MoonrakerConfig(),
        telemetry=TelemetryConfig(),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )

    with pytest.raises(TelemetryConfigurationError):
        TelemetryPublisher(config, FakeMoonrakerClient({}), FakeMQTTClient())
