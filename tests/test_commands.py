"""Tests for the command processor."""

import json
from configparser import ConfigParser
from pathlib import Path
from typing import Any, Dict

import pytest

from moonraker_owl.commands import (
    CommandConfigurationError,
    CommandProcessor,
)
from moonraker_owl.config import (
    CloudConfig,
    CommandConfig,
    LoggingConfig,
    MoonrakerConfig,
    OwlConfig,
    TelemetryConfig,
)


class FakeMoonraker:
    def __init__(self) -> None:
        self.actions: list[str] = []

    async def execute_print_action(self, action: str) -> None:
        if action not in {"pause", "resume", "cancel"}:
            raise ValueError(f"Unsupported Moonraker action: {action}")
        self.actions.append(action)


class FakeMQTT:
    def __init__(self) -> None:
        self.handler = None
        self.subscriptions: list[tuple[str, int]] = []
        self.unsubscriptions: list[str] = []
        self.published: list[tuple[str, bytes, int, bool]] = []

    def set_message_handler(self, handler):
        self.handler = handler

    def subscribe(self, topic: str, qos: int = 0):
        self.subscriptions.append((topic, qos))

    def unsubscribe(self, topic: str):
        self.unsubscriptions.append(topic)

    def publish(self, topic: str, payload: bytes, qos: int = 0, retain: bool = False):
        self.published.append((topic, payload, qos, retain))

    async def emit(self, topic: str, payload: Dict[str, Any]) -> None:
        if self.handler is None:
            raise RuntimeError("No handler registered")
        data = json.dumps(payload).encode("utf-8")
        result = self.handler(topic, data)
        if hasattr(result, "__await__"):
            await result


@pytest.fixture
def config() -> OwlConfig:
    parser = ConfigParser()
    parser.add_section("cloud")
    parser.set("cloud", "device_id", "device-123")
    parser.set("cloud", "tenant_id", "tenant-99")
    parser.set("cloud", "printer_id", "printer-17")

    return OwlConfig(
        cloud=CloudConfig(
            base_url="https://api.owl.dev",
            broker_host="broker.owl.dev",
            broker_port=1883,
            username="tenant-99:device-123",
            password="token",
        ),
        moonraker=MoonrakerConfig(),
        telemetry=TelemetryConfig(),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        raw=parser,
        path=Path("moonraker-owl.cfg"),
    )


@pytest.mark.asyncio
async def test_command_processor_executes_action_and_sends_ack(config):
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    message = {
        "commandId": "cmd-1",
        "action": "pause",
        "payload": {},
    }

    await mqtt.emit("owl/printers/device-123/commands/invoke", message)

    assert moonraker.actions == ["pause"]

    statuses = [
        json.loads(payload.decode("utf-8"))["status"]
        for _, payload, _, _ in mqtt.published
    ]
    assert statuses == ["received", "succeeded"]

    await processor.stop()
    assert mqtt.unsubscriptions == ["owl/printers/device-123/commands/invoke"]


@pytest.mark.asyncio
async def test_command_processor_handles_unknown_action(config):
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    message = {
        "commandId": "cmd-2",
        "action": "scrub",
    }

    await mqtt.emit("owl/printers/device-123/commands/invoke", message)

    statuses = [
        json.loads(payload.decode("utf-8"))["status"]
        for _, payload, _, _ in mqtt.published
    ]
    assert statuses[-1] == "failed"
    assert not moonraker.actions

    await processor.stop()


@pytest.mark.asyncio
async def test_command_processor_rejects_invalid_payload(config):
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    if mqtt.handler is None:
        pytest.fail("handler not registered")

    result = mqtt.handler("owl/printers/device-123/commands/invoke", b"not json")
    if hasattr(result, "__await__"):
        await result

    statuses = [
        json.loads(payload.decode("utf-8"))["status"]
        for _, payload, _, _ in mqtt.published
    ]
    assert statuses[-1] == "failed"

    await processor.stop()


def test_command_processor_requires_device_id():
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

    with pytest.raises(CommandConfigurationError):
        CommandProcessor(config, FakeMoonraker(), FakeMQTT())
