"""Tests for the command processor."""

import json
from configparser import ConfigParser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from moonraker_owl.commands import (
    CommandConfigurationError,
    CommandMessage,
    CommandProcessor,
)
from moonraker_owl.config import (
    CloudConfig,
    CommandConfig,
    LoggingConfig,
    MoonrakerConfig,
    OwlConfig,
    ResilienceConfig,
    TelemetryConfig,
    TelemetryCadenceConfig,
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

    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 0,
        retain: bool = False,
        *,
        properties=None,
    ):
        self.published.append((topic, payload, qos, retain))

    async def emit(self, topic: str, payload: Dict[str, Any]) -> None:
        if self.handler is None:
            raise RuntimeError("No handler registered")
        data = json.dumps(payload).encode("utf-8")
        result = self.handler(topic, data)
        if hasattr(result, "__await__"):
            await result


class FakeTelemetry:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.applied: list[dict[str, Any]] = []
        self.next_expires_at: Optional[datetime] = datetime(2030, 1, 1, tzinfo=timezone.utc)

    def record_command_state(
        self,
        *,
        command_id: str,
        command_type: str,
        state: str,
        session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.records.append(
            {
                "command_id": command_id,
                "command_type": command_type,
                "state": state,
                "details": details,
                "session_id": session_id,
            }
        )

    def apply_sensors_rate(
        self,
        *,
        mode: str,
        max_hz: float,
        duration_seconds: Optional[int],
        requested_at: Optional[datetime],
    ) -> Optional[datetime]:
        self.applied.append(
            {
                "mode": mode,
                "max_hz": max_hz,
                "duration_seconds": duration_seconds,
                "requested_at": requested_at,
            }
        )
        return self.next_expires_at


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
    telemetry_cadence=TelemetryCadenceConfig(),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        resilience=ResilienceConfig(),
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

    await mqtt.emit("owl/printers/device-123/commands/pause", message)

    assert moonraker.actions == ["pause"]

    assert mqtt.subscriptions == [("owl/printers/device-123/commands/#", 1)]

    assert len(mqtt.published) == 2

    # First ack confirms receipt
    topic, payload, qos, retain = mqtt.published[0]
    accepted = json.loads(payload.decode("utf-8"))
    assert topic == "owl/printers/device-123/acks/pause"
    assert accepted["status"] == "accepted"
    assert accepted["commandId"] == "cmd-1"
    assert accepted["stage"] == "dispatch"
    assert "reason" not in accepted
    assert "timestamps" in accepted
    assert "acknowledgedAt" in accepted["timestamps"]
    assert qos == 1
    assert retain is False

    # Second ack reports final outcome
    topic, payload, qos, retain = mqtt.published[1]
    completed = json.loads(payload.decode("utf-8"))
    assert topic == "owl/printers/device-123/acks/pause"
    assert completed["status"] == "completed"
    assert completed["stage"] == "execution"
    assert completed["commandId"] == "cmd-1"
    assert "reason" not in completed
    assert "timestamps" in completed
    assert "acknowledgedAt" in completed["timestamps"]
    assert qos == 1
    assert retain is False

    await processor.stop()
    assert mqtt.unsubscriptions == ["owl/printers/device-123/commands/#"]


@pytest.mark.asyncio
async def test_command_processor_handles_unknown_action(config):
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    message = {
        "commandId": "cmd-2",
        "command": "scrub",
    }

    await mqtt.emit("owl/printers/device-123/commands/scrub", message)

    assert len(mqtt.published) == 2

    accepted = json.loads(mqtt.published[0][1].decode("utf-8"))
    assert accepted["status"] == "accepted"
    assert accepted["stage"] == "dispatch"
    assert accepted["correlation"]["tenantId"] == "tenant-99"
    assert mqtt.published[0][2] == 1

    failed = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert failed["status"] == "failed"
    assert failed["stage"] == "execution"
    assert failed["reason"]["code"] == "unsupported_command"
    assert mqtt.published[1][2] == 1
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

    result = mqtt.handler("owl/printers/device-123/commands/pause", b"not json")
    if hasattr(result, "__await__"):
        await result

    assert len(mqtt.published) == 1
    payload = json.loads(mqtt.published[0][1].decode("utf-8"))
    assert payload["status"] == "failed"
    assert payload.get("reason", {}).get("code") == "invalid_json"
    assert payload["stage"] == "dispatch"

    await processor.stop()


@pytest.mark.asyncio
async def test_command_processor_rejects_invalid_parameters(config):
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    message = {
        "commandId": "cmd-3",
        "command": "pause",
        "parameters": "not-an-object",
    }

    await mqtt.emit("owl/printers/device-123/commands/pause", message)

    assert len(mqtt.published) == 1
    payload = json.loads(mqtt.published[0][1].decode("utf-8"))
    assert payload["status"] == "failed"
    assert payload["stage"] == "dispatch"
    assert payload.get("reason", {}).get("code") == "invalid_parameters"

    await processor.stop()


@pytest.mark.asyncio
async def test_command_processor_abandons_inflight_on_stop(config):
    mqtt = FakeMQTT()
    telemetry = FakeTelemetry()
    processor = CommandProcessor(config, FakeMoonraker(), mqtt, telemetry=telemetry)

    message = CommandMessage(command_id="cmd-abandon", command="pause", parameters={})
    processor._begin_inflight("pause", message)  # type: ignore[attr-defined]
    assert processor.pending_count == 1

    await processor.abandon_inflight("reconnecting")

    assert processor.pending_count == 0
    assert len(mqtt.published) == 1
    topic, payload, qos, retain = mqtt.published[0]
    abandoned = json.loads(payload.decode("utf-8"))
    assert topic == "owl/printers/device-123/acks/pause"
    assert abandoned["status"] == "failed"
    assert abandoned["stage"] == "execution"
    assert abandoned["reason"]["code"] == "agent_restart"
    assert "reconnecting" in abandoned["reason"]["message"]
    assert qos == 1
    assert retain is False

    assert telemetry.records
    last_record = telemetry.records[-1]
    assert last_record["state"] == "abandoned"
    assert last_record["details"] is not None
    assert last_record["details"]["reason"] == "agent_restart"


@pytest.mark.asyncio
async def test_command_processor_replays_cached_ack_for_duplicate(config):
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    message = {
        "commandId": "cmd-dup",
        "action": "pause",
        "payload": {},
    }

    await mqtt.emit("owl/printers/device-123/commands/pause", message)
    assert len(mqtt.published) == 2
    assert moonraker.actions == ["pause"]

    await mqtt.emit("owl/printers/device-123/commands/pause", message)

    assert len(mqtt.published) == 3
    topic, payload, qos, retain = mqtt.published[-1]
    replay = json.loads(payload.decode("utf-8"))
    assert topic == "owl/printers/device-123/acks/pause"
    assert replay["status"] == "completed"
    assert replay["stage"] == "execution"
    assert replay["commandId"] == "cmd-dup"
    assert qos == 1
    assert retain is False
    assert moonraker.actions == ["pause"]

    await processor.stop()


@pytest.mark.asyncio
async def test_command_processor_handles_sensors_set_rate(config):
    mqtt = FakeMQTT()
    telemetry = FakeTelemetry()
    telemetry.next_expires_at = datetime(2025, 1, 1, 13, 0, tzinfo=timezone.utc)
    processor = CommandProcessor(
        config,
        FakeMoonraker(),
        mqtt,
        telemetry=telemetry,
    )

    await processor.start()

    message = {
        "commandId": "cmd-sensors",
        "command": "sensors:set-rate",
        "parameters": {
            "mode": "watch",
            "maxHz": 5.0,
            "durationSeconds": 120,
            "issuedAt": "2025-01-01T12:00:00Z",
        },
    }

    await mqtt.emit(
        "owl/printers/device-123/commands/sensors:set-rate",
        message,
    )

    assert len(telemetry.applied) == 1
    apply_args = telemetry.applied[0]
    assert apply_args["mode"] == "watch"
    assert apply_args["max_hz"] == pytest.approx(5.0)
    assert apply_args["duration_seconds"] == 120
    assert apply_args["requested_at"] == datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)

    completed_records = [r for r in telemetry.records if r["state"] == "completed"]
    assert completed_records
    completed = completed_records[-1]
    assert completed["command_type"] == "sensors:set-rate"
    details = completed["details"]
    assert details is not None
    assert details["mode"] == "watch"
    assert details["maxHz"] == 5.0
    assert details["durationSeconds"] == 120
    assert details["watchWindowExpiresUtc"] == "2025-01-01T13:00:00+00:00"

    assert len(mqtt.published) == 2
    first_topic, first_payload, _, _ = mqtt.published[0]
    assert first_topic == "owl/printers/device-123/acks/sensors%3Aset-rate"
    first_ack = json.loads(first_payload.decode("utf-8"))
    assert first_ack["status"] == "accepted"
    assert first_ack["stage"] == "dispatch"

    second_topic, second_payload, _, _ = mqtt.published[1]
    assert second_topic == "owl/printers/device-123/acks/sensors%3Aset-rate"
    second_ack = json.loads(second_payload.decode("utf-8"))
    assert second_ack["status"] == "completed"
    assert second_ack["stage"] == "execution"

    await processor.stop()


def test_command_processor_requires_device_id():
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

    with pytest.raises(CommandConfigurationError):
        CommandProcessor(config, FakeMoonraker(), FakeMQTT())
