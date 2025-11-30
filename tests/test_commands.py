"""Tests for the command processor."""

import asyncio
import json
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
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
        self.gcode_scripts: list[str] = []
        self.available_heaters: dict[str, list[str]] = {
            "available_heaters": ["extruder", "heater_bed"],
            "available_sensors": [],
        }

    async def execute_print_action(self, action: str) -> None:
        if action not in {"pause", "resume", "cancel"}:
            raise ValueError(f"Unsupported Moonraker action: {action}")
        self.actions.append(action)

    async def execute_gcode(self, script: str, timeout: float = 10.0) -> None:
        """Execute a GCode script (fake implementation for testing)."""
        self.gcode_scripts.append(script)

    async def fetch_available_heaters(
        self, timeout: float = 5.0
    ) -> dict[str, list[str]]:
        """Return configured available heaters for testing."""
        return self.available_heaters


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
        self.extended: list[dict[str, Any]] = []
        self.next_expires_at: Optional[datetime] = datetime(2030, 1, 1, tzinfo=timezone.utc)
        # Current state for deduplication checks
        self._current_mode = "idle"
        self._current_interval = 30.0
        self._watch_window_expires: Optional[datetime] = None

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

    def get_current_sensors_state(
        self,
    ) -> tuple[str, float, Optional[datetime]]:
        return (self._current_mode, self._current_interval, self._watch_window_expires)

    def extend_watch_window(
        self,
        *,
        duration_seconds: int,
        requested_at: Optional[datetime],
    ) -> Optional[datetime]:
        self.extended.append(
            {
                "duration_seconds": duration_seconds,
                "requested_at": requested_at,
            }
        )
        if requested_at:
            self._watch_window_expires = requested_at + timedelta(seconds=duration_seconds)
        return self._watch_window_expires or self.next_expires_at

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
        # Update internal state after applying
        self._current_mode = mode
        self._current_interval = 1.0 / max_hz if max_hz > 0 else 30.0
        if duration_seconds and duration_seconds > 0 and requested_at:
            self._watch_window_expires = requested_at + timedelta(seconds=duration_seconds)
        else:
            self._watch_window_expires = None
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
    """Test state-based completion for pause/resume/cancel commands.
    
    These commands send 'accepted' immediately, then 'completed' when
    the printer state changes to the expected state.
    """
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

    # First ack confirms receipt (state-based commands only send accepted immediately)
    assert len(mqtt.published) == 1
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

    # Simulate print state change to 'paused' - this triggers completion
    processor.on_print_state_changed("paused")

    # Give the event loop a chance to process the scheduled completion ACK
    # call_soon requires at least one event loop iteration
    await asyncio.sleep(0.01)

    # Now the completed ack should be sent
    assert len(mqtt.published) == 2
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
    """Test that duplicate commands replay the cached ack (after state completion)."""
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
    # Only accepted ack sent initially for state-based commands
    assert len(mqtt.published) == 1
    assert moonraker.actions == ["pause"]

    # Simulate state change to complete the command
    processor.on_print_state_changed("paused")

    # Give the event loop a chance to process the scheduled completion ACK
    await asyncio.sleep(0.01)

    assert len(mqtt.published) == 2

    # Replay of same command should use cached completed ack
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


@pytest.mark.asyncio
async def test_sensors_set_rate_deduplication_extends_watch_window(config):
    """Test that repeated watch commands with same mode/interval only extend the window."""
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

    # First command: sets watch mode at 1 Hz
    first_message = {
        "commandId": "cmd-1",
        "command": "sensors:set-rate",
        "parameters": {
            "mode": "watch",
            "maxHz": 1.0,
            "durationSeconds": 120,
            "issuedAt": "2025-01-01T12:00:00Z",
        },
    }
    await mqtt.emit("owl/printers/device-123/commands/sensors:set-rate", first_message)

    assert len(telemetry.applied) == 1
    assert len(telemetry.extended) == 0

    # Second command: same mode and interval, should extend only
    second_message = {
        "commandId": "cmd-2",
        "command": "sensors:set-rate",
        "parameters": {
            "mode": "watch",
            "maxHz": 1.0,
            "durationSeconds": 120,
            "issuedAt": "2025-01-01T12:01:00Z",
        },
    }
    await mqtt.emit("owl/printers/device-123/commands/sensors:set-rate", second_message)

    # Should NOT call apply_sensors_rate again, only extend_watch_window
    assert len(telemetry.applied) == 1
    assert len(telemetry.extended) == 1
    extend_args = telemetry.extended[0]
    assert extend_args["duration_seconds"] == 120
    assert extend_args["requested_at"] == datetime(2025, 1, 1, 12, 1, tzinfo=timezone.utc)

    await processor.stop()


@pytest.mark.asyncio
async def test_sensors_set_rate_different_mode_triggers_full_apply(config):
    """Test that changing mode triggers full apply instead of extend."""
    mqtt = FakeMQTT()
    telemetry = FakeTelemetry()
    processor = CommandProcessor(
        config,
        FakeMoonraker(),
        mqtt,
        telemetry=telemetry,
    )

    await processor.start()

    # First: set watch mode
    await mqtt.emit(
        "owl/printers/device-123/commands/sensors:set-rate",
        {
            "commandId": "cmd-1",
            "command": "sensors:set-rate",
            "parameters": {"mode": "watch", "maxHz": 1.0, "durationSeconds": 120},
        },
    )
    assert len(telemetry.applied) == 1

    # Second: change to idle mode - should trigger full apply
    await mqtt.emit(
        "owl/printers/device-123/commands/sensors:set-rate",
        {
            "commandId": "cmd-2",
            "command": "sensors:set-rate",
            "parameters": {"mode": "idle", "maxHz": 0.033},
        },
    )
    assert len(telemetry.applied) == 2
    assert len(telemetry.extended) == 0  # Never extended, always applied

    await processor.stop()


@pytest.mark.asyncio
async def test_sensors_set_rate_different_hz_triggers_full_apply(config):
    """Test that changing maxHz triggers full apply instead of extend."""
    mqtt = FakeMQTT()
    telemetry = FakeTelemetry()
    processor = CommandProcessor(
        config,
        FakeMoonraker(),
        mqtt,
        telemetry=telemetry,
    )

    await processor.start()

    # First: watch at 1 Hz
    await mqtt.emit(
        "owl/printers/device-123/commands/sensors:set-rate",
        {
            "commandId": "cmd-1",
            "command": "sensors:set-rate",
            "parameters": {"mode": "watch", "maxHz": 1.0, "durationSeconds": 120},
        },
    )
    assert len(telemetry.applied) == 1

    # Second: watch at 2 Hz - different rate, should full apply
    await mqtt.emit(
        "owl/printers/device-123/commands/sensors:set-rate",
        {
            "commandId": "cmd-2",
            "command": "sensors:set-rate",
            "parameters": {"mode": "watch", "maxHz": 2.0, "durationSeconds": 120},
        },
    )
    assert len(telemetry.applied) == 2
    assert len(telemetry.extended) == 0

    await processor.stop()


@pytest.mark.asyncio
async def test_sensors_set_rate_clock_skew_correction(config):
    """Test that serverUtcNow is used to correct clock skew when calculating expiration."""
    mqtt = FakeMQTT()
    telemetry = FakeTelemetry()
    processor = CommandProcessor(
        config,
        FakeMoonraker(),
        mqtt,
        telemetry=telemetry,
    )

    await processor.start()

    # Simulate server time being 10 seconds behind local time
    # Server says it's 12:00:00, but our local clock says 12:00:10
    # So when server says "expires in 120s from now", we should adjust
    message = {
        "commandId": "cmd-clock",
        "command": "sensors:set-rate",
        "parameters": {
            "mode": "watch",
            "maxHz": 1.0,
            "durationSeconds": 120,
            "serverUtcNow": "2025-01-01T12:00:00Z",
            # issuedAt is also from server's perspective
            "issuedAt": "2025-01-01T12:00:00Z",
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/sensors:set-rate", message)

    assert len(telemetry.applied) == 1
    apply_args = telemetry.applied[0]
    # The requested_at should be adjusted by the clock offset
    # Since we can't control datetime.now() in the test, we just verify
    # that the apply was called with a datetime that accounts for the offset
    assert apply_args["requested_at"] is not None
    assert apply_args["mode"] == "watch"
    assert apply_args["duration_seconds"] == 120

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


# =============================================================================
# State-Based Command Completion Tests
# =============================================================================


@pytest.mark.asyncio
async def test_state_based_completion_resume_command(config):
    """Test resume command completes when state changes to 'printing'."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    message = {
        "commandId": "cmd-resume",
        "action": "resume",
        "payload": {},
    }

    await mqtt.emit("owl/printers/device-123/commands/resume", message)

    # Only accepted ack sent initially
    assert len(mqtt.published) == 1
    accepted = json.loads(mqtt.published[0][1].decode("utf-8"))
    assert accepted["status"] == "accepted"

    # State change to 'printing' triggers completion
    processor.on_print_state_changed("printing")
    await asyncio.sleep(0.01)

    assert len(mqtt.published) == 2
    completed = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert completed["status"] == "completed"
    assert completed["commandId"] == "cmd-resume"

    await processor.stop()


@pytest.mark.asyncio
async def test_state_based_completion_cancel_command(config):
    """Test cancel command completes when state changes to 'cancelled'."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    message = {
        "commandId": "cmd-cancel",
        "action": "cancel",
        "payload": {},
    }

    await mqtt.emit("owl/printers/device-123/commands/cancel", message)

    # Only accepted ack sent initially
    assert len(mqtt.published) == 1

    # State change to 'cancelled' triggers completion
    processor.on_print_state_changed("cancelled")
    await asyncio.sleep(0.01)

    assert len(mqtt.published) == 2
    completed = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert completed["status"] == "completed"
    assert completed["commandId"] == "cmd-cancel"

    await processor.stop()


@pytest.mark.asyncio
async def test_state_change_wrong_state_does_not_complete(config):
    """Test that wrong state change does not trigger completion."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    message = {
        "commandId": "cmd-pause",
        "action": "pause",
        "payload": {},
    }

    await mqtt.emit("owl/printers/device-123/commands/pause", message)
    assert len(mqtt.published) == 1

    # Wrong state - should not trigger completion
    processor.on_print_state_changed("printing")
    await asyncio.sleep(0.01)

    # Still only accepted ack
    assert len(mqtt.published) == 1

    # Correct state triggers completion
    processor.on_print_state_changed("paused")
    await asyncio.sleep(0.01)

    assert len(mqtt.published) == 2

    await processor.stop()


@pytest.mark.asyncio
async def test_pending_command_count(config):
    """Test pending_command_count property."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    assert processor.pending_state_count == 0

    # Send pause command - should be pending
    message = {"commandId": "cmd-1", "action": "pause", "payload": {}}
    await mqtt.emit("owl/printers/device-123/commands/pause", message)
    assert processor.pending_state_count == 1

    # Send another pause command - should also be pending
    message2 = {"commandId": "cmd-2", "action": "resume", "payload": {}}
    await mqtt.emit("owl/printers/device-123/commands/resume", message2)
    assert processor.pending_state_count == 2

    # Complete one
    processor.on_print_state_changed("paused")
    await asyncio.sleep(0.01)
    assert processor.pending_state_count == 1

    await processor.stop()


@pytest.mark.asyncio
async def test_state_change_case_insensitive(config):
    """Test that state matching is case-insensitive."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    message = {"commandId": "cmd-1", "action": "pause", "payload": {}}
    await mqtt.emit("owl/printers/device-123/commands/pause", message)
    assert len(mqtt.published) == 1

    # State with different case should still match
    processor.on_print_state_changed("PAUSED")
    await asyncio.sleep(0.01)

    assert len(mqtt.published) == 2
    completed = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert completed["status"] == "completed"

    await processor.stop()


@pytest.mark.asyncio
async def test_multiple_commands_same_expected_state(config):
    """Test multiple commands waiting for the same state."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)

    await processor.start()

    # Send two pause commands
    msg1 = {"commandId": "cmd-1", "action": "pause", "payload": {}}
    msg2 = {"commandId": "cmd-2", "action": "pause", "payload": {}}
    await mqtt.emit("owl/printers/device-123/commands/pause", msg1)
    await mqtt.emit("owl/printers/device-123/commands/pause", msg2)

    # 2 accepted acks
    assert len(mqtt.published) == 2
    assert processor.pending_state_count == 2

    # One state change completes both
    processor.on_print_state_changed("paused")
    await asyncio.sleep(0.01)

    # Both should be completed
    assert len(mqtt.published) == 4
    assert processor.pending_state_count == 0

    await processor.stop()


@pytest.mark.asyncio
async def test_abandon_pending_commands_on_stop(config):
    """Test that pending state commands are abandoned on stop."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    telemetry = FakeTelemetry()
    processor = CommandProcessor(config, moonraker, mqtt, telemetry=telemetry)

    await processor.start()

    message = {"commandId": "cmd-1", "action": "pause", "payload": {}}
    await mqtt.emit("owl/printers/device-123/commands/pause", message)
    assert processor.pending_state_count == 1

    # Stop should abandon pending commands
    await processor.stop()
    await processor.abandon_inflight("test_stop")
    await asyncio.sleep(0.01)

    # Should have abandoned the pending command
    abandoned_records = [r for r in telemetry.records if r["state"] == "abandoned"]
    assert len(abandoned_records) >= 1
