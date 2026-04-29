"""Security regression tests for command handlers (audit A-01).

Each test pushes a malicious cloud payload through the real CommandProcessor
and asserts that:

1. No G-code script was sent to Moonraker (`gcode_scripts` stays empty).
2. The cloud receives a rejection ACK with the documented error code.

This covers led / output_pin / fan / gcode-macro / heater handlers and the
macro pre-flight fail-closed behaviour.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from moonraker_owl.commands import CommandProcessor
from tests.test_commands import FakeMoonraker, FakeMQTT  # reuse existing fakes


@pytest.fixture
def config():
    from helpers import build_config

    return build_config()


def _last_ack_payload(mqtt: FakeMQTT) -> dict[str, Any]:
    assert mqtt.published, "no ACK was published"
    _, payload, _, _ = mqtt.published[-1]
    return json.loads(payload.decode("utf-8"))


def _assert_rejected(mqtt: FakeMQTT, expected_code_prefix: str) -> None:
    """The last ACK must be a non-success terminal state with the expected error code."""
    final = _last_ack_payload(mqtt)
    status = final["payload"]["status"]
    assert status in {"rejected", "failed"}, (
        f"expected rejected/failed status, got {status!r}: {final}"
    )
    reason = final["payload"].get("reason") or {}
    assert isinstance(reason, dict), f"reason not a dict: {reason!r}"
    code = reason.get("code", "")
    assert code.startswith(expected_code_prefix), (
        f"expected error code starting with {expected_code_prefix!r}, got {code!r}"
    )


async def _emit(mqtt: FakeMQTT, command: str, parameters: dict[str, Any]) -> None:
    await mqtt.emit(
        f"owl/printers/device-123/commands/{command}",
        {
            "$id": "cmd-injection-1",
            "payload": {
                "command": command,
                "parameters": parameters,
            },
        },
    )


# ---------------------------------------------------------------------------
# led:set-brightness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "evil_led",
    [
        "my_led\nM112",                       # newline → emergency stop on next line
        "my_led RED=1 GREEN=1",                # space → terminates LED= argument
        "my_led;FIRMWARE_RESTART",             # semicolon
        "neopixel my_led\rEMERGENCY_STOP",     # carriage return after prefix-strip
        "../../etc/passwd",                    # path-ish payload
        "led=foo",                             # = sign
    ],
)
async def test_set_led_rejects_injection(config, evil_led: str) -> None:
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    await _emit(
        mqtt,
        "led:set-brightness",
        {"led": evil_led, "brightness": 0.5},
    )

    assert moonraker.gcode_scripts == [], (
        "Injection payload reached Moonraker: " + repr(moonraker.gcode_scripts)
    )
    _assert_rejected(mqtt, "invalid_led")


# ---------------------------------------------------------------------------
# output-pin:set-value
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "evil_pin",
    [
        "my_pin\nM112",
        "my_pin VALUE=1.0",
        "output_pin my_pin;EMERGENCY_STOP",
    ],
)
async def test_set_output_pin_rejects_injection(config, evil_pin: str) -> None:
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    await _emit(
        mqtt,
        "output-pin:set-value",
        {"pin": evil_pin, "value": 0.5},
    )

    assert moonraker.gcode_scripts == []
    _assert_rejected(mqtt, "invalid_pin")


# ---------------------------------------------------------------------------
# fan:set-speed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "evil_fan",
    [
        "my_fan\nM112",
        "my_fan SPEED=1.0",
        "fan_generic my_fan;G28",
    ],
)
async def test_fan_set_speed_rejects_injection(config, evil_fan: str) -> None:
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    await _emit(
        mqtt,
        "fan:set-speed",
        {"fan": evil_fan, "speed": 0.5},
    )

    assert moonraker.gcode_scripts == []
    _assert_rejected(mqtt, "invalid_fan")


@pytest.mark.asyncio
async def test_fan_set_speed_part_cooling_still_works(config) -> None:
    """Sanity: the legitimate 'fan' alias must continue to work after hardening."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    await _emit(
        mqtt,
        "fan:set-speed",
        {"fan": "fan", "speed": 0.5},
    )

    # Part-cooling fan uses M106/M107, not SET_FAN_SPEED — the fix must not
    # have broken that branch.
    assert any(s.startswith("M106") or s.startswith("M107") for s in moonraker.gcode_scripts)


# ---------------------------------------------------------------------------
# print:gcode-macro — injection + fail-closed pre-flight
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "evil_macro",
    [
        "MY_MACRO\nM112",
        "MY_MACRO ;EMERGENCY_STOP",
        "MY MACRO",  # space
    ],
)
async def test_gcode_macro_rejects_injection(config, evil_macro: str) -> None:
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    await _emit(
        mqtt,
        "gcode:macro",
        {"macro": evil_macro},
    )

    assert moonraker.gcode_scripts == []
    _assert_rejected(mqtt, "invalid_macro")


@pytest.mark.asyncio
async def test_gcode_macro_legitimate_macro_executes(config) -> None:
    moonraker = FakeMoonraker()
    moonraker.registered_macros = {"LOAD_FILAMENT"}
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    await _emit(mqtt, "gcode:macro", {"macro": "LOAD_FILAMENT"})

    assert moonraker.gcode_scripts == ["LOAD_FILAMENT"]


@pytest.mark.asyncio
async def test_gcode_macro_preflight_failure_is_fail_closed(config) -> None:
    """If the existence check raises, we must refuse — not 'proceed anyway'.

    Audit Q-5: the previous behaviour silently allowed macro dispatch when
    the Moonraker query layer was degraded, weakening defense against a
    compromised cloud bus.
    """
    moonraker = FakeMoonraker()

    async def boom(*args: Any, **kwargs: Any) -> None:
        raise TimeoutError("simulated moonraker timeout")

    moonraker.fetch_printer_state = boom  # type: ignore[assignment]
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    await _emit(mqtt, "gcode:macro", {"macro": "LOAD_FILAMENT"})

    assert moonraker.gcode_scripts == [], (
        "macro was executed despite pre-flight failure: "
        + repr(moonraker.gcode_scripts)
    )
    _assert_rejected(mqtt, "macro_preflight_failed")


# ---------------------------------------------------------------------------
# heater:set-target — defense-in-depth on top of allow-list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_heater_set_target_rejects_injection(config) -> None:
    moonraker = FakeMoonraker()
    moonraker.available_heaters = {
        "available_heaters": ["extruder", "heater_bed"],
        "available_sensors": [],
    }
    mqtt = FakeMQTT()
    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    await _emit(
        mqtt,
        "heater:set-target",
        {"heater": "extruder\nM112", "target": 200},
    )

    assert moonraker.gcode_scripts == []
    _assert_rejected(mqtt, "invalid_heater")
