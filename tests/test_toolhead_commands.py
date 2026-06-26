"""Tests for multi-toolhead control command handlers (Phase 3).

Covers archetype-aware G-code selection (multi-extruder / IDEX / toolchanger),
the collision-aware idle gate, parameter validation, and IDEX mode handling.
"""

import pytest

from moonraker_owl.commands import CommandMessage, CommandProcessor
from moonraker_owl.commands.types import CommandProcessingError
from moonraker_owl.printer_command_names import PrinterCommandNames

from test_commands import FakeMoonraker, FakeMQTT


@pytest.fixture
def config():
    from helpers import build_config

    return build_config()


def _multi_extruder_moonraker(state: str = "standby") -> FakeMoonraker:
    moonraker = FakeMoonraker()
    moonraker.printer_state = state
    moonraker.available_heaters = {
        "available_heaters": ["extruder", "extruder1", "extruder2", "heater_bed"],
        "available_sensors": [],
    }
    return moonraker


def _activate(tool) -> CommandMessage:
    return CommandMessage(
        command_id="t-activate",
        command=PrinterCommandNames.TOOLHEAD_ACTIVATE_TOOL,
        parameters={"tool": tool},
    )


def _set_mode(mode) -> CommandMessage:
    return CommandMessage(
        command_id="t-mode",
        command=PrinterCommandNames.IDEX_SET_MODE,
        parameters={"mode": mode},
    )


# ── Tool activation: archetype-aware G-code ─────────────────────────────────


@pytest.mark.asyncio
async def test_activate_tool_multi_extruder_emits_activate_extruder(config):
    moonraker = _multi_extruder_moonraker()
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    result = await processor._execute_toolhead_activate_tool(_activate(1))

    assert moonraker.gcode_scripts == ["ACTIVATE_EXTRUDER EXTRUDER=extruder1"]
    assert result == {"tool": 1}


@pytest.mark.asyncio
async def test_activate_tool_zero_uses_base_extruder(config):
    moonraker = _multi_extruder_moonraker()
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    await processor._execute_toolhead_activate_tool(_activate(0))

    assert moonraker.gcode_scripts == ["ACTIVATE_EXTRUDER EXTRUDER=extruder"]


@pytest.mark.asyncio
async def test_activate_tool_idex_emits_dual_carriage_then_activate(config):
    moonraker = _multi_extruder_moonraker()
    moonraker.idex = True
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    await processor._execute_toolhead_activate_tool(_activate(1))

    assert moonraker.gcode_scripts == [
        "SET_DUAL_CARRIAGE CARRIAGE=1 MODE=PRIMARY\nACTIVATE_EXTRUDER EXTRUDER=extruder1"
    ]


@pytest.mark.asyncio
async def test_activate_tool_generic_cartesian_idex_uses_configured_carriage_name(config):
    moonraker = _multi_extruder_moonraker()
    moonraker.idex = True
    moonraker.dual_carriage_status = {
        "carriages": {"carriage_x": "PRIMARY", "carriage_u": "INACTIVE"}
    }
    moonraker.configfile_settings = {
        "dual_carriage carriage_u": {
            "primary_carriage": "carriage_x",
            "axis": "x",
        }
    }
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    await processor._execute_toolhead_activate_tool(_activate(1))

    assert moonraker.gcode_scripts == [
        "SET_DUAL_CARRIAGE CARRIAGE=carriage_u MODE=PRIMARY\nACTIVATE_EXTRUDER EXTRUDER=extruder1"
    ]


@pytest.mark.asyncio
async def test_activate_tool_toolchanger_emits_t_command(config):
    moonraker = _multi_extruder_moonraker()
    moonraker.toolchanger = True
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    await processor._execute_toolhead_activate_tool(_activate(2))

    # Toolchanger T<n> macro owns the physical change; no extruder-name dependency.
    assert moonraker.gcode_scripts == ["T2"]


# ── Collision-aware idle gate ───────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["printing", "paused", "cancelled"])
async def test_activate_tool_rejected_mid_print(config, state):
    moonraker = _multi_extruder_moonraker(state=state)
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    with pytest.raises(CommandProcessingError) as exc:
        await processor._execute_toolhead_activate_tool(_activate(1))

    assert exc.value.code == "printer_busy"
    assert moonraker.gcode_scripts == []  # no physical move issued


@pytest.mark.asyncio
async def test_activate_tool_rejected_when_klippy_not_ready(config):
    moonraker = _multi_extruder_moonraker()
    moonraker.klippy_state = "shutdown"
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    with pytest.raises(CommandProcessingError) as exc:
        await processor._execute_toolhead_activate_tool(_activate(1))

    assert exc.value.code == "printer_unavailable"
    assert moonraker.gcode_scripts == []


@pytest.mark.asyncio
async def test_activate_tool_rejected_when_state_unavailable(config):
    moonraker = _multi_extruder_moonraker()

    async def _boom(*_args, **_kwargs):
        raise RuntimeError("moonraker unreachable")

    moonraker.fetch_printer_state = _boom
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    with pytest.raises(CommandProcessingError) as exc:
        await processor._execute_toolhead_activate_tool(_activate(1))

    assert exc.value.code == "state_unavailable"
    assert moonraker.gcode_scripts == []


@pytest.mark.asyncio
async def test_activate_tool_unknown_extruder_rejected(config):
    moonraker = _multi_extruder_moonraker()  # has extruder/1/2
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    with pytest.raises(CommandProcessingError) as exc:
        await processor._execute_toolhead_activate_tool(_activate(5))

    assert exc.value.code == "invalid_tool"
    assert moonraker.gcode_scripts == []


# ── Parameter validation ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_activate_tool_missing_param_rejected(config):
    moonraker = _multi_extruder_moonraker()
    processor = CommandProcessor(config, moonraker, FakeMQTT())
    msg = CommandMessage(
        command_id="t", command=PrinterCommandNames.TOOLHEAD_ACTIVATE_TOOL, parameters={}
    )

    with pytest.raises(CommandProcessingError) as exc:
        await processor._execute_toolhead_activate_tool(msg)

    assert exc.value.code == "invalid_parameters"


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", [-1, 999, "abc", None])
async def test_activate_tool_invalid_index_rejected(config, tool):
    moonraker = _multi_extruder_moonraker()
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    with pytest.raises(CommandProcessingError) as exc:
        await processor._execute_toolhead_activate_tool(_activate(tool))

    assert exc.value.code == "invalid_parameters"
    assert moonraker.gcode_scripts == []


# ── IDEX mode ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_idex_set_mode_copy(config):
    moonraker = _multi_extruder_moonraker()
    moonraker.idex = True
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    result = await processor._execute_idex_set_mode(_set_mode("COPY"))

    assert moonraker.gcode_scripts == ["SET_DUAL_CARRIAGE CARRIAGE=1 MODE=COPY"]
    assert result == {"mode": "COPY", "carriage": 1}


@pytest.mark.asyncio
async def test_idex_set_mode_generic_cartesian_uses_configured_carriage_name(config):
    moonraker = _multi_extruder_moonraker()
    moonraker.idex = True
    moonraker.dual_carriage_status = {
        "carriages": {"carriage_x": "PRIMARY", "carriage_u": "PRIMARY"}
    }
    moonraker.configfile_settings = {
        "dual_carriage carriage_u": {
            "primary_carriage": "carriage_x",
            "axis": "x",
        }
    }
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    result = await processor._execute_idex_set_mode(_set_mode("PRIMARY"))

    assert moonraker.gcode_scripts == ["SET_DUAL_CARRIAGE CARRIAGE=carriage_x MODE=PRIMARY"]
    assert result == {"mode": "PRIMARY", "carriage": "carriage_x"}


@pytest.mark.asyncio
async def test_idex_set_mode_mirror_is_case_insensitive(config):
    moonraker = _multi_extruder_moonraker()
    moonraker.idex = True
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    await processor._execute_idex_set_mode(_set_mode("mirror"))

    assert moonraker.gcode_scripts == ["SET_DUAL_CARRIAGE CARRIAGE=1 MODE=MIRROR"]


@pytest.mark.asyncio
async def test_idex_set_mode_primary_targets_carriage_zero(config):
    moonraker = _multi_extruder_moonraker()
    moonraker.idex = True
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    result = await processor._execute_idex_set_mode(_set_mode("PRIMARY"))

    assert moonraker.gcode_scripts == ["SET_DUAL_CARRIAGE CARRIAGE=0 MODE=PRIMARY"]
    assert result == {"mode": "PRIMARY", "carriage": 0}


@pytest.mark.asyncio
async def test_idex_set_mode_invalid_mode_rejected(config):
    moonraker = _multi_extruder_moonraker()
    moonraker.idex = True
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    with pytest.raises(CommandProcessingError) as exc:
        await processor._execute_idex_set_mode(_set_mode("TURBO; M112"))

    assert exc.value.code == "invalid_mode"
    assert moonraker.gcode_scripts == []  # no injection reaches Klipper


@pytest.mark.asyncio
async def test_idex_set_mode_not_idex_rejected(config):
    moonraker = _multi_extruder_moonraker()  # idex stays False
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    with pytest.raises(CommandProcessingError) as exc:
        await processor._execute_idex_set_mode(_set_mode("COPY"))

    assert exc.value.code == "not_idex"
    assert moonraker.gcode_scripts == []


@pytest.mark.asyncio
async def test_idex_set_mode_rejected_mid_print(config):
    moonraker = _multi_extruder_moonraker(state="printing")
    moonraker.idex = True
    processor = CommandProcessor(config, moonraker, FakeMQTT())

    with pytest.raises(CommandProcessingError) as exc:
        await processor._execute_idex_set_mode(_set_mode("COPY"))

    assert exc.value.code == "printer_busy"
    assert moonraker.gcode_scripts == []


# ── Dispatch-table registration (end-to-end via MQTT) ───────────────────────


@pytest.mark.asyncio
async def test_toolhead_commands_registered_in_dispatch(config):
    processor = CommandProcessor(config, _multi_extruder_moonraker(), FakeMQTT())

    assert PrinterCommandNames.TOOLHEAD_ACTIVATE_TOOL in processor._dispatch
    assert PrinterCommandNames.IDEX_SET_MODE in processor._dispatch


def test_toolhead_commands_are_hot_path():
    """Dual-path contract (ADR-0039): control commands are Hot Path, never Cold Path."""
    for command in (
        PrinterCommandNames.TOOLHEAD_ACTIVATE_TOOL,
        PrinterCommandNames.IDEX_SET_MODE,
    ):
        assert PrinterCommandNames.is_hot_path(command)
        assert not PrinterCommandNames.is_cold_path(command)

