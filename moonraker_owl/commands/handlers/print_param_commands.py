"""Print parameter control command handlers (speed, flow rate, Z offset)."""

from __future__ import annotations

import logging
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)


class PrintParamCommandsMixin:
    """Mixin providing print parameter command handlers.

    - print-param:set-speed  → M220 S<percent>
    - print-param:set-flow   → M221 S<percent>
    - print-param:set-z-offset → SET_GCODE_OFFSET Z_ADJUST=<mm> MOVE=1
    - print-param:reset-z-offset → SET_GCODE_OFFSET Z=0 MOVE=1
    """

    async def _execute_set_speed(self, message: CommandMessage) -> Dict[str, Any]:
        """Set print speed factor.

        Parameters:
            percent (int): Speed percentage (50-200).
        """
        params = message.parameters or {}

        percent = params.get("percent")
        if percent is None:
            raise CommandProcessingError(
                "percent parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        try:
            percent_value = int(percent)
        except (TypeError, ValueError):
            raise CommandProcessingError(
                "percent must be an integer value",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if percent_value < 1 or percent_value > 999:
            raise CommandProcessingError(
                f"Speed percent must be between 1 and 999, got: {percent_value}",
                code="invalid_speed",
                command_id=message.command_id,
            )

        script = f"M220 S{percent_value}"

        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to set speed: {exc}",
                code="gcode_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.debug("Speed -> %d%%", percent_value)
        return {"percent": percent_value}

    async def _execute_set_flow(self, message: CommandMessage) -> Dict[str, Any]:
        """Set extrusion flow rate factor.

        Parameters:
            percent (int): Flow rate percentage (50-200).
        """
        params = message.parameters or {}

        percent = params.get("percent")
        if percent is None:
            raise CommandProcessingError(
                "percent parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        try:
            percent_value = int(percent)
        except (TypeError, ValueError):
            raise CommandProcessingError(
                "percent must be an integer value",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if percent_value < 1 or percent_value > 999:
            raise CommandProcessingError(
                f"Flow percent must be between 1 and 999, got: {percent_value}",
                code="invalid_flow",
                command_id=message.command_id,
            )

        script = f"M221 S{percent_value}"

        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to set flow rate: {exc}",
                code="gcode_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.debug("Flow -> %d%%", percent_value)
        return {"percent": percent_value}

    async def _execute_set_z_offset(self, message: CommandMessage) -> Dict[str, Any]:
        """Adjust Z offset by a relative amount.

        Parameters:
            adjustMm (float): Relative Z adjustment in mm (e.g. 0.01, -0.05).
        """
        params = message.parameters or {}

        adjust_mm = params.get("adjustMm")
        if adjust_mm is None:
            raise CommandProcessingError(
                "adjustMm parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        try:
            adjust_value = float(adjust_mm)
        except (TypeError, ValueError):
            raise CommandProcessingError(
                "adjustMm must be a numeric value",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if abs(adjust_value) > 1.0:
            raise CommandProcessingError(
                f"Z adjustment must be within ±1.0mm, got: {adjust_value}",
                code="invalid_z_offset",
                command_id=message.command_id,
            )

        script = f"SET_GCODE_OFFSET Z_ADJUST={adjust_value:.3f} MOVE=1"

        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to adjust Z offset: {exc}",
                code="gcode_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.debug("Z offset adjust -> %.3fmm", adjust_value)
        return {"adjustMm": adjust_value}

    async def _execute_reset_z_offset(self, message: CommandMessage) -> Dict[str, Any]:
        """Reset Z offset to zero.

        Uses SET_GCODE_OFFSET Z=0 MOVE=1 to reset absolutely.
        """
        script = "SET_GCODE_OFFSET Z=0 MOVE=1"

        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to reset Z offset: {exc}",
                code="gcode_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.debug("Z offset reset to 0")
        return {"zOffsetMm": 0.0}
