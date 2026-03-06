"""Fan control command handlers."""

from __future__ import annotations

import logging
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)


class FanCommandsMixin:
    """Mixin providing fan command handlers."""

    async def _execute_fan_set_speed(self, message: CommandMessage) -> Dict[str, Any]:
        """Set fan speed.

        Parameters:
            fan (str): Fan object name (e.g., 'fan', 'fan_generic exhaust_fan')
            speed (float): Speed from 0.0 to 1.0 (0% to 100%)

        GCode for part cooling fan: M106 S<0-255>
        GCode for named fans: SET_FAN_SPEED FAN=<name> SPEED=<0.0-1.0>
        """
        params = message.parameters or {}

        fan = params.get("fan")
        if not fan or not isinstance(fan, str):
            raise CommandProcessingError(
                "fan parameter is required and must be a string",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        speed = params.get("speed")
        if speed is None:
            raise CommandProcessingError(
                "speed parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        try:
            speed_value = float(speed)
        except (TypeError, ValueError):
            raise CommandProcessingError(
                "speed must be a numeric value",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # Validate speed range (0.0 to 1.0)
        if speed_value < 0.0 or speed_value > 1.0:
            raise CommandProcessingError(
                f"Speed must be between 0.0 and 1.0, got: {speed_value}",
                code="invalid_speed",
                command_id=message.command_id,
            )

        fan_lower = fan.strip().lower()

        # Part cooling fan uses M106/M107
        if fan_lower == "fan":
            if speed_value == 0:
                script = "M107"  # Turn off fan
            else:
                # M106 uses 0-255 scale
                pwm_value = int(speed_value * 255)
                script = f"M106 S{pwm_value}"
            fan_name = "fan"
        else:
            # Named fans use SET_FAN_SPEED
            # Extract the short fan name from Moonraker object format:
            # - "fan_generic exhaust_fan" -> "exhaust_fan"
            # - "heater_fan hotend_fan" -> "hotend_fan"
            # - "controller_fan controller_fan" -> "controller_fan"
            if fan_lower.startswith("fan_generic "):
                fan_name = fan_lower[len("fan_generic "):]
            elif fan_lower.startswith("heater_fan "):
                fan_name = fan_lower[len("heater_fan "):]
            elif fan_lower.startswith("controller_fan "):
                fan_name = fan_lower[len("controller_fan "):]
            else:
                # Assume fan_lower is already the short name
                fan_name = fan_lower

            script = f"SET_FAN_SPEED FAN={fan_name} SPEED={speed_value:.2f}"

        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to set fan speed: {exc}",
                code="gcode_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.debug(
            "Fan %s -> %.0f%%",
            fan_name,
            speed_value * 100,
        )

        return {
            "fan": fan_name,
            "speed": speed_value,
        }
