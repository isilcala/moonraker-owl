"""Output pin control command handlers."""

from __future__ import annotations

import logging
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError
from ._validation import validate_klipper_identifier

LOGGER = logging.getLogger(__name__)


class OutputPinCommandsMixin:
    """Mixin providing output pin command handlers.

    - output-pin:set-value → SET_PIN PIN=<name> VALUE=<0.0-1.0>
    """

    async def _execute_set_output_pin(self, message: CommandMessage) -> Dict[str, Any]:
        """Set output pin value.

        Parameters:
            pin (str): Output pin object name (e.g., 'output_pin my_pin')
            value (float): Pin value from 0.0 to 1.0 (0% to 100%)

        GCode: SET_PIN PIN=<config_name> VALUE=<0.0-1.0>
        """
        params = message.parameters or {}

        pin = params.get("pin")
        if not pin or not isinstance(pin, str):
            raise CommandProcessingError(
                "pin parameter is required and must be a string",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        value = params.get("value")
        if value is None:
            raise CommandProcessingError(
                "value parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        try:
            pin_value = float(value)
        except (TypeError, ValueError):
            raise CommandProcessingError(
                "value must be a numeric value",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if pin_value < 0.0 or pin_value > 1.0:
            raise CommandProcessingError(
                f"Value must be between 0.0 and 1.0, got: {pin_value}",
                code="invalid_value",
                command_id=message.command_id,
            )

        pin_lower = pin.strip().lower()

        # Extract the config name: "output_pin my_pin" → "my_pin"
        if pin_lower.startswith("output_pin "):
            pin_name = pin_lower[len("output_pin "):]
        else:
            pin_name = pin_lower

        # Defense against G-code injection: see _validation module.
        pin_name = validate_klipper_identifier(
            pin_name, field="pin", command_id=message.command_id
        )

        script = f"SET_PIN PIN={pin_name} VALUE={pin_value:.2f}"

        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to set output pin: {exc}",
                code="output_pin_command_failed",
                command_id=message.command_id,
            )

        LOGGER.info(
            "Output pin set: %s → %.0f%%",
            pin_name,
            pin_value * 100,
        )

        return {
            "pin": pin,
            "value": pin_value,
        }
