"""LED control command handlers."""

from __future__ import annotations

import logging
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError
from ._validation import validate_klipper_identifier

LOGGER = logging.getLogger(__name__)


class LedCommandsMixin:
    """Mixin providing LED command handlers.

    - led:set-brightness → SET_LED LED=<name> WHITE=<val> (or RGB=<val>)
    """

    async def _execute_set_led(self, message: CommandMessage) -> Dict[str, Any]:
        """Set LED brightness.

        Parameters:
            led (str): LED object name (e.g., 'neopixel my_led', 'dotstar bar')
            brightness (float): Brightness from 0.0 to 1.0 (0% to 100%)

        GCode: SET_LED LED=<config_name> RED=<val> GREEN=<val> BLUE=<val> WHITE=<val>
        For simplicity we set all colour channels to the same brightness value,
        producing a white/dim effect.  Per-channel colour control can be added later.
        """
        params = message.parameters or {}

        led = params.get("led")
        if not led or not isinstance(led, str):
            raise CommandProcessingError(
                "led parameter is required and must be a string",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        brightness = params.get("brightness")
        if brightness is None:
            raise CommandProcessingError(
                "brightness parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        try:
            brightness_value = float(brightness)
        except (TypeError, ValueError):
            raise CommandProcessingError(
                "brightness must be a numeric value",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if brightness_value < 0.0 or brightness_value > 1.0:
            raise CommandProcessingError(
                f"Brightness must be between 0.0 and 1.0, got: {brightness_value}",
                code="invalid_brightness",
                command_id=message.command_id,
            )

        led_lower = led.strip().lower()

        # Extract the config name from Moonraker object format:
        # "neopixel my_led" → "my_led", "dotstar bar" → "bar", "led foo" → "foo"
        for prefix in ("neopixel ", "dotstar ", "led "):
            if led_lower.startswith(prefix):
                led_name = led_lower[len(prefix):]
                break
        else:
            led_name = led_lower

        # Defense against G-code injection: reject any name with whitespace,
        # newlines, '=' or other metacharacters before interpolating into the
        # SET_LED script. See _validation.validate_klipper_identifier.
        led_name = validate_klipper_identifier(
            led_name, field="led", command_id=message.command_id
        )

        v = f"{brightness_value:.2f}"
        script = f"SET_LED LED={led_name} RED={v} GREEN={v} BLUE={v} WHITE={v}"

        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to set LED brightness: {exc}",
                code="led_command_failed",
                command_id=message.command_id,
            )

        LOGGER.info(
            "LED brightness set: %s → %.0f%%",
            led_name,
            brightness_value * 100,
        )

        return {
            "led": led,
            "brightness": brightness_value,
        }
