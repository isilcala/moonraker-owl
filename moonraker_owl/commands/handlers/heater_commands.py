"""Heater control command handlers."""

from __future__ import annotations

import logging
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)


class HeaterCommandsMixin:
    """Mixin providing heater command handlers."""

    async def _execute_heater_set_target(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Set target temperature for a heater.

        Parameters:
            heater (str): Heater object name (e.g., 'extruder', 'heater_bed')
            target (float): Target temperature in Celsius

        GCode: SET_HEATER_TEMPERATURE HEATER=<name> TARGET=<temp>
        """
        params = message.parameters or {}

        heater = params.get("heater")
        if not heater or not isinstance(heater, str):
            raise CommandProcessingError(
                "heater parameter is required and must be a string",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        target = params.get("target")
        if target is None:
            raise CommandProcessingError(
                "target parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        try:
            target_temp = float(target)
        except (TypeError, ValueError):
            raise CommandProcessingError(
                "target must be a numeric value",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # Validate heater exists and get temperature limits
        heater_normalized = heater.strip().lower()
        valid_heaters = await self._get_valid_heaters()
        if heater_normalized not in valid_heaters:
            raise CommandProcessingError(
                f"Unknown heater: {heater}. Valid heaters: {', '.join(sorted(valid_heaters))}",
                code="invalid_heater",
                command_id=message.command_id,
            )

        # Validate temperature range
        max_temp = self._get_heater_max_temp(heater_normalized)
        if target_temp < 0:
            raise CommandProcessingError(
                f"Target temperature cannot be negative: {target_temp}",
                code="invalid_target",
                command_id=message.command_id,
            )
        if target_temp > max_temp:
            raise CommandProcessingError(
                f"Target temperature {target_temp}°C exceeds maximum {max_temp}°C for {heater}",
                code="invalid_target",
                command_id=message.command_id,
            )

        # Execute GCode
        # Use SET_HEATER_TEMPERATURE which works for all heater types
        script = f"SET_HEATER_TEMPERATURE HEATER={heater_normalized} TARGET={target_temp:.1f}"
        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to set heater temperature: {exc}",
                code="gcode_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.debug(
            "Heater %s -> %.1f°C",
            heater_normalized,
            target_temp,
        )

        return {
            "heater": heater_normalized,
            "target": target_temp,
        }

    async def _execute_heater_turn_off(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Turn off a specific heater by setting target to 0.

        Parameters:
            heater (str): Heater object name (e.g., 'extruder', 'heater_bed')

        GCode: SET_HEATER_TEMPERATURE HEATER=<name> TARGET=0
        """
        params = message.parameters or {}

        heater = params.get("heater")
        if not heater or not isinstance(heater, str):
            raise CommandProcessingError(
                "heater parameter is required and must be a string",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        heater_normalized = heater.strip().lower()
        valid_heaters = await self._get_valid_heaters()
        if heater_normalized not in valid_heaters:
            raise CommandProcessingError(
                f"Unknown heater: {heater}. Valid heaters: {', '.join(sorted(valid_heaters))}",
                code="invalid_heater",
                command_id=message.command_id,
            )

        script = f"SET_HEATER_TEMPERATURE HEATER={heater_normalized} TARGET=0"
        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to turn off heater: {exc}",
                code="gcode_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.debug(
            "Heater %s off",
            heater_normalized,
        )

        return {
            "heater": heater_normalized,
            "target": 0,
        }

    async def _get_valid_heaters(self) -> set[str]:
        """Get set of valid heater names from Moonraker."""
        try:
            heaters_info = await self._moonraker.fetch_available_heaters(timeout=5.0)
            return set(h.lower() for h in heaters_info.get("available_heaters", []))
        except Exception as exc:
            LOGGER.warning("Failed to fetch available heaters: %s", exc)
            # Fall back to common heater names
            return {"extruder", "heater_bed"}

    def _get_heater_max_temp(self, heater: str) -> float:
        """Get maximum safe temperature for a heater type.

        These are conservative defaults. In future, we could query
        Klipper's config to get the actual max_temp settings.
        """
        heater_lower = heater.lower()

        # Extruders typically max out at 250-300°C
        if heater_lower.startswith("extruder"):
            return 300.0

        # Heated beds typically max at 100-120°C
        if heater_lower == "heater_bed":
            return 120.0

        # Generic heaters - use a conservative limit
        if heater_lower.startswith("heater_generic"):
            return 150.0

        # Default conservative limit
        return 100.0
