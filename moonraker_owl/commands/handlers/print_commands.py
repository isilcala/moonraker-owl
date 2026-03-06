"""Print control command handlers (emergency stop, firmware restart, reprint)."""

from __future__ import annotations

import logging
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)


class PrintCommandsMixin:
    """Mixin providing print control command handlers."""

    async def _execute_emergency_stop(self, message: CommandMessage) -> Dict[str, Any]:
        """Handle print:emergency-stop command.

        Immediately halts all printer operations via Moonraker's emergency_stop API.
        This is equivalent to sending M112 but uses the dedicated endpoint.
        """
        try:
            await self._moonraker.emergency_stop()
            LOGGER.warning("Emergency stop executed (command_id=%s)", message.command_id[:8])
        except Exception as exc:
            raise CommandProcessingError(
                f"Emergency stop failed: {exc}",
                code="moonraker_error",
                command_id=message.command_id,
            ) from exc
        return {"command": message.command}

    async def _execute_firmware_restart(self, message: CommandMessage) -> Dict[str, Any]:
        """Handle print:firmware-restart command.

        Restarts the Klipper firmware via Moonraker's firmware_restart API.
        This reloads the firmware configuration and resets the MCU.
        """
        try:
            await self._moonraker.firmware_restart()
            LOGGER.warning("Firmware restart executed (command_id=%s)", message.command_id[:8])
        except Exception as exc:
            raise CommandProcessingError(
                f"Firmware restart failed: {exc}",
                code="moonraker_error",
                command_id=message.command_id,
            ) from exc
        return {"command": message.command}

    async def _execute_reprint(self, message: CommandMessage) -> Dict[str, Any]:
        """Handle print:reprint command.

        Starts printing the specified GCode file. The filename is passed as a parameter.
        """
        params = message.parameters or {}
        filename = params.get("filename")

        if not filename or not str(filename).strip():
            raise CommandProcessingError(
                "Missing or empty 'filename' parameter",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        filename = str(filename).strip()

        try:
            await self._moonraker.start_print(filename)
            LOGGER.info("Reprint started for file '%s' (command_id=%s)", filename, message.command_id[:8])
        except ValueError as exc:
            raise CommandProcessingError(
                str(exc), code="invalid_parameters", command_id=message.command_id
            ) from exc
        except Exception as exc:
            raise CommandProcessingError(
                f"Start print failed: {exc}",
                code="moonraker_error",
                command_id=message.command_id,
            ) from exc

        return {"command": message.command, "filename": filename}
