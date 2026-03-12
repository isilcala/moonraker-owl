"""Print control command handlers (emergency stop, firmware restart, start print)."""

from __future__ import annotations

import logging
import posixpath
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)

# print_stats states where starting a new print is not allowed
_BUSY_STATES = frozenset({"printing", "paused"})


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

    async def _handle_print_start(self, message: CommandMessage) -> Dict[str, Any]:
        """Handle print:start command.

        Starts printing the specified GCode file after validating printer state
        and filename safety.
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

        # Filename safety: reject path traversal and absolute paths
        if ".." in filename or posixpath.isabs(filename):
            raise CommandProcessingError(
                "Invalid filename: path traversal or absolute paths are not allowed",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # State validation: reject if printer is currently printing or paused
        try:
            state_resp = await self._moonraker.fetch_printer_state(
                objects={"print_stats": ["state"]}, timeout=5.0
            )
            status = state_resp.get("result", {}).get("status", {})
            print_state = (status.get("print_stats") or {}).get("state", "")
            if print_state in _BUSY_STATES:
                raise CommandProcessingError(
                    f"Printer is currently {print_state}, cannot start a new print",
                    code="printer_busy",
                    command_id=message.command_id,
                )
        except CommandProcessingError:
            raise
        except Exception as exc:
            LOGGER.warning(
                "Could not check printer state before print:start (command_id=%s): %s",
                message.command_id[:8], exc,
            )
            # Proceed anyway — Moonraker will reject if truly busy

        try:
            await self._moonraker.start_print(filename)
            LOGGER.info("Print started for file '%s' (command_id=%s)", filename, message.command_id[:8])
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
