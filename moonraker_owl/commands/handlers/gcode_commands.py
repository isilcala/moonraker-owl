"""GCode macro execution command handlers."""

from __future__ import annotations

import logging
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)


class GCodeCommandsMixin:
    """Mixin providing GCode macro execution command handlers."""

    async def _execute_gcode_macro(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Execute a user-defined GCode macro by name.

        Parameters:
            macro (str): The macro name to execute (e.g. 'LOAD_FILAMENT').
        """
        params = message.parameters or {}

        macro = params.get("macro")
        if not macro or not isinstance(macro, str):
            raise CommandProcessingError(
                "macro parameter is required and must be a string",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # Sanitize: macro names are uppercase GCode commands
        macro_name = macro.strip().upper()

        # Reject internal macros (prefixed with _)
        if macro_name.startswith("_"):
            raise CommandProcessingError(
                "Internal macros cannot be executed directly",
                code="forbidden_macro",
                command_id=message.command_id,
            )

        # Validate macro exists in Klipper's registered objects
        try:
            result = await self._moonraker.fetch_printer_state(
                objects={f"gcode_macro {macro_name}": None}, timeout=3.0
            )
            status = result.get("result", {}).get("status", {})
            if f"gcode_macro {macro_name}" not in status:
                raise CommandProcessingError(
                    f"Macro '{macro_name}' is not registered in Klipper",
                    code="unknown_macro",
                    command_id=message.command_id,
                )
        except CommandProcessingError:
            raise
        except Exception:
            # If pre-flight check fails (e.g. timeout), proceed anyway —
            # execute_gcode will catch real errors
            LOGGER.debug("Macro existence check failed, proceeding: %s", macro_name)

        try:
            await self._moonraker.execute_gcode(macro_name)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to execute macro {macro_name}: {exc}",
                code="gcode_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.debug("Executed GCode macro: %s", macro_name)

        return {"macro": macro_name}
