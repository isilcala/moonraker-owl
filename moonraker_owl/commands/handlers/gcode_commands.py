"""GCode macro execution command handlers."""

from __future__ import annotations

import logging
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError
from ._validation import validate_klipper_identifier

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

        # Defense against G-code injection: macros are sent verbatim to
        # `execute_gcode`, so any whitespace / newline / metacharacter would
        # let a compromised cloud queue follow-up commands. Reject anything
        # outside the Klipper identifier alphabet.
        macro_name = validate_klipper_identifier(
            macro_name, field="macro", command_id=message.command_id
        )

        # Validate macro exists in Klipper's registered objects.
        # Fail-closed: if the pre-flight query times out or errors, we refuse
        # the command. The previous behaviour ("proceed anyway") was a
        # defense-in-depth gap against a compromised cloud bus — it allowed
        # an attacker to dispatch arbitrary macro names whenever the
        # Moonraker query layer was degraded. (audit A-01, Q-5)
        try:
            result = await self._moonraker.fetch_printer_state(
                objects={f"gcode_macro {macro_name}": None}, timeout=3.0
            )
        except CommandProcessingError:
            raise
        except Exception as exc:
            LOGGER.warning(
                "Macro pre-flight check failed for %s; refusing command: %s",
                macro_name,
                exc,
            )
            raise CommandProcessingError(
                "Unable to verify macro registration; refusing for safety",
                code="macro_preflight_failed",
                command_id=message.command_id,
            ) from exc

        status = result.get("result", {}).get("status", {})
        if f"gcode_macro {macro_name}" not in status:
            raise CommandProcessingError(
                f"Macro '{macro_name}' is not registered in Klipper",
                code="unknown_macro",
                command_id=message.command_id,
            )

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
