"""Multi-toolhead control command handlers (Phase 3).

Implements two user-initiated, **idle-time only** commands:

- ``toolhead:activate-tool`` → ``ACTIVATE_EXTRUDER`` / ``T<n>`` (+ ``SET_DUAL_CARRIAGE``
  for IDEX), selecting the right G-code from the live machine archetype.
- ``idex:set-mode`` → ``SET_DUAL_CARRIAGE MODE=PRIMARY|COPY|MIRROR``.

Both are **collision-aware**: a tool change or carriage-mode change while a print is
running (or paused) risks a head crash, so the agent queries ``print_stats.state`` and
refuses the command unless the printer is confirmed idle. If the state cannot be read,
the command is rejected (fail-safe) rather than risking a blind physical move.

Security: the only values interpolated into G-code are an integer tool index and a
hard-coded mode enum (``PRIMARY``/``COPY``/``MIRROR``) — never raw cloud strings — so
there is no G-code injection surface here.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)

# Only explicitly idle / post-print states are safe for physical tool moves.
# Treating this as an allow-list is safer than maintaining a busy-state deny-list:
# it blocks cancelled/error/unknown states where Klipper may still be unwinding a
# print or the machine state may be stale.
_SAFE_IDLE_STATES = frozenset({"standby", "complete", "completed"})

# Klipper SET_DUAL_CARRIAGE MODE values (mainline). PRIMARY = independent full control.
_VALID_IDEX_MODES = frozenset({"PRIMARY", "COPY", "MIRROR"})

# Defensive upper bound on the tool index. No real machine approaches this; it stops a
# crafted payload from emitting an absurd identifier (e.g. ``extruder999999``).
_MAX_TOOL_INDEX = 25


class ToolheadCommandsMixin:
    """Mixin providing multi-toolhead control command handlers."""

    async def _execute_toolhead_activate_tool(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Activate a specific tool/extruder.

        Parameters:
            tool (int): Zero-based tool index (0 = ``extruder``, 1 = ``extruder1`` …).

        G-code (selected from the live machine archetype):
            - Toolchanger:   ``T<n>``               (macro handles dock/undock)
            - IDEX:          ``SET_DUAL_CARRIAGE CARRIAGE=<n> MODE=PRIMARY`` +
                             ``ACTIVATE_EXTRUDER EXTRUDER=extruder<n>``
            - Multi-extruder ``ACTIVATE_EXTRUDER EXTRUDER=extruder<n>``
        """
        params = message.parameters or {}

        tool = params.get("tool")
        if tool is None:
            raise CommandProcessingError(
                "tool parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        try:
            tool_index = int(tool)
        except (TypeError, ValueError):
            raise CommandProcessingError(
                "tool must be an integer index",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if tool_index < 0 or tool_index > _MAX_TOOL_INDEX:
            raise CommandProcessingError(
                f"tool index must be between 0 and {_MAX_TOOL_INDEX}",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # Collision-aware idle gate + capability probe in a single query.
        status = await self._assert_idle_for_toolchange(
            message,
            extra_objects={
                "dual_carriage": None,
                "toolchanger": None,
                "configfile": ["settings"],
            },
        )
        has_idex = "dual_carriage" in status
        has_toolchanger = "toolchanger" in status

        extruder_name = "extruder" if tool_index == 0 else f"extruder{tool_index}"

        if has_toolchanger:
            # klipper-toolchanger exposes T<n> macros that own the physical change.
            script = f"T{tool_index}"
        else:
            valid_heaters = await self._fetch_extruder_heaters()
            if extruder_name not in valid_heaters:
                raise CommandProcessingError(
                    f"Unknown tool {tool_index}: extruder '{extruder_name}' is not configured",
                    code="invalid_tool",
                    command_id=message.command_id,
                )
            if has_idex:
                if tool_index not in (0, 1):
                    raise CommandProcessingError(
                        "IDEX tool selection only supports tool indices 0 and 1",
                        code="invalid_tool",
                        command_id=message.command_id,
                    )
                primary_carriage, dual_carriage = self._resolve_idex_carriages(
                    status, command_id=message.command_id
                )
                carriage = primary_carriage if tool_index == 0 else dual_carriage
                script = (
                    f"SET_DUAL_CARRIAGE CARRIAGE={carriage} MODE=PRIMARY\n"
                    f"ACTIVATE_EXTRUDER EXTRUDER={extruder_name}"
                )
            else:
                script = f"ACTIVATE_EXTRUDER EXTRUDER={extruder_name}"

        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to activate tool: {exc}",
                code="gcode_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.info(
            "Activated tool %d (idex=%s, toolchanger=%s)",
            tool_index,
            has_idex,
            has_toolchanger,
        )

        return {"tool": tool_index}

    async def _execute_idex_set_mode(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Set the IDEX dual-carriage mode.

        Parameters:
            mode (str): One of ``PRIMARY``, ``COPY``, ``MIRROR`` (case-insensitive).

        G-code: ``SET_DUAL_CARRIAGE CARRIAGE=<0|1> MODE=<mode>``. ``PRIMARY`` targets
        carriage 0 (return to independent control); ``COPY``/``MIRROR`` target carriage 1.
        """
        params = message.parameters or {}

        mode = params.get("mode")
        if not mode or not isinstance(mode, str):
            raise CommandProcessingError(
                "mode parameter is required and must be a string",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        mode_normalized = mode.strip().upper()
        if mode_normalized not in _VALID_IDEX_MODES:
            raise CommandProcessingError(
                f"Invalid IDEX mode. Valid modes: {', '.join(sorted(_VALID_IDEX_MODES))}",
                code="invalid_mode",
                command_id=message.command_id,
            )

        status = await self._assert_idle_for_toolchange(
            message,
            extra_objects={"dual_carriage": None, "configfile": ["settings"]},
        )
        if "dual_carriage" not in status:
            raise CommandProcessingError(
                "Printer is not an IDEX machine (no dual_carriage configured)",
                code="not_idex",
                command_id=message.command_id,
            )

        primary_carriage, dual_carriage = self._resolve_idex_carriages(
            status, command_id=message.command_id
        )
        carriage = primary_carriage if mode_normalized == "PRIMARY" else dual_carriage
        script = f"SET_DUAL_CARRIAGE CARRIAGE={carriage} MODE={mode_normalized}"

        try:
            await self._moonraker.execute_gcode(script)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to set IDEX mode: {exc}",
                code="gcode_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.info("Set IDEX mode -> %s (carriage %s)", mode_normalized, carriage)

        return {"mode": mode_normalized, "carriage": self._format_carriage_result(carriage)}

    # -- Helpers ------------------------------------------------------------------

    async def _assert_idle_for_toolchange(
        self,
        message: CommandMessage,
        *,
        extra_objects: Optional[Mapping[str, Optional[list[str]]]] = None,
    ) -> Dict[str, Any]:
        """Return the queried printer status, raising unless the printer is idle.

        Collision-aware safety gate shared by both handlers: refuses the command when
        Klippy is not ready, the print is not in a known idle state, or the state
        cannot be read at all.
        """
        objects: Dict[str, Optional[list[str]]] = {
            "print_stats": ["state"],
            "webhooks": ["state"],
        }
        if extra_objects:
            objects.update(extra_objects)

        try:
            response = await self._moonraker.fetch_printer_state(
                objects=objects, timeout=5.0
            )
        except CommandProcessingError:
            raise
        except Exception as exc:
            # Fail-safe: never issue a physical tool move we cannot confirm is safe.
            raise CommandProcessingError(
                "Could not confirm the printer is idle; refusing the tool change for safety",
                code="state_unavailable",
                command_id=message.command_id,
            ) from exc

        status = (response or {}).get("result", {}).get("status", {})
        klippy_state = str((status.get("webhooks") or {}).get("state", "") or "").strip().lower()
        if not klippy_state:
            raise CommandProcessingError(
                "Could not confirm Klippy is ready; refusing the tool change for safety",
                code="state_unavailable",
                command_id=message.command_id,
            )
        if klippy_state != "ready":
            raise CommandProcessingError(
                f"Klippy is currently {klippy_state}; tool changes are only allowed when the printer is ready",
                code="printer_unavailable",
                command_id=message.command_id,
            )

        print_state = str((status.get("print_stats") or {}).get("state", "") or "").strip().lower()
        if not print_state:
            raise CommandProcessingError(
                "Could not confirm the printer is idle; refusing the tool change for safety",
                code="state_unavailable",
                command_id=message.command_id,
            )
        if print_state not in _SAFE_IDLE_STATES:
            raise CommandProcessingError(
                f"Printer is currently {print_state}; tool changes are only allowed when idle",
                code="printer_busy",
                command_id=message.command_id,
            )

        return status

    def _resolve_idex_carriages(
        self,
        status: Mapping[str, Any],
        *,
        command_id: str,
    ) -> tuple[str, str]:
        """Resolve the primary + dual carriage identifiers for SET_DUAL_CARRIAGE.

        Standard cartesian / hybrid_corexy(z) IDEX accepts numeric identifiers (0/1).
        generic_cartesian requires the configured carriage names (e.g. ``carriage_x`` /
        ``carriage_u``), which we resolve from the live ``configfile.settings`` snapshot.
        """
        dual_carriage = status.get("dual_carriage")
        if not isinstance(dual_carriage, Mapping):
            raise CommandProcessingError(
                "Could not resolve dual carriage state for the printer",
                code="state_unavailable",
                command_id=command_id,
            )

        if "carriage_0" in dual_carriage or "carriage_1" in dual_carriage:
            return ("0", "1")

        carriages = dual_carriage.get("carriages")
        if not isinstance(carriages, Mapping) or not carriages:
            raise CommandProcessingError(
                "Could not resolve IDEX carriage names for the printer",
                code="state_unavailable",
                command_id=command_id,
            )

        settings = ((status.get("configfile") or {}).get("settings", {}))
        if not isinstance(settings, Mapping):
            settings = {}

        primary_carriage, dual_carriage_name = self._resolve_generic_cartesian_carriages(
            carriages=carriages,
            settings=settings,
        )
        if primary_carriage and dual_carriage_name:
            return (primary_carriage, dual_carriage_name)

        raise CommandProcessingError(
            "Could not resolve IDEX carriage names from Moonraker config; refusing the tool change for safety",
            code="state_unavailable",
            command_id=command_id,
        )

    @staticmethod
    def _resolve_generic_cartesian_carriages(
        *,
        carriages: Mapping[str, Any],
        settings: Mapping[str, Any],
    ) -> tuple[Optional[str], Optional[str]]:
        """Resolve generic_cartesian primary + dual carriage names from configfile settings."""
        carriage_names = [name for name in carriages if isinstance(name, str) and name]
        if not carriage_names:
            return (None, None)

        primary_carriage: Optional[str] = None
        dual_carriage_name: Optional[str] = None

        for section_name, section_settings in settings.items():
            if not isinstance(section_name, str) or not section_name.startswith("dual_carriage "):
                continue
            if not isinstance(section_settings, Mapping):
                continue

            candidate_dual = section_name.split(" ", 1)[1]
            if candidate_dual not in carriages:
                continue

            dual_carriage_name = candidate_dual

            configured_primary = section_settings.get("primary_carriage")
            if isinstance(configured_primary, str) and configured_primary in carriages:
                primary_carriage = configured_primary
                break

            remaining = [name for name in carriage_names if name != dual_carriage_name]
            if len(remaining) == 1:
                primary_carriage = remaining[0]
                break

        if primary_carriage and not dual_carriage_name:
            remaining = [name for name in carriage_names if name != primary_carriage]
            dual_carriage_name = remaining[0] if len(remaining) == 1 else None
        elif dual_carriage_name and not primary_carriage:
            remaining = [name for name in carriage_names if name != dual_carriage_name]
            primary_carriage = remaining[0] if len(remaining) == 1 else None

        return (primary_carriage, dual_carriage_name)

    @staticmethod
    def _format_carriage_result(carriage: str) -> int | str:
        return int(carriage) if carriage.isdigit() else carriage

    async def _fetch_extruder_heaters(self) -> set[str]:
        """Return the set of configured extruder heater names (lower-cased)."""
        try:
            heaters_info = await self._moonraker.fetch_available_heaters(timeout=5.0)
        except Exception as exc:
            LOGGER.warning("Failed to fetch available heaters: %s", exc)
            return {"extruder"}
        return {
            h.lower()
            for h in heaters_info.get("available_heaters", [])
            if h.lower().startswith("extruder")
        }
