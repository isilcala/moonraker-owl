"""Control and lifecycle command handlers (telemetry rate, job registration, object exclusion)."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from ..parsing import _parse_iso8601
from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)

# Regex pattern for valid object names (alphanumeric, underscores, hyphens, dots)
# Used to prevent G-code injection attacks
# Dots are allowed because slicers like PrusaSlicer/OrcaSlicer use them in object names
_OBJECT_NAME_PATTERN = re.compile(r"^[\w\-\.]+$")


class ControlCommandsMixin:
    """Mixin providing control, lifecycle, and object exclusion command handlers."""

    def _execute_job_registered(self, message: CommandMessage) -> Dict[str, Any]:
        """Handle job:registered command.

        This command is sent by the server after creating a PrintJob record.
        It provides the mapping between Moonraker job ID and cloud-side PrintJobId,
        enabling accurate correlation for timelapse and other events.

        Parameters:
            moonrakerJobId (str): Moonraker's job ID (e.g., "0002DD")
            printJobId (str): Cloud-side PrintJob UUID
            fileName (str): The gcode filename being printed
        """
        params = message.parameters or {}

        moonraker_job_id = params.get("moonrakerJobId")
        print_job_id = params.get("printJobId")
        filename = params.get("fileName")

        # All fields are required
        if not moonraker_job_id or not isinstance(moonraker_job_id, str):
            raise CommandProcessingError(
                "moonrakerJobId parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not print_job_id or not isinstance(print_job_id, str):
            raise CommandProcessingError(
                "printJobId parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not filename or not isinstance(filename, str):
            raise CommandProcessingError(
                "fileName parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # Register the mapping if registry is available
        if self._job_registry is not None:
            self._job_registry.register(
                moonraker_job_id=moonraker_job_id,
                print_job_id=print_job_id,
                filename=filename,
            )
            LOGGER.info(
                "Job registered: moonrakerJobId=%s -> printJobId=%s (file: %s)",
                moonraker_job_id,
                print_job_id[:8] if len(print_job_id) > 8 else print_job_id,
                filename,
            )
        else:
            LOGGER.warning(
                "Job registry unavailable, cannot store mapping for moonrakerJobId=%s",
                moonraker_job_id,
            )

        return {
            "success": True,
            "moonrakerJobId": moonraker_job_id,
            "printJobId": print_job_id,
        }

    def _execute_set_telemetry_rate(self, message: CommandMessage) -> Dict[str, Any]:
        """Handle control:set-telemetry-rate command."""
        if self._telemetry is None:
            raise CommandProcessingError(
                "Telemetry publisher unavailable",
                code="sensors_unavailable",
                command_id=message.command_id,
            )

        params = message.parameters or {}

        # Log current state before processing
        current_mode, current_interval, current_expires = (
            self._telemetry.get_current_sensors_state()
        )
        LOGGER.info(
            "[SetTelemetryRate] Current state: mode=%s, interval=%.2fs, expires=%s",
            current_mode,
            current_interval or 0.0,
            current_expires.isoformat() if current_expires else "none",
        )

        mode = str(params.get("mode", "idle")).strip().lower() or "idle"
        max_hz_value = params.get("maxHz", 0.0)
        try:
            max_hz = float(max_hz_value)
        except (TypeError, ValueError):
            raise CommandProcessingError(
                "maxHz must be numeric",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        requested_at = _parse_iso8601(params.get("issuedAt"))
        if requested_at is None:
            requested_at = _parse_iso8601(params.get("requestedAt"))

        # Clock skew correction: if server provides its current time, use it to
        # compute the intended expiration relative to server time, then adjust
        # to our local clock
        server_utc_now = _parse_iso8601(params.get("serverUtcNow"))
        local_utc_now = datetime.now(timezone.utc)
        clock_offset = timedelta(seconds=0)
        if server_utc_now is not None:
            clock_offset = local_utc_now - server_utc_now

        duration_value = params.get("durationSeconds")
        duration_seconds: Optional[int]
        if duration_value is None:
            duration_seconds = None
        else:
            try:
                duration_seconds = int(duration_value)
            except (TypeError, ValueError):
                raise CommandProcessingError(
                    "durationSeconds must be an integer",
                    code="invalid_parameters",
                    command_id=message.command_id,
                )
            if duration_seconds < 0:
                duration_seconds = None

        expires_override = _parse_iso8601(params.get("expiresAt"))
        expires_at: Optional[datetime] = None
        if expires_override is not None:
            baseline = requested_at or datetime.now(timezone.utc)
            computed_duration = int((expires_override - baseline).total_seconds())
            duration_seconds = max(0, computed_duration)

        # Apply clock offset to requested_at for accurate expiration calculation
        effective_requested_at = requested_at
        if effective_requested_at is not None and clock_offset != timedelta(seconds=0):
            effective_requested_at = effective_requested_at + clock_offset

        # Deduplication: check if this is a no-op or extend-only scenario
        current_mode, current_interval, current_expires = (
            self._telemetry.get_current_sensors_state()
        )
        target_interval = 1.0 / max_hz if max_hz > 0 else None

        # Allow small floating-point tolerance for interval comparison
        intervals_match = (
            current_interval is not None
            and target_interval is not None
            and abs(current_interval - target_interval) < 0.01
        ) or (current_interval is None and target_interval is None)

        is_extend_only = (
            mode == current_mode
            and intervals_match
            and mode == "watch"
            and duration_seconds is not None
            and duration_seconds > 0
        )

        LOGGER.info(
            "[SetTelemetryRate] Request: mode=%s, maxHz=%.2f, duration=%s, target_interval=%.2fs, "
            "intervals_match=%s, is_extend_only=%s",
            mode,
            max_hz,
            duration_seconds,
            target_interval or 0.0,
            intervals_match,
            is_extend_only,
        )

        if is_extend_only:
            # Same mode and interval - just extend the watch window
            expires_at = self._telemetry.extend_watch_window(
                duration_seconds=duration_seconds,
                requested_at=effective_requested_at,
            )
        else:
            # Full cadence reconfiguration needed
            expires_at = self._telemetry.apply_sensors_rate(
                mode=mode,
                max_hz=max_hz,
                duration_seconds=duration_seconds,
                requested_at=effective_requested_at,
            )

        effective_expires = expires_override or expires_at

        details: Dict[str, Any] = {
            "mode": mode,
            "maxHz": max(0.0, max_hz),
        }
        if duration_seconds is not None:
            details["durationSeconds"] = duration_seconds
        if requested_at is not None:
            details["requestedAtUtc"] = requested_at.replace(microsecond=0).isoformat()
        if effective_expires is not None:
            details["watchWindowExpires"] = effective_expires.replace(
                microsecond=0
            ).isoformat()

        return details

    async def _execute_object_exclude(self, message: CommandMessage) -> Dict[str, Any]:
        """Exclude an object from the current print (ADR-0016).

        Parameters:
            objectName (str): Name of the object to exclude

        GCode: EXCLUDE_OBJECT NAME=<object_name>

        Returns:
            Dict with excluded object name on success.

        Raises:
            CommandProcessingError: If objectName is missing, invalid, or execution fails.
        """
        params = message.parameters or {}
        LOGGER.debug("[ExcludeObject] Received parameters: %s", params)

        object_name = params.get("objectName")
        if not object_name or not isinstance(object_name, str):
            LOGGER.warning("[ExcludeObject] Missing objectName parameter. Received: %s", params)
            raise CommandProcessingError(
                "objectName parameter is required",
                code="missing_parameter",
                command_id=message.command_id,
            )

        object_name = object_name.strip()
        if not object_name:
            raise CommandProcessingError(
                "objectName cannot be empty",
                code="missing_parameter",
                command_id=message.command_id,
            )

        # Validate object name to prevent G-code injection
        # Object names should only contain alphanumeric characters, underscores, hyphens
        if not _OBJECT_NAME_PATTERN.match(object_name):
            raise CommandProcessingError(
                f"Invalid object name: {object_name}. "
                "Object names must contain only letters, numbers, underscores, and hyphens.",
                code="invalid_object_name",
                command_id=message.command_id,
            )

        # Execute the EXCLUDE_OBJECT G-code command using fire-and-forget.
        # State changes are tracked via Moonraker WebSocket notifications,
        # not command execution result. This allows:
        # 1. No timeout issues for slow GCode execution
        # 2. UI updates from any source (Mainsail, Fluidd, our UI)
        # 3. ACK means "command sent" not "command completed"
        script = f"EXCLUDE_OBJECT NAME={object_name}"

        try:
            await self._moonraker.execute_gcode(script, fire_and_forget=True)
        except Exception as exc:
            error_message = str(exc).lower()
            # Only catch send failures, not execution failures
            if "unknown" in error_message or "not found" in error_message:
                raise CommandProcessingError(
                    f"Object '{object_name}' not found in current print",
                    code="object_not_found",
                    command_id=message.command_id,
                ) from exc
            raise CommandProcessingError(
                f"Failed to send exclude command: {exc}",
                code="send_error",
                command_id=message.command_id,
            ) from exc

        LOGGER.info("Excluded object: %s", object_name)

        return {
            "objectName": object_name,
        }
