"""Command handling pipeline for Moonraker Owl."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, Optional, Protocol
from urllib.parse import quote

from .printer_command_names import PrinterCommandNames
from .config import OwlConfig
from .telemetry import TelemetryPublisher
from .adapters.s3_upload import S3UploadClient, UploadResult
from .adapters.camera import CameraClient
from .adapters.image_preprocessor import ImagePreprocessor
from .core.idempotency import CommandIdempotencyGuard
from .core.job_registry import PrintJobRegistry

LOGGER = logging.getLogger(__name__)


class CommandConfigurationError(RuntimeError):
    """Raised when the command processor cannot be configured."""


class CommandProcessingError(RuntimeError):
    """Raised when an individual command cannot be processed."""

    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        command_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.command_id = command_id


class MoonrakerCommandClient(Protocol):
    """Protocol for Moonraker command execution.

    This protocol defines the minimal interface required by CommandProcessor
    to execute commands on the printer via Moonraker.
    """

    async def execute_print_action(self, action: str) -> None:
        """Execute a high-level print control action (pause, resume, cancel)."""
        ...

    async def execute_gcode(self, script: str) -> None:
        """Execute an arbitrary GCode script on the printer."""
        ...

    async def emergency_stop(self) -> None:
        """Execute emergency stop (M112) on the printer."""
        ...

    async def start_print(self, filename: str) -> None:
        """Start printing the specified GCode file.

        Args:
            filename: The GCode filename to print.
        """
        ...

    async def fetch_available_heaters(
        self, timeout: float = 5.0
    ) -> dict[str, list[str]]:
        """Discover all available heaters and temperature sensors.

        Returns:
            Dictionary with 'available_heaters' and 'available_sensors' lists.
        """
        ...

    async def fetch_thumbnail(
        self,
        relative_path: str,
        gcode_filename: Optional[str] = None,
        timeout: float = 30.0,
    ) -> Optional[bytes]:
        """Fetch thumbnail image data from Moonraker file server.

        Args:
            relative_path: Relative path to the thumbnail.
            gcode_filename: The gcode filename, used to extract subdirectory prefix.
            timeout: Request timeout in seconds.

        Returns:
            Raw image bytes if successful, None if not found.
        """
        ...

    async def fetch_gcode_metadata(
        self, filename: str, timeout: float = 10.0
    ) -> Optional[dict]:
        """Fetch GCode file metadata including thumbnail paths.

        Args:
            filename: The GCode filename.
            timeout: Request timeout in seconds.

        Returns:
            Metadata dictionary or None if not found.
        """
        ...

    async def fetch_timelapse_file(
        self, filename: str, timeout: float = 120.0
    ) -> Optional[bytes]:
        """Fetch timelapse video or preview from Moonraker file server.

        Args:
            filename: Timelapse filename (e.g., "timelapse_xxx.mp4")
            timeout: Request timeout in seconds.

        Returns:
            Raw file bytes if successful, None if not found.
        """
        ...


class MQTTCommandsClient(Protocol):
    def subscribe(self, topic: str, qos: int = 1) -> None: ...

    def unsubscribe(self, topic: str) -> None: ...

    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 1,
        retain: bool = False,
        *,
        properties=None,
    ) -> None: ...

    def set_message_handler(self, handler): ...


@dataclass(slots=True)
class CommandMessage:
    command_id: str
    command: str
    parameters: Dict[str, Any]


@dataclass(slots=True)
class _InflightCommand:
    command_name: str
    message: CommandMessage
    dispatched_at: datetime


@dataclass(slots=True)
class _CommandHistoryEntry:
    status: str
    stage: str
    error_code: Optional[str]
    error_message: Optional[str]


@dataclass(slots=True)
class _PendingStateCommand:
    """Tracks a command awaiting state confirmation.

    Some commands (pause, resume, cancel) need to wait for the printer
    to actually reach the expected state before sending 'completed' ACK.
    This allows the UI to show accurate command status.
    """

    command_id: str
    command_name: str
    message: CommandMessage
    expected_state: str
    accepted_at: datetime
    timeout_seconds: float = 30.0

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        """Check if this pending command has timed out."""
        current = now or datetime.now(timezone.utc)
        deadline = self.accepted_at + timedelta(seconds=self.timeout_seconds)
        return current >= deadline

    @property
    def remaining_seconds(self) -> float:
        """Get remaining time before timeout."""
        now = datetime.now(timezone.utc)
        deadline = self.accepted_at + timedelta(seconds=self.timeout_seconds)
        remaining = (deadline - now).total_seconds()
        return max(0.0, remaining)


# Commands that require state confirmation before sending 'completed' ACK
# Maps command name to expected print_stats.state value
COMMAND_EXPECTED_STATES: Dict[str, str] = {
    PrinterCommandNames.PAUSE: "paused",
    PrinterCommandNames.RESUME: "printing",
    PrinterCommandNames.CANCEL: "cancelled",
}


class CommandProcessor:
    """Consumes MQTT command messages and forwards them to Moonraker."""

    def __init__(
        self,
        config: OwlConfig,
        moonraker: MoonrakerCommandClient,
        mqtt: MQTTCommandsClient,
        telemetry: Optional[TelemetryPublisher] = None,
        s3_upload: Optional[S3UploadClient] = None,
        camera: Optional[CameraClient] = None,
        image_preprocessor: Optional[ImagePreprocessor] = None,
        job_registry: Optional[PrintJobRegistry] = None,
    ) -> None:
        self._config = config
        self._moonraker = moonraker
        self._mqtt = mqtt
        self._telemetry = telemetry
        self._s3_upload = s3_upload
        self._camera = camera
        self._image_preprocessor = image_preprocessor
        self._job_registry = job_registry

        (
            self._tenant_id,
            self._device_id,
            self._printer_id,
        ) = _resolve_identity(config)

        self._command_topic_prefix = f"owl/printers/{self._device_id}/commands"
        self._command_subscription = f"{self._command_topic_prefix}/#"
        self._handler_registered = False
        self._inflight: Dict[str, _InflightCommand] = {}

        # Idempotency guard for duplicate command detection (ADR-0013 Appendix D)
        # Uses TTL-based expiration instead of fixed-size history
        self._idempotency = CommandIdempotencyGuard(
            ttl_hours=24,
            max_entries=10000,
            cleanup_interval=100,
        )

        # Legacy history tracking (kept for backward compatibility with tests)
        # Will be removed once tests are updated to use idempotency guard
        self._history: Dict[str, _CommandHistoryEntry] = {}
        self._history_order: Deque[str] = deque()
        self._history_limit = 64

        # State-based command completion tracking
        self._pending_state_commands: Dict[str, _PendingStateCommand] = {}
        self._command_timeout_seconds = 30.0
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def start(self) -> None:
        if self._handler_registered:
            raise RuntimeError("CommandProcessor already started")

        self._loop = asyncio.get_running_loop()
        self._mqtt.set_message_handler(self._handle_message)
        self._mqtt.subscribe(self._command_subscription, qos=1)
        self._handler_registered = True
        LOGGER.info("Command processor subscribed to %s", self._command_subscription)

    async def stop(self) -> None:
        if not self._handler_registered:
            return

        try:
            self._mqtt.unsubscribe(self._command_subscription)
        except Exception:  # pragma: no cover - defensive cleanup
            pass  # Cleanup - ignore unsubscribe errors
        finally:
            self._mqtt.set_message_handler(None)
            self._handler_registered = False

    async def _handle_message(self, topic: str, payload: bytes) -> None:
        command_name = _extract_command_name(topic, self._device_id)
        if not command_name:
            return

        LOGGER.info(
            "Received command on topic %s: command=%s, payload_size=%d",
            topic, command_name, len(payload)
        )

        try:
            message = _parse_command(payload, command_name)
        except CommandProcessingError as exc:
            LOGGER.warning("Invalid command payload: %s", exc)
            await self._publish_ack(
                command_name,
                exc.command_id,
                "failed",
                stage="dispatch",
                error_code=exc.code or "invalid_payload",
                error_message=str(exc),
            )
            if exc.command_id:
                placeholder = CommandMessage(exc.command_id, command_name, {})
                self._record_command_state(
                    placeholder,
                    "rejected",
                    details={"code": exc.code or "invalid_payload"},
                )
            return

        if await self._replay_duplicate(command_name, message.command_id):
            return

        if message.command_id in self._inflight:
            # Duplicate delivery while still processing - ignore
            return

        self._begin_inflight(command_name, message)
        self._record_command_state(message, "dispatched")

        try:
            await self._publish_ack(
                command_name,
                message.command_id,
                "accepted",
                stage="dispatch",
            )
            self._record_command_state(message, "accepted")

            details = await self._execute(message)
        except CommandProcessingError as exc:
            await self._publish_ack(
                command_name,
                message.command_id,
                "failed",
                stage="execution",
                error_code=exc.code or "command_failed",
                error_message=str(exc),
            )
            failure_details: Dict[str, Any] = {}
            if exc.code:
                failure_details["code"] = exc.code
            failure_details["message"] = str(exc)
            self._record_command_state(
                message,
                "failed",
                details=failure_details or None,
            )
            self._finish_inflight(message.command_id)
        else:
            # Check if this command requires state confirmation
            expected_state = COMMAND_EXPECTED_STATES.get(message.command)
            if expected_state:
                # Don't send 'completed' yet - wait for state change
                self._begin_pending_state(command_name, message, expected_state)
                LOGGER.debug(
                    "Command %s awaiting state '%s'",
                    message.command_id[:8],
                    expected_state,
                )
            else:
                # No state confirmation needed - complete immediately
                await self._publish_ack(
                    command_name,
                    message.command_id,
                    "completed",
                    stage="execution",
                    result=details,
                )
                self._record_command_state(message, "completed", details=details)
                self._finish_inflight(message.command_id)

    def _record_command_state(
        self,
        message: CommandMessage,
        state: str,
        *,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self._telemetry is None:
            return

        try:
            self._telemetry.record_command_state(
                command_id=message.command_id,
                command_type=message.command,
                state=state,
                details=details,
            )
        except Exception:  # pragma: no cover - defensive
            pass  # Non-critical - telemetry will recover

    def _begin_inflight(self, command_name: str, message: CommandMessage) -> None:
        self._inflight[message.command_id] = _InflightCommand(
            command_name=command_name,
            message=message,
            dispatched_at=datetime.now(timezone.utc),
        )

    def _finish_inflight(self, command_id: str) -> None:
        self._inflight.pop(command_id, None)

    def _begin_pending_state(
        self, command_name: str, message: CommandMessage, expected_state: str
    ) -> None:
        """Begin tracking a command that awaits state confirmation."""
        pending = _PendingStateCommand(
            command_id=message.command_id,
            command_name=command_name,
            message=message,
            expected_state=expected_state,
            accepted_at=datetime.now(timezone.utc),
            timeout_seconds=self._command_timeout_seconds,
        )
        self._pending_state_commands[message.command_id] = pending

    def on_print_state_changed(self, new_state: str) -> None:
        """Called by telemetry when print state changes.

        Checks if any pending commands are now completed because the
        printer has reached their expected state.

        This method is called synchronously from TelemetryOrchestrator.ingest(),
        so we schedule async ACK publishing on the event loop.

        Args:
            new_state: The new print state (e.g., 'paused', 'printing', 'cancelled')
        """
        if not self._pending_state_commands:
            return

        normalized_state = new_state.lower() if new_state else ""
        completed_ids: list[str] = []

        for cmd_id, pending in self._pending_state_commands.items():
            if pending.expected_state == normalized_state:
                completed_ids.append(cmd_id)
                LOGGER.debug(
                    "Command %s: state -> '%s'",
                    cmd_id[:8],
                    new_state,
                )

        for cmd_id in completed_ids:
            pending = self._pending_state_commands.pop(cmd_id)
            self._schedule_completion_ack(pending)

    def _schedule_completion_ack(self, pending: _PendingStateCommand) -> None:
        """Schedule async ACK publishing on the event loop."""
        self._schedule_ack(
            pending,
            status="completed",
            error_code=None,
            error_message=None,
            state="completed",
            details=None,
        )

    def check_expired_commands(self) -> None:
        """Check for and fail expired pending commands.

        Should be called periodically (e.g., once per telemetry cycle)
        to detect commands that timed out waiting for state change.
        """
        if not self._pending_state_commands:
            return

        now = datetime.now(timezone.utc)
        expired_ids: list[str] = []

        for cmd_id, pending in self._pending_state_commands.items():
            if pending.is_expired(now):
                expired_ids.append(cmd_id)
                LOGGER.warning(
                    "Command %s timed out waiting for state '%s'",
                    cmd_id[:8],
                    pending.expected_state,
                )

        for cmd_id in expired_ids:
            pending = self._pending_state_commands.pop(cmd_id)
            self._schedule_timeout_ack(pending)

    def _schedule_timeout_ack(self, pending: _PendingStateCommand) -> None:
        """Schedule async timeout ACK publishing on the event loop."""
        self._schedule_ack(
            pending,
            status="failed",
            error_code="state_timeout",
            error_message=f"Timeout waiting for state '{pending.expected_state}'",
            state="timeout",
            details={
                "expectedState": pending.expected_state,
                "timeoutSeconds": pending.timeout_seconds,
            },
        )

    def _schedule_ack(
        self,
        pending: _PendingStateCommand,
        *,
        status: str,
        error_code: Optional[str],
        error_message: Optional[str],
        state: str,
        details: Optional[Dict[str, Any]],
    ) -> None:
        """Schedule async ACK publishing for a pending command."""
        if self._loop is None:
            LOGGER.warning(
                "Cannot send %s ACK for %s: no event loop",
                status,
                pending.command_id[:8],
            )
            return

        async def send_ack() -> None:
            try:
                await self._publish_ack(
                    pending.command_name,
                    pending.command_id,
                    status,
                    stage="execution",
                    error_code=error_code,
                    error_message=error_message,
                )
                self._record_command_state(pending.message, state, details=details)
                self._finish_inflight(pending.command_id)
            except Exception:
                LOGGER.exception(
                    "Error sending %s ACK for %s", status, pending.command_id[:8]
                )

        self._loop.call_soon(
            lambda: asyncio.ensure_future(send_ack(), loop=self._loop)
        )

    @property
    def pending_state_count(self) -> int:
        """Get number of commands awaiting state confirmation."""
        return len(self._pending_state_commands)

    def _remember_history(
        self,
        command_id: Optional[str],
        *,
        status: str,
        stage: str,
        error_code: Optional[str],
        error_message: Optional[str],
    ) -> None:
        if not command_id:
            return
        self._history[command_id] = _CommandHistoryEntry(
            status=status,
            stage=stage,
            error_code=error_code,
            error_message=error_message,
        )
        self._history_order.append(command_id)
        while len(self._history_order) > self._history_limit:
            expired = self._history_order.popleft()
            self._history.pop(expired, None)

        # Also record in idempotency guard for TTL-based duplicate detection
        self._idempotency.mark_processed(
            command_id,
            status=status,
            stage=stage,
            error_code=error_code,
            error_message=error_message,
        )

    async def _replay_duplicate(self, command_name: str, command_id: str) -> bool:
        """Check if command is a duplicate and replay cached ACK if so.

        Uses the idempotency guard (TTL-based) as primary check,
        falls back to legacy history for backward compatibility.

        Per ADR-0013 Appendix D, duplicate ACKs include 'skipped: true'.
        """
        # Primary: Check idempotency guard (TTL-based)
        cached = self._idempotency.get_cached_result(command_id)
        if cached is not None:
            LOGGER.info(
                "Duplicate command %s detected via idempotency guard (processed at %s)",
                command_id,
                cached.processed_at.isoformat(),
            )
            await self._publish_ack(
                command_name,
                command_id,
                cached.status,
                stage=cached.stage,
                error_code=cached.error_code,
                error_message=cached.error_message,
                skipped=True,  # ADR-0013 Appendix D: indicate duplicate detection
            )
            return True

        # Fallback: Check legacy history (for commands processed before upgrade)
        entry = self._history.get(command_id)
        if entry is not None:
            LOGGER.debug("Replaying duplicate command %s with cached status (legacy)", command_id)
            await self._publish_ack(
                command_name,
                command_id,
                entry.status,
                stage=entry.stage,
                error_code=entry.error_code,
                error_message=entry.error_message,
                skipped=True,
            )
            return True

        return False

    @property
    def pending_count(self) -> int:
        return len(self._inflight)

    async def abandon_inflight(self, reason: str) -> None:
        """Abandon all inflight and pending state commands.

        Called during agent shutdown/restart to notify cloud that
        commands will not be completed.
        """
        # Abandon regular inflight commands
        if self._inflight:
            items = list(self._inflight.values())
            self._inflight.clear()

            for tracked in items:
                await self._publish_ack(
                    tracked.command_name,
                    tracked.message.command_id,
                    "failed",
                    stage="execution",
                    error_code="agent_restart",
                    error_message=reason,
                )
                self._record_command_state(
                    tracked.message,
                    "abandoned",
                    details={
                        "reason": "agent_restart",
                        "detail": reason,
                    },
                )

        # Abandon pending state commands
        if self._pending_state_commands:
            pending_items = list(self._pending_state_commands.values())
            self._pending_state_commands.clear()

            for pending in pending_items:
                await self._publish_ack(
                    pending.command_name,
                    pending.command_id,
                    "failed",
                    stage="execution",
                    error_code="agent_restart",
                    error_message=reason,
                )
                self._record_command_state(
                    pending.message,
                    "abandoned",
                    details={
                        "reason": "agent_restart",
                        "detail": reason,
                        "expectedState": pending.expected_state,
                    },
                )

    async def _execute(self, message: CommandMessage) -> Optional[Dict[str, Any]]:
        """Execute a command and return result details.

        Commands are dispatched based on their name (ADR-0013 naming):
        - control:set-telemetry-rate: Configure telemetry cadence
        - heater:set-target: Set heater target temperature
        - heater:turn-off: Turn off a specific heater
        - fan:set-speed: Set fan speed
        - print:pause/resume/cancel: Print control actions
        - print:emergency-stop: Emergency stop
        - print:reprint: Reprint the last print job
        - sync:job-thumbnail: Set thumbnail URL for current print job
        - task:upload-thumbnail: Upload thumbnail to presigned URL
        - task:capture-image: Capture and upload camera frame
        - object:exclude: Exclude an object from the current print (ADR-0016)
        """
        LOGGER.debug("[CommandDispatch] Executing command: %s (id=%s)", message.command, message.command_id[:8])
        
        # System control commands
        if message.command == PrinterCommandNames.SET_TELEMETRY_RATE:
            return self._execute_set_telemetry_rate(message)

        # Heater control commands
        if message.command == PrinterCommandNames.HEATER_SET_TARGET:
            return await self._execute_heater_set_target(message)
        if message.command == PrinterCommandNames.HEATER_TURN_OFF:
            return await self._execute_heater_turn_off(message)

        # Fan control commands
        if message.command == PrinterCommandNames.FAN_SET_SPEED:
            return await self._execute_fan_set_speed(message)

        # Object control commands (ADR-0016)
        if message.command == PrinterCommandNames.OBJECT_EXCLUDE:
            return await self._execute_object_exclude(message)

        # Note: sync:job-thumbnail command has been removed.
        # Thumbnail URLs are now pushed via SignalR after ACK processing.

        # System task commands
        if message.command == PrinterCommandNames.UPLOAD_THUMBNAIL:
            return await self._execute_upload_thumbnail(message)

        if message.command == PrinterCommandNames.CAPTURE_IMAGE:
            return await self._execute_capture_image(message)

        if message.command == PrinterCommandNames.UPLOAD_TIMELAPSE:
            return await self._execute_upload_timelapse(message)

        # Job lifecycle commands
        if message.command == PrinterCommandNames.JOB_REGISTERED:
            return self._execute_job_registered(message)

        # Emergency stop command
        if message.command == PrinterCommandNames.EMERGENCY_STOP:
            return await self._execute_emergency_stop(message)

        # Reprint command
        if message.command == PrinterCommandNames.REPRINT:
            return await self._execute_reprint(message)

        # Print control commands (pause, resume, cancel)
        # Map command name to Moonraker action (print:pause -> pause)
        command_to_action = {
            PrinterCommandNames.PAUSE: "pause",
            PrinterCommandNames.RESUME: "resume",
            PrinterCommandNames.CANCEL: "cancel",
        }
        if message.command in command_to_action:
            moonraker_action = command_to_action[message.command]
            try:
                await self._moonraker.execute_print_action(moonraker_action)
            except ValueError as exc:
                raise CommandProcessingError(
                    str(exc), code="unsupported_command", command_id=message.command_id
                ) from exc
            except Exception as exc:  # pragma: no cover - networking errors
                raise CommandProcessingError(
                    f"Moonraker command failed: {exc}",
                    code="moonraker_error",
                    command_id=message.command_id,
                ) from exc
            return {"command": message.command}

        # Unknown command
        raise CommandProcessingError(
            f"Unknown command: {message.command}",
            code="unsupported_command",
            command_id=message.command_id,
        )

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
            details["watchWindowExpiresUtc"] = effective_expires.replace(
                microsecond=0
            ).isoformat()

        return details

    # -------------------------------------------------------------------------
    # System Task Commands
    # -------------------------------------------------------------------------

    async def _execute_upload_thumbnail(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Handle task:upload-thumbnail command.

        This command is sent by the server to request thumbnail upload.
        The agent fetches the thumbnail from Moonraker and uploads it
        to the presigned S3 URL.

        Parameters:
            jobId (str): The print job ID
            uploadUrl (str): Presigned URL for uploading
            thumbnailKey (str): S3 key for the thumbnail
            contentType (str): Expected content type (e.g., "image/png")
            maxSizeBytes (int, optional): Maximum file size allowed
            expiresAt (str, optional): When the presigned URL expires
        """
        if self._s3_upload is None:
            raise CommandProcessingError(
                "S3 upload client unavailable",
                code="upload_unavailable",
                command_id=message.command_id,
            )

        if self._telemetry is None:
            raise CommandProcessingError(
                "Telemetry publisher unavailable",
                code="telemetry_unavailable",
                command_id=message.command_id,
            )

        params = message.parameters or {}

        job_id = params.get("jobId")
        upload_url = params.get("uploadUrl")
        thumbnail_key = params.get("thumbnailKey")
        content_type = params.get("contentType", "image/png")
        max_size_bytes = params.get("maxSizeBytes", 5 * 1024 * 1024)  # 5 MB default

        if not upload_url or not isinstance(upload_url, str):
            raise CommandProcessingError(
                "uploadUrl parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not thumbnail_key or not isinstance(thumbnail_key, str):
            raise CommandProcessingError(
                "thumbnailKey parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # Get current job thumbnail info from telemetry state
        thumbnail_info = self._telemetry.get_current_thumbnail_info()
        if thumbnail_info is None:
            LOGGER.warning(
                "No thumbnail available for job %s",
                job_id[:8] if job_id else "unknown",
            )
            return {
                "success": False,
                "jobId": job_id,
                "error": "no_thumbnail_available",
                "errorMessage": "No thumbnail path available from current print job",
            }

        relative_path = thumbnail_info.get("relative_path")
        gcode_filename = thumbnail_info.get("gcode_filename")

        if not relative_path:
            return {
                "success": False,
                "jobId": job_id,
                "error": "no_thumbnail_path",
                "errorMessage": "No thumbnail relative path available",
            }

        # Fetch thumbnail from Moonraker
        try:
            thumbnail_data = await self._moonraker.fetch_thumbnail(
                relative_path, gcode_filename=gcode_filename, timeout=30.0
            )
        except Exception as exc:
            LOGGER.error("Failed to fetch thumbnail: %s", exc)
            return {
                "success": False,
                "jobId": job_id,
                "error": "fetch_failed",
                "errorMessage": str(exc),
            }

        if thumbnail_data is None:
            return {
                "success": False,
                "jobId": job_id,
                "error": "thumbnail_not_found",
                "errorMessage": f"Thumbnail not found at {relative_path}",
            }

        # Validate size
        if len(thumbnail_data) > max_size_bytes:
            return {
                "success": False,
                "jobId": job_id,
                "error": "thumbnail_too_large",
                "errorMessage": f"Thumbnail size {len(thumbnail_data)} exceeds max {max_size_bytes}",
            }

        # Upload to S3
        result = await self._s3_upload.upload(
            presigned_url=upload_url,
            data=thumbnail_data,
            s3_key=thumbnail_key,
            content_type=content_type,
        )

        if result.success:
            LOGGER.info(
                "Uploaded thumbnail for job %s: %d bytes to %s",
                job_id[:8] if job_id else "unknown",
                result.file_size_bytes,
                thumbnail_key,
            )

            # Note: We no longer set thumbnailUrl in telemetry status.
            # The server will push the URL via SignalR after processing the ACK.

            return {
                "success": True,
                "jobId": job_id,
                "thumbnailKey": thumbnail_key,
                "sizeBytes": result.file_size_bytes,
            }
        else:
            LOGGER.warning(
                "Failed to upload thumbnail for job %s: %s",
                job_id[:8] if job_id else "unknown",
                result.error_message,
            )
            return {
                "success": False,
                "jobId": job_id,
                "error": result.error_code or "upload_failed",
                "errorMessage": result.error_message,
            }

    async def _execute_capture_image(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Handle task:capture-image command.

        This command is sent by the server to request camera capture.
        The agent captures a snapshot from the webcam and uploads it
        to the presigned S3 URL.

        Parameters:
            frameId (str): The capture frame ID for correlation
            uploadUrl (str): Presigned URL for uploading
            blobKey (str): S3 key for the captured image
            maxFileSizeBytes (int, optional): Maximum file size allowed
            allowedContentTypes (list, optional): Allowed MIME types
        """
        if self._s3_upload is None:
            raise CommandProcessingError(
                "S3 upload client unavailable",
                code="upload_unavailable",
                command_id=message.command_id,
            )

        if self._camera is None:
            return {
                "success": False,
                "frameId": message.parameters.get("frameId") if message.parameters else None,
                "errorCode": "camera_unavailable",
                "errorMessage": "Camera capture is not configured or disabled",
            }

        params = message.parameters or {}

        frame_id = params.get("frameId")
        upload_url = params.get("uploadUrl")
        blob_key = params.get("blobKey")
        max_size_bytes = params.get("maxFileSizeBytes", 5 * 1024 * 1024)  # 5 MB default

        if not frame_id:
            raise CommandProcessingError(
                "frameId parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not upload_url or not isinstance(upload_url, str):
            raise CommandProcessingError(
                "uploadUrl parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not blob_key or not isinstance(blob_key, str):
            raise CommandProcessingError(
                "blobKey parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # Capture image from webcam
        from datetime import timezone
        from datetime import datetime as dt
        capture_result = await self._camera.capture()

        if not capture_result.success:
            LOGGER.warning(
                "Camera capture failed for frame %s: %s",
                frame_id[:8] if isinstance(frame_id, str) else frame_id,
                capture_result.error_message,
            )
            return {
                "success": False,
                "frameId": frame_id,
                "errorCode": capture_result.error_code or "capture_failed",
                "errorMessage": capture_result.error_message,
            }

        image_data = capture_result.image_data
        content_type = capture_result.content_type or "image/jpeg"
        captured_at = capture_result.captured_at or dt.now(timezone.utc)

        # Preprocess image (resize/compress) if preprocessor is available
        original_size_bytes = len(image_data) if image_data else 0
        was_resized = False
        if image_data and self._image_preprocessor:
            preprocess_result = self._image_preprocessor.preprocess(
                image_data, content_type
            )
            image_data = preprocess_result.image_data
            content_type = preprocess_result.content_type
            was_resized = preprocess_result.was_resized
            if was_resized:
                LOGGER.debug(
                    "Image preprocessed for frame %s: %dx%d -> %dx%d, %d -> %d bytes",
                    frame_id[:8] if isinstance(frame_id, str) else frame_id,
                    preprocess_result.original_size[0],
                    preprocess_result.original_size[1],
                    preprocess_result.processed_size[0],
                    preprocess_result.processed_size[1],
                    preprocess_result.original_bytes,
                    preprocess_result.processed_bytes,
                )

        # Validate size after preprocessing
        if image_data and len(image_data) > max_size_bytes:
            return {
                "success": False,
                "frameId": frame_id,
                "errorCode": "image_too_large",
                "errorMessage": f"Image size {len(image_data)} exceeds max {max_size_bytes}",
            }

        # Upload to S3
        result = await self._s3_upload.upload(
            presigned_url=upload_url,
            data=image_data,
            s3_key=blob_key,
            content_type=content_type,
        )

        if result.success:
            LOGGER.info(
                "Captured and uploaded frame %s: %d bytes to %s%s",
                frame_id[:8] if isinstance(frame_id, str) else frame_id,
                result.file_size_bytes,
                blob_key,
                f" (resized from {original_size_bytes} bytes)" if was_resized else "",
            )
            response: Dict[str, Any] = {
                "success": True,
                "frameId": frame_id,
                "capturedAt": captured_at.isoformat(),
                "fileSizeBytes": result.file_size_bytes,
            }
            if was_resized:
                response["originalSizeBytes"] = original_size_bytes
                response["wasResized"] = True
            # Include image dimensions if available
            if capture_result.image_width is not None:
                response["imageWidth"] = capture_result.image_width
            if capture_result.image_height is not None:
                response["imageHeight"] = capture_result.image_height
            return response
        else:
            LOGGER.warning(
                "Failed to upload frame %s: %s",
                frame_id[:8] if isinstance(frame_id, str) else frame_id,
                result.error_message,
            )
            return {
                "success": False,
                "frameId": frame_id,
                "errorCode": result.error_code or "upload_failed",
                "errorMessage": result.error_message,
            }

    async def _execute_upload_timelapse(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Handle task:upload-timelapse command.

        This command is sent by the server to request timelapse video upload.
        The agent fetches the video and preview image from Moonraker's
        timelapse directory and uploads them to the presigned S3 URLs.

        Parameters:
            printJobId (str): The print job ID
            videoUploadUrl (str): Presigned URL for uploading video
            videoKey (str): S3 key for the video
            previewUploadUrl (str, optional): Presigned URL for uploading preview
            previewKey (str, optional): S3 key for the preview
            videoFilename (str): Video filename from timelapse event
            previewFilename (str, optional): Preview image filename
            maxVideoSizeBytes (int, optional): Maximum video size allowed
            maxPreviewSizeBytes (int, optional): Maximum preview size allowed
        """
        if self._s3_upload is None:
            raise CommandProcessingError(
                "S3 upload client unavailable",
                code="upload_unavailable",
                command_id=message.command_id,
            )

        params = message.parameters or {}

        print_job_id = params.get("printJobId")
        video_upload_url = params.get("videoUploadUrl")
        video_key = params.get("videoKey")
        video_filename = params.get("videoFilename")
        preview_upload_url = params.get("previewUploadUrl")
        preview_key = params.get("previewKey")
        preview_filename = params.get("previewFilename")
        max_video_size = params.get("maxVideoSizeBytes", 500 * 1024 * 1024)  # 500 MB default
        max_preview_size = params.get("maxPreviewSizeBytes", 5 * 1024 * 1024)  # 5 MB default

        # Validate required parameters
        if not video_upload_url or not isinstance(video_upload_url, str):
            raise CommandProcessingError(
                "videoUploadUrl parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not video_key or not isinstance(video_key, str):
            raise CommandProcessingError(
                "videoKey parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not video_filename or not isinstance(video_filename, str):
            raise CommandProcessingError(
                "videoFilename parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        result_data: Dict[str, Any] = {
            "printJobId": print_job_id,
        }

        # Fetch and upload video
        video_uploaded = False
        try:
            LOGGER.info(
                "Fetching timelapse video %s for job %s",
                video_filename,
                print_job_id[:8] if print_job_id else "unknown",
            )
            video_data = await self._moonraker.fetch_timelapse_file(
                video_filename, timeout=120.0
            )
        except Exception as exc:
            LOGGER.error("Failed to fetch timelapse video: %s", exc)
            return {
                "success": False,
                "printJobId": print_job_id,
                "errorCode": "video_fetch_failed",
                "errorMessage": str(exc),
            }

        if video_data is None:
            return {
                "success": False,
                "printJobId": print_job_id,
                "errorCode": "video_not_found",
                "errorMessage": f"Timelapse video not found: {video_filename}",
            }

        # Validate video size
        if len(video_data) > max_video_size:
            return {
                "success": False,
                "printJobId": print_job_id,
                "errorCode": "video_too_large",
                "errorMessage": f"Video size {len(video_data)} exceeds max {max_video_size}",
            }

        # Determine content type
        video_content_type = "video/mp4"
        if video_filename.endswith(".webm"):
            video_content_type = "video/webm"

        # Upload video to S3
        video_result = await self._s3_upload.upload(
            presigned_url=video_upload_url,
            data=video_data,
            s3_key=video_key,
            content_type=video_content_type,
        )

        if video_result.success:
            video_uploaded = True
            result_data["videoKey"] = video_key
            result_data["videoSizeBytes"] = video_result.file_size_bytes
            LOGGER.info(
                "Uploaded timelapse video for job %s: %d bytes to %s",
                print_job_id[:8] if print_job_id else "unknown",
                video_result.file_size_bytes,
                video_key,
            )
        else:
            LOGGER.warning(
                "Failed to upload timelapse video for job %s: %s",
                print_job_id[:8] if print_job_id else "unknown",
                video_result.error_message,
            )
            return {
                "success": False,
                "printJobId": print_job_id,
                "errorCode": video_result.error_code or "video_upload_failed",
                "errorMessage": video_result.error_message,
            }

        # Fetch and upload preview (optional)
        preview_uploaded = False
        if preview_upload_url and preview_key and preview_filename:
            try:
                preview_data = await self._moonraker.fetch_timelapse_file(
                    preview_filename, timeout=30.0
                )

                if preview_data:
                    # Validate preview size
                    if len(preview_data) > max_preview_size:
                        LOGGER.warning(
                            "Preview image too large: %d > %d, skipping",
                            len(preview_data),
                            max_preview_size,
                        )
                    else:
                        preview_result = await self._s3_upload.upload(
                            presigned_url=preview_upload_url,
                            data=preview_data,
                            s3_key=preview_key,
                            content_type="image/jpeg",
                        )

                        if preview_result.success:
                            preview_uploaded = True
                            result_data["previewKey"] = preview_key
                            result_data["previewSizeBytes"] = preview_result.file_size_bytes
                            LOGGER.info(
                                "Uploaded timelapse preview for job %s: %d bytes to %s",
                                print_job_id[:8] if print_job_id else "unknown",
                                preview_result.file_size_bytes,
                                preview_key,
                            )
                        else:
                            LOGGER.warning(
                                "Failed to upload timelapse preview: %s",
                                preview_result.error_message,
                            )
                else:
                    LOGGER.debug("Preview image not found: %s", preview_filename)
            except Exception as exc:
                LOGGER.warning("Failed to fetch timelapse preview: %s", exc)

        result_data["success"] = video_uploaded
        result_data["previewUploaded"] = preview_uploaded
        return result_data

    # -------------------------------------------------------------------------
    # Heater Control Commands
    # -------------------------------------------------------------------------

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

    # -------------------------------------------------------------------------
    # Fan Control Commands
    # -------------------------------------------------------------------------

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
                fan_name = fan_lower[len("fan_generic ") :]
            elif fan_lower.startswith("heater_fan "):
                fan_name = fan_lower[len("heater_fan ") :]
            elif fan_lower.startswith("controller_fan "):
                fan_name = fan_lower[len("controller_fan ") :]
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

    # -------------------------------------------------------------------------
    # Object Control Commands (ADR-0016: Exclude Object)
    # -------------------------------------------------------------------------

    # Regex pattern for valid object names (alphanumeric, underscores, hyphens, dots)
    # Used to prevent G-code injection attacks
    # Dots are allowed because slicers like PrusaSlicer/OrcaSlicer use them in object names
    _OBJECT_NAME_PATTERN = __import__("re").compile(r"^[\w\-\.]+$")

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
        if not self._OBJECT_NAME_PATTERN.match(object_name):
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

    # -------------------------------------------------------------------------
    # Heater/Fan Discovery Helpers
    # -------------------------------------------------------------------------

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

    async def _publish_ack(
        self,
        command_name: str,
        command_id: Optional[str],
        status: str,
        *,
        stage: str = "execution",
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None,
        skipped: bool = False,
    ) -> None:
        """Publish command acknowledgment to MQTT.

        Args:
            command_name: The command name (e.g., "print:pause").
            command_id: Unique command identifier.
            status: ACK status ("accepted", "completed", "failed").
            stage: Processing stage ("dispatch", "execution").
            error_code: Error code if failed.
            error_message: Error message if failed.
            result: Optional result data from command execution.
            skipped: If True, indicates this is a duplicate command replay (ADR-0013 Appendix D).
        """
        topic = _build_ack_topic(self._device_id, command_name)
        acknowledged_at = datetime.now(timezone.utc).replace(microsecond=0)
        stage_value = stage.lower()
        effective_command_id = command_id or ""
        document: Dict[str, Any] = {
            "commandId": effective_command_id,
            "deviceId": self._device_id,
            "status": status,
            "stage": stage_value,
            "timestamps": {
                "acknowledgedAt": acknowledged_at.isoformat(timespec="seconds"),
            },
        }

        # ADR-0013 Appendix D: Indicate duplicate detection
        if skipped:
            document["skipped"] = True

        if stage_value == "dispatch":
            document["timestamps"]["dispatchedAt"] = acknowledged_at.isoformat(
                timespec="seconds"
            )

        if self._tenant_id:
            document["tenantId"] = self._tenant_id
        if self._printer_id:
            document["printerId"] = self._printer_id
        correlation: Dict[str, Any] = {}
        if self._tenant_id:
            correlation["tenantId"] = self._tenant_id
        if self._printer_id:
            correlation["printerId"] = self._printer_id
        if correlation:
            document["correlation"] = correlation

        if error_code or error_message:
            reason: Dict[str, Any] = {}
            if error_code:
                reason["code"] = error_code
            if error_message:
                reason["message"] = error_message
            if reason:
                document["reason"] = reason

        # Include execution result data (e.g., capture details, uploaded file info)
        if result:
            document["result"] = result

        payload = json.dumps(document).encode("utf-8")
        self._mqtt.publish(topic, payload, qos=1, retain=False)

        if stage_value == "execution":
            self._remember_history(
                effective_command_id,
                status=status,
                stage=stage_value,
                error_code=error_code,
                error_message=error_message,
            )


def _parse_command(raw_payload: bytes, command_name: str) -> CommandMessage:
    try:
        decoded = raw_payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CommandProcessingError(
            "Payload is not valid UTF-8", code="invalid_encoding"
        ) from exc

    try:
        data = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise CommandProcessingError(
            "Payload is not valid JSON", code="invalid_json"
        ) from exc

    command_id = str(data.get("commandId", "")).strip()
    if not command_id:
        raise CommandProcessingError(
            "Missing commandId in payload", code="invalid_payload"
        )

    command_field = data.get("command") or data.get("action") or ""
    parsed_command = (
        str(command_field).strip().lower() if command_field else command_name
    )
    if not parsed_command:
        raise CommandProcessingError(
            "Missing command in payload",
            code="invalid_payload",
            command_id=command_id,
        )

    parameters = data.get("parameters")
    if parameters is None:
        parameters = data.get("payload", {}) or {}

    if not isinstance(parameters, dict):
        raise CommandProcessingError(
            "parameters field must be an object",
            code="invalid_parameters",
            command_id=command_id,
        )

    return CommandMessage(
        command_id=command_id, command=parsed_command, parameters=parameters
    )


def _build_ack_topic(device_id: str, command_name: str) -> str:
    safe_command_name = quote(command_name, safe="")
    return f"owl/printers/{device_id}/acks/{safe_command_name}"


def _parse_iso8601(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def _extract_command_name(topic: str, device_id: str) -> Optional[str]:
    segments = topic.split("/")
    if len(segments) != 5:
        return None

    prefix_match = (
        segments[0].lower() == "owl"
        and segments[1].lower() == "printers"
        and segments[2] == device_id
        and segments[3].lower() == "commands"
    )

    if not prefix_match:
        return None

    return segments[4]


def _resolve_identity(config: OwlConfig) -> tuple[Optional[str], str, Optional[str]]:
    parser = config.raw
    device_id = parser.get("cloud", "device_id", fallback="").strip()
    tenant_id = parser.get("cloud", "tenant_id", fallback="").strip()
    printer_id = parser.get("cloud", "printer_id", fallback="").strip()

    username = (config.cloud.username or "").strip()
    if not device_id and ":" in username:
        _, maybe_device = username.split(":", 1)
        device_id = maybe_device
    if not tenant_id and username:
        tenant_id = username.split(":", 1)[0]

    if not device_id:
        raise CommandConfigurationError("Device ID is required for command handling")

    return tenant_id or None, device_id, printer_id or None
