"""Command handling pipeline for Moonraker Owl."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..printer_command_names import PrinterCommandNames
from ..identifiers import uuid7
from ..config import OwlConfig
from ..version import __version__
from ..telemetry import TelemetryPublisher
from ..adapters.s3_upload import S3UploadClient
from ..adapters.camera import CameraClient
from ..adapters.image_preprocessor import ImagePreprocessor
from ..core.idempotency import CommandIdempotencyGuard
from ..core.job_registry import PrintJobRegistry

from .types import (
    CommandConfigurationError,
    CommandMessage,
    CommandProcessingError,
    MoonrakerCommandClient,
    MQTTCommandsClient,
    COMMAND_EXPECTED_STATES,
    _InflightCommand,
    _PendingStateCommand,
)
from .parsing import _build_ack_topic, _extract_command_name, _parse_command, _resolve_identity
from .handlers import (
    ControlCommandsMixin,
    FanCommandsMixin,
    HeaterCommandsMixin,
    MetadataCommandsMixin,
    PrintCommandsMixin,
    QueryCommandsMixin,
    TaskCommandsMixin,
)

LOGGER = logging.getLogger(__name__)


class CommandProcessor(
    PrintCommandsMixin,
    HeaterCommandsMixin,
    FanCommandsMixin,
    TaskCommandsMixin,
    ControlCommandsMixin,
    QueryCommandsMixin,
    MetadataCommandsMixin,
):
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
        metadata_reporter: Optional[Any] = None,
    ) -> None:
        self._config = config
        self._moonraker = moonraker
        self._mqtt = mqtt
        self._telemetry = telemetry
        self._s3_upload = s3_upload
        self._camera = camera
        self._image_preprocessor = image_preprocessor
        self._job_registry = job_registry
        self._metadata_reporter = metadata_reporter

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

        # State-based command completion tracking
        self._pending_state_commands: Dict[str, _PendingStateCommand] = {}
        self._command_timeout_seconds = 30.0
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Capture concurrency control: prevent overlapping captures (C-01)
        self._capture_semaphore = asyncio.Semaphore(1)
        self._last_capture_time: Optional[float] = None
        self._min_capture_interval = 2.0  # seconds

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
            except (OSError, asyncio.TimeoutError) as exc:
                LOGGER.exception(
                    "Error sending %s ACK for %s", status, pending.command_id[:8]
                )

        self._loop.call_soon(
            lambda: self._loop.create_task(send_ack())
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
        - print:firmware-restart: Restart Klipper firmware
        - print:start: Start printing a specified GCode file
        - query:file-list: Query GCode file list (Cold Path data retrieval)
        - task:upload-thumbnail: Upload thumbnail to presigned URL
        - task:capture-image: Capture and upload camera frame
        - object:exclude: Exclude an object from the current print (ADR-0016)
        """
        LOGGER.debug("[CommandDispatch] Executing command: %s (id=%s)", message.command, message.command_id[:8])
        
        # System control commands
        if message.command == PrinterCommandNames.SET_TELEMETRY_RATE:
            return self._execute_set_telemetry_rate(message)

        # Metadata commands
        if message.command == PrinterCommandNames.METADATA_SYSTEM_REFRESH:
            return self._execute_metadata_system_refresh(message)

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

        # System task commands
        if message.command == PrinterCommandNames.UPLOAD_THUMBNAIL:
            return await self._execute_upload_thumbnail(message)

        if message.command == PrinterCommandNames.CAPTURE_IMAGE:
            return await self._execute_capture_image(message)

        if message.command == PrinterCommandNames.DOWNLOAD_GCODE:
            return await self._execute_download_gcode(message)

        # Job lifecycle commands
        if message.command == PrinterCommandNames.JOB_REGISTERED:
            return self._execute_job_registered(message)

        # Emergency stop command
        if message.command == PrinterCommandNames.EMERGENCY_STOP:
            return await self._execute_emergency_stop(message)

        # Firmware restart command
        if message.command == PrinterCommandNames.FIRMWARE_RESTART:
            return await self._execute_firmware_restart(message)

        # Start print command
        if message.command == PrinterCommandNames.START:
            return await self._handle_print_start(message)

        # Query commands (Cold Path — data retrieval via ACK result)
        if message.command == PrinterCommandNames.QUERY_FILE_LIST:
            return await self._handle_query_file_list(message)

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

            # Trigger immediate print_stats query to detect state change.
            # Moonraker may not push state changes via WebSocket for these commands,
            # so we actively query to ensure the orchestrator sees the transition.
            if self._telemetry is not None:
                try:
                    await self._telemetry.request_print_state_query()
                except Exception as exc:  # pragma: no cover - non-critical
                    LOGGER.debug("Failed to request print state query: %s", exc)

            return {"command": message.command}

        # Unknown command
        raise CommandProcessingError(
            f"Unknown command: {message.command}",
            code="unsupported_command",
            command_id=message.command_id,
        )

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
        """Publish command acknowledgment to MQTT using $-prefix envelope.

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

        # Build payload (business data)
        ack_payload: Dict[str, Any] = {
            "commandId": effective_command_id,
            "status": status,
            "stage": stage_value,
        }

        # ADR-0013 Appendix D: Indicate duplicate detection
        if skipped:
            ack_payload["skipped"] = True

        if self._tenant_id:
            ack_payload["tenantId"] = self._tenant_id
        if self._printer_id:
            ack_payload["printerId"] = self._printer_id
        correlation: Dict[str, Any] = {}
        if self._tenant_id:
            correlation["tenantId"] = self._tenant_id
        if self._printer_id:
            correlation["printerId"] = self._printer_id
        if correlation:
            ack_payload["correlation"] = correlation

        if error_code or error_message:
            reason: Dict[str, Any] = {}
            if error_code:
                reason["code"] = error_code
            if error_message:
                reason["message"] = error_message
            if reason:
                ack_payload["reason"] = reason

        # Include execution result data (e.g., capture details, uploaded file info)
        if result:
            ack_payload["result"] = result

        # Build $-prefix envelope (D16) — $ts serves as acknowledgedAt, no separate timestamps object
        document: Dict[str, Any] = {
            "$v": 1,
            "$type": "command.ack",
            "$id": str(uuid7()),
            "$ts": acknowledged_at.isoformat(timespec="seconds"),
            "$origin": f"moonraker-owl@{__version__}",
            "deviceId": self._device_id,
            "payload": ack_payload,
        }

        payload_bytes = json.dumps(document).encode("utf-8")
        self._mqtt.publish(topic, payload_bytes, qos=1, retain=False)

        if stage_value == "execution":
            self._remember_history(
                effective_command_id,
                status=status,
                stage=stage_value,
                error_code=error_code,
                error_message=error_message,
            )
