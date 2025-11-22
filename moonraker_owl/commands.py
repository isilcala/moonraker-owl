"""Command handling pipeline for Moonraker Owl."""

from __future__ import annotations

import json
import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Optional, Protocol
from urllib.parse import quote

from .config import OwlConfig
from .telemetry import TelemetryPublisher

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
    async def execute_print_action(self, action: str) -> None: ...


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


class CommandProcessor:
    """Consumes MQTT command messages and forwards them to Moonraker."""

    def __init__(
        self,
        config: OwlConfig,
        moonraker: MoonrakerCommandClient,
        mqtt: MQTTCommandsClient,
        telemetry: Optional[TelemetryPublisher] = None,
    ) -> None:
        self._config = config
        self._moonraker = moonraker
        self._mqtt = mqtt
        self._telemetry = telemetry

        (
            self._tenant_id,
            self._device_id,
            self._printer_id,
        ) = _resolve_identity(config)

        self._command_topic_prefix = f"owl/printers/{self._device_id}/commands"
        self._command_subscription = f"{self._command_topic_prefix}/#"
        self._handler_registered = False
        self._inflight: Dict[str, _InflightCommand] = {}
        self._history: Dict[str, _CommandHistoryEntry] = {}
        self._history_order: Deque[str] = deque()
        self._history_limit = 64

    async def start(self) -> None:
        if self._handler_registered:
            raise RuntimeError("CommandProcessor already started")

        self._mqtt.set_message_handler(self._handle_message)
        self._mqtt.subscribe(self._command_subscription, qos=1)
        self._handler_registered = True
        LOGGER.info("Command processor subscribed to %s", self._command_subscription)

    async def stop(self) -> None:
        if not self._handler_registered:
            return

        try:
            self._mqtt.unsubscribe(self._command_subscription)
        except Exception as exc:  # pragma: no cover - defensive cleanup
            LOGGER.debug("Error unsubscribing from command topic: %s", exc)
        finally:
            self._mqtt.set_message_handler(None)
            self._handler_registered = False

    async def _handle_message(self, topic: str, payload: bytes) -> None:
        command_name = _extract_command_name(topic, self._device_id)
        if not command_name:
            return

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
            LOGGER.debug(
                "Duplicate inflight command received: %s", message.command_id
            )
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
            await self._publish_ack(
                command_name,
                message.command_id,
                "completed",
                stage="execution",
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
            LOGGER.debug("Failed to record command state %s", state, exc_info=True)

    def _begin_inflight(self, command_name: str, message: CommandMessage) -> None:
        self._inflight[message.command_id] = _InflightCommand(
            command_name=command_name,
            message=message,
            dispatched_at=datetime.now(timezone.utc),
        )

    def _finish_inflight(self, command_id: str) -> None:
        self._inflight.pop(command_id, None)

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

    async def _replay_duplicate(self, command_name: str, command_id: str) -> bool:
        entry = self._history.get(command_id)
        if entry is None:
            return False

        LOGGER.debug("Replaying duplicate command %s with cached status", command_id)
        await self._publish_ack(
            command_name,
            command_id,
            entry.status,
            stage=entry.stage,
            error_code=entry.error_code,
            error_message=entry.error_message,
        )
        return True

    @property
    def pending_count(self) -> int:
        return len(self._inflight)

    async def abandon_inflight(self, reason: str) -> None:
        if not self._inflight:
            return

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

    async def _execute(self, message: CommandMessage) -> Optional[Dict[str, Any]]:
        if message.command == "sensors:set-rate":
            return self._execute_sensors_set_rate(message)

        try:
            await self._moonraker.execute_print_action(message.command)
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

    def _execute_sensors_set_rate(self, message: CommandMessage) -> Dict[str, Any]:
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

        expires_at = self._telemetry.apply_sensors_rate(
            mode=mode,
            max_hz=max_hz,
            duration_seconds=duration_seconds,
            requested_at=requested_at,
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

    async def _publish_ack(
        self,
        command_name: str,
        command_id: Optional[str],
        status: str,
        *,
        stage: str = "execution",
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
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

        if not command_id:
            LOGGER.debug(
                "Publishing acknowledgment for %s without a command ID", command_name
            )

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
