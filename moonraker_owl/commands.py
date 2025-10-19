"""Command handling pipeline for Moonraker Owl."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Protocol
from urllib.parse import quote

from .config import OwlConfig

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


class CommandProcessor:
    """Consumes MQTT command messages and forwards them to Moonraker."""

    def __init__(
        self,
        config: OwlConfig,
        moonraker: MoonrakerCommandClient,
        mqtt: MQTTCommandsClient,
    ) -> None:
        self._config = config
        self._moonraker = moonraker
        self._mqtt = mqtt

        (
            self._tenant_id,
            self._device_id,
            self._printer_id,
        ) = _resolve_identity(config)

        self._command_topic_prefix = f"owl/printers/{self._device_id}/commands"
        self._command_subscription = f"{self._command_topic_prefix}/#"
        self._handler_registered = False

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
                error_code=exc.code or "invalid_payload",
                error_message=str(exc),
            )
            return

        try:
            await self._publish_ack(
                command_name,
                message.command_id,
                "accepted",
                stage="dispatch",
            )

            await self._execute(message)
        except CommandProcessingError as exc:
            await self._publish_ack(
                command_name,
                message.command_id,
                "failed",
                stage="execution",
                error_code=exc.code or "command_failed",
                error_message=str(exc),
            )
        else:
            await self._publish_ack(
                command_name,
                message.command_id,
                "success",
                stage="execution",
            )

    async def _execute(self, message: CommandMessage) -> None:
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
        if not command_id:
            LOGGER.debug(
                "Skipping acknowledgment for %s because command ID is not available",
                command_name,
            )
            return

        topic = _build_ack_topic(self._device_id, command_name)
        document: Dict[str, Any] = {
            "commandId": command_id,
            "deviceId": self._device_id,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }

        if stage:
            document["stage"] = stage

        if self._tenant_id:
            document["tenantId"] = self._tenant_id
        if self._printer_id:
            document["printerId"] = self._printer_id
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
