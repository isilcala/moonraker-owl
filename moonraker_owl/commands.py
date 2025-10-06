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


class MoonrakerCommandClient(Protocol):
    async def execute_print_action(self, action: str) -> None: ...


class MQTTCommandsClient(Protocol):
    def subscribe(self, topic: str, qos: int = 1) -> None: ...

    def unsubscribe(self, topic: str) -> None: ...

    def publish(
        self, topic: str, payload: bytes, qos: int = 1, retain: bool = False
    ) -> None: ...

    def set_message_handler(self, handler): ...


@dataclass(slots=True)
class CommandMessage:
    command_id: str
    action: str
    payload: Dict[str, Any]


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

        self._command_topic = f"owl/printers/{self._device_id}/commands/invoke"
        self._handler_registered = False

    async def start(self) -> None:
        if self._handler_registered:
            raise RuntimeError("CommandProcessor already started")

        self._mqtt.set_message_handler(self._handle_message)
        self._mqtt.subscribe(self._command_topic, qos=1)
        self._handler_registered = True
        LOGGER.info("Command processor subscribed to %s", self._command_topic)

    async def stop(self) -> None:
        if not self._handler_registered:
            return

        try:
            self._mqtt.unsubscribe(self._command_topic)
        except Exception as exc:  # pragma: no cover - defensive cleanup
            LOGGER.debug("Error unsubscribing from command topic: %s", exc)
        finally:
            self._mqtt.set_message_handler(None)
            self._handler_registered = False

    async def _handle_message(self, topic: str, payload: bytes) -> None:
        if topic != self._command_topic:
            return

        try:
            message = _parse_command(payload)
        except CommandProcessingError as exc:
            LOGGER.warning("Invalid command payload: %s", exc)
            await self._publish_ack(
                command_id="unknown",
                status="failed",
                detail=str(exc),
            )
            return

        await self._publish_ack(message.command_id, "received")

        try:
            await self._execute(message)
        except CommandProcessingError as exc:
            await self._publish_ack(
                message.command_id,
                "failed",
                detail=str(exc),
            )
        else:
            await self._publish_ack(message.command_id, "succeeded")

    async def _execute(self, message: CommandMessage) -> None:
        try:
            await self._moonraker.execute_print_action(message.action)
        except ValueError as exc:
            raise CommandProcessingError(str(exc)) from exc
        except Exception as exc:  # pragma: no cover - networking errors
            raise CommandProcessingError(f"Moonraker command failed: {exc}") from exc

    async def _publish_ack(
        self,
        command_id: str,
        status: str,
        *,
        detail: Optional[str] = None,
    ) -> None:
        topic = _build_ack_topic(self._device_id, command_id)
        document: Dict[str, Any] = {
            "commandId": command_id,
            "deviceId": self._device_id,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }

        if self._tenant_id:
            document["tenantId"] = self._tenant_id
        if self._printer_id:
            document["printerId"] = self._printer_id
        if detail:
            document["detail"] = detail

        payload = json.dumps(document).encode("utf-8")
        self._mqtt.publish(topic, payload, qos=1, retain=False)


def _parse_command(raw_payload: bytes) -> CommandMessage:
    try:
        decoded = raw_payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CommandProcessingError("Payload is not valid UTF-8") from exc

    try:
        data = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise CommandProcessingError("Payload is not valid JSON") from exc

    command_id = str(data.get("commandId", "")).strip()
    action = str(data.get("action", "")).strip()
    payload = data.get("payload", {}) or {}

    if not command_id:
        raise CommandProcessingError("Missing commandId in payload")
    if not action:
        raise CommandProcessingError("Missing action in payload")
    if not isinstance(payload, dict):
        raise CommandProcessingError("payload field must be an object")

    return CommandMessage(command_id=command_id, action=action, payload=payload)


def _build_ack_topic(device_id: str, command_id: str) -> str:
    safe_command_id = quote(command_id, safe="")
    return f"owl/printers/{device_id}/commands/{safe_command_id}/ack"


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
