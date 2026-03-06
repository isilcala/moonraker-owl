"""Module-level utility functions for command parsing and routing."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote

from ..config import OwlConfig
from .types import CommandConfigurationError, CommandMessage, CommandProcessingError


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

    # $-prefix envelope: $id is the command identifier, business data in payload
    command_id = str(data.get("$id", "") or data.get("commandId", "")).strip()
    if not command_id:
        raise CommandProcessingError(
            "Missing $id in command envelope", code="invalid_payload"
        )

    # Business data lives in payload sub-object
    inner = data.get("payload", data)

    command_field = inner.get("command") or inner.get("action") or ""
    parsed_command = (
        str(command_field).strip().lower() if command_field else command_name
    )
    if not parsed_command:
        raise CommandProcessingError(
            "Missing command in payload",
            code="invalid_payload",
            command_id=command_id,
        )

    parameters = inner.get("parameters")
    if parameters is None:
        parameters = {}

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
    device_id = (config.cloud.device_id or "").strip()
    tenant_id = (config.cloud.tenant_id or "").strip()
    printer_id = (config.cloud.printer_id or "").strip()

    username = (config.cloud.username or "").strip()
    if not device_id and ":" in username:
        _, maybe_device = username.split(":", 1)
        device_id = maybe_device
    if not tenant_id and username:
        tenant_id = username.split(":", 1)[0]

    if not device_id:
        raise CommandConfigurationError("Device ID is required for command handling")

    return tenant_id or None, device_id, printer_id or None
