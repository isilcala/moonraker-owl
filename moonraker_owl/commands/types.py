"""Shared types, protocols, and exceptions for the command pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Protocol

from ..printer_command_names import PrinterCommandNames


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
    """Protocol for Moonraker command execution."""

    async def execute_print_action(self, action: str) -> None: ...

    async def execute_gcode(self, script: str, **kwargs) -> None: ...

    async def emergency_stop(self) -> None: ...

    async def firmware_restart(self) -> None: ...

    async def start_print(self, filename: str) -> None: ...

    async def list_gcode_files(self, timeout: float = 15.0) -> list[dict]: ...

    async def fetch_available_heaters(
        self, timeout: float = 5.0
    ) -> dict[str, list[str]]: ...

    async def fetch_thumbnail(
        self,
        relative_path: str,
        gcode_filename: Optional[str] = None,
        timeout: float = 30.0,
    ) -> Optional[bytes]: ...

    async def fetch_gcode_metadata(
        self, filename: str, timeout: float = 10.0
    ) -> Optional[dict]: ...

    async def fetch_timelapse_file(
        self, filename: str, timeout: float = 120.0
    ) -> Optional[bytes]: ...


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
    def set_message_handler(self, handler) -> None: ...


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
class _PendingStateCommand:
    """Tracks a command awaiting state confirmation."""

    command_id: str
    command_name: str
    message: CommandMessage
    expected_state: str
    accepted_at: datetime
    timeout_seconds: float = 30.0

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        current = now or datetime.now(timezone.utc)
        deadline = self.accepted_at + timedelta(seconds=self.timeout_seconds)
        return current >= deadline

    @property
    def remaining_seconds(self) -> float:
        now = datetime.now(timezone.utc)
        deadline = self.accepted_at + timedelta(seconds=self.timeout_seconds)
        remaining = (deadline - now).total_seconds()
        return max(0.0, remaining)


# Commands that require state confirmation before sending 'completed' ACK
COMMAND_EXPECTED_STATES: Dict[str, str] = {
    PrinterCommandNames.PAUSE: "paused",
    PrinterCommandNames.RESUME: "printing",
    PrinterCommandNames.CANCEL: "cancelled",
}
