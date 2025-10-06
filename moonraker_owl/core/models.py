"""Domain models for telemetry and commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping


@dataclass(slots=True)
class TelemetrySample:
    channels: Mapping[str, str]


@dataclass(slots=True)
class CommandRequest:
    command_id: str
    action: str
    payload: Dict[str, str]
