"""Telemetry event collection helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class EventCollector:
    def __init__(self) -> None:
        self._pending: List[Dict[str, Any]] = []

    def record_command_state(
        self,
        *,
        command_id: str,
        command_type: str,
        state: str,
        occurred_at: Optional[datetime] = None,
        session_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        occurred = occurred_at or datetime.now(timezone.utc)
        payload: Dict[str, Any] = {
            "eventName": "commandStateChanged",
            "severity": "info",
            "occurredAtUtc": occurred.replace(microsecond=0).isoformat(),
            "data": {
                "commandId": command_id,
                "commandType": command_type,
                "state": state,
            },
        }
        if session_id:
            payload["sessionId"] = session_id
        if details:
            payload["data"].update(details)
        self._pending.append(payload)

    def drain(self) -> List[Dict[str, Any]]:
        events = list(self._pending)
        self._pending.clear()
        return events
