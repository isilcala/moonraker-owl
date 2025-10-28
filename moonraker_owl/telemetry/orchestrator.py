"""High-level orchestration for the telemetry pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..version import __version__
from .events import EventCollector
from .selectors import EventsSelector, OverviewSelector, TelemetrySelector
from .state_store import MoonrakerStateStore
from .trackers import HeaterMonitor, PrintSessionTracker


@dataclass
class ChannelEnvelope:
    channel: str
    sequence: int
    payload: Dict[str, Any]


class TelemetryOrchestrator:
    """Coordinates store, trackers, and selectors to emit channel payloads."""

    def __init__(
        self,
        *,
        origin: Optional[str] = None,
        heartbeat_seconds: int = 60,
        clock=None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._origin = origin or f"moonraker-owl@{__version__}"

        self.store = MoonrakerStateStore(clock=self._clock)
        self.session_tracker = PrintSessionTracker()
        self.heater_monitor = HeaterMonitor()
        self.events = EventCollector()

        self.overview_selector = OverviewSelector(heartbeat_seconds=heartbeat_seconds)
        self.telemetry_selector = TelemetrySelector()
        self.events_selector = EventsSelector()

        self._channel_sequences: Dict[str, int] = {}

        self._telemetry_mode = "idle"
        self._telemetry_max_hz = 0.033
        self._watch_window_expires: Optional[datetime] = None

    def ingest(self, payload: Dict[str, Any]) -> None:
        self.store.ingest(payload)
        self.heater_monitor.refresh(self.store)

    def set_telemetry_mode(
        self, *, mode: str, max_hz: float, watch_window_expires: Optional[datetime]
    ) -> None:
        self._telemetry_mode = mode
        self._telemetry_max_hz = max_hz
        self._watch_window_expires = watch_window_expires

    def build_envelopes(self) -> Dict[str, Dict[str, Any]]:
        observed_at = self._clock()
        session = self.session_tracker.compute(self.store)

        overview_payload = self.overview_selector.build(
            self.store,
            session,
            self.heater_monitor,
            observed_at,
        )
        telemetry_payload = self.telemetry_selector.build(
            self.store,
            mode=self._telemetry_mode,
            max_hz=self._telemetry_max_hz,
            watch_window_expires=self._watch_window_expires,
            observed_at=observed_at,
        )
        events_payload = self.events_selector.build(
            events=self.events.drain(),
            observed_at=observed_at,
        )

        frames: Dict[str, Dict[str, Any]] = {}
        if overview_payload:
            overview_payload["flags"]["watchWindowActive"] = (
                self._telemetry_mode != "idle"
            )
            frames["overview"] = self._wrap_envelope(
                channel="overview",
                body=overview_payload,
                session_id=session.session_id,
            )

        if telemetry_payload:
            frames["telemetry"] = self._wrap_envelope(
                channel="telemetry",
                body=telemetry_payload,
                session_id=session.session_id,
            )

        if events_payload:
            frames["events"] = self._wrap_envelope(
                channel="events",
                body=events_payload,
                session_id=session.session_id,
            )

        return frames

    def _wrap_envelope(
        self,
        *,
        channel: str,
        body: Dict[str, Any],
        session_id: str,
    ) -> Dict[str, Any]:
        sequence = self._next_sequence(channel)
        envelope = {
            "_schema": 1,
            "kind": "full",
            "_ts": self._clock().replace(microsecond=0).isoformat(),
            "_origin": self._origin,
            "_seq": sequence,
            "sessionId": session_id,
            channel: body,
        }
        return envelope

    def _next_sequence(self, channel: str) -> int:
        current = self._channel_sequences.get(channel, 0) + 1
        self._channel_sequences[channel] = current
        return current
