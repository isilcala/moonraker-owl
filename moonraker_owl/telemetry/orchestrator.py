"""High-level orchestration for the telemetry pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Set

from ..version import __version__
from .events import EventCollector
from .selectors import EventsSelector, OverviewSelector, TelemetrySelector
from .state_store import MoonrakerStateStore
from .trackers import HeaterMonitor, PrintSessionTracker


@dataclass
class ChannelPayload:
    channel: str
    payload: Dict[str, Any]
    session_id: str
    observed_at: datetime
    forced: bool


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

    @property
    def origin(self) -> str:
        return self._origin

    def build_payloads(
        self,
        *,
        forced_channels: Optional[Iterable[str]] = None,
    ) -> Dict[str, ChannelPayload]:
        observed_at = self._clock()
        forced: Set[str] = set(forced_channels or ())
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

        frames: Dict[str, ChannelPayload] = {}
        if overview_payload:
            overview_payload["flags"]["watchWindowActive"] = (
                self._telemetry_mode != "idle"
            )
            frames["overview"] = ChannelPayload(
                channel="overview",
                payload=overview_payload,
                session_id=session.session_id,
                observed_at=observed_at,
                forced="overview" in forced,
            )

        if telemetry_payload:
            frames["telemetry"] = ChannelPayload(
                channel="telemetry",
                payload=telemetry_payload,
                session_id=session.session_id,
                observed_at=observed_at,
                forced="telemetry" in forced,
            )

        if events_payload:
            frames["events"] = ChannelPayload(
                channel="events",
                payload=events_payload,
                session_id=session.session_id,
                observed_at=observed_at,
                forced="events" in forced,
            )

        return frames
