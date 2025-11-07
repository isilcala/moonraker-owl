"""High-level orchestration for the telemetry pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Set

from ..config import TelemetryCadenceConfig
from ..version import __version__
from .events import EventCollector
from .selectors import EventsSelector, MetricsSelector, OverviewSelector
from .state_store import MoonrakerStateStore, MoonrakerStoreState
from .trackers import HeaterMonitor, PrintSessionTracker


@dataclass
class ChannelPayload:
    channel: str
    payload: Dict[str, Any]
    session_id: str
    observed_at: datetime
    forced: bool


class TelemetryOrchestrator:
    """Coordinates store, trackers, and selectors to emit channel payloads.

    The orchestrator is invoked from the telemetry publisher's asyncio event
    loop; callers must treat it as single-threaded unless future refactors add
    explicit locking.
    """

    def __init__(
        self,
        *,
        origin: Optional[str] = None,
        heartbeat_seconds: Optional[int] = None,
        clock=None,
        cadence: Optional[TelemetryCadenceConfig] = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._origin = origin or f"moonraker-owl@{__version__}"

        self._cadence = cadence or TelemetryCadenceConfig()
        heartbeat = (
            heartbeat_seconds
            if heartbeat_seconds is not None
            else self._cadence.overview_heartbeat_seconds
        )

        self.store = MoonrakerStateStore(clock=self._clock)
        self.session_tracker = PrintSessionTracker()
        self.heater_monitor = HeaterMonitor()
        self.events = EventCollector()

        self.overview_selector = OverviewSelector(heartbeat_seconds=heartbeat)
        self.metrics_selector = MetricsSelector()
        self.events_selector = EventsSelector(
            max_per_second=self._cadence.events_max_per_second,
            max_per_minute=self._cadence.events_max_per_minute,
        )

        self._metrics_mode = "idle"
        self._metrics_max_hz = 0.033
        self._watch_window_expires: Optional[datetime] = None

    def reset(self, *, snapshot: Optional[MoonrakerStoreState] = None) -> None:
        self.store = MoonrakerStateStore(clock=self._clock)
        if snapshot is not None:
            self.store.restore_state(snapshot)
        self.overview_selector.reset()
        self.metrics_selector.reset()
        self.events.reset()
        self.session_tracker = PrintSessionTracker()
        self.heater_monitor = HeaterMonitor()
        self._metrics_mode = "idle"
        self._metrics_max_hz = 0.033
        self._watch_window_expires = None

    def ingest(self, payload: Dict[str, Any]) -> None:
        self.store.ingest(payload)
        self.heater_monitor.refresh(self.store)

    def set_metrics_mode(
        self, *, mode: str, max_hz: float, watch_window_expires: Optional[datetime]
    ) -> None:
        self._metrics_mode = mode
        self._metrics_max_hz = max_hz
        self._watch_window_expires = watch_window_expires

    def set_telemetry_mode(
        self, *, mode: str, max_hz: float, watch_window_expires: Optional[datetime]
    ) -> None:
        """Backward-compatible wrapper for the legacy telemetry channel name."""

        self.set_metrics_mode(mode=mode, max_hz=max_hz, watch_window_expires=watch_window_expires)

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
        if "telemetry" in forced:
            forced.discard("telemetry")
            forced.add("metrics")
        session = self.session_tracker.compute(self.store)

        overview_payload = self.overview_selector.build(
            self.store,
            session,
            self.heater_monitor,
            observed_at,
        )
        metrics_payload = self.metrics_selector.build(
            self.store,
            mode=self._metrics_mode,
            max_hz=self._metrics_max_hz,
            watch_window_expires=self._watch_window_expires,
            observed_at=observed_at,
            force_emit="metrics" in forced,
        )
        events_payload = self.events_selector.build(
            events=self.events.drain(),
            observed_at=observed_at,
        )

        frames: Dict[str, ChannelPayload] = {}
        if overview_payload:
            cadence = overview_payload.setdefault("cadence", {})
            cadence["watchWindowActive"] = self._metrics_mode != "idle"
            overview_payload.setdefault("flags", {})["watchWindowActive"] = (
                self._metrics_mode != "idle"
            )
            frames["overview"] = ChannelPayload(
                channel="overview",
                payload=overview_payload,
                session_id=session.session_id,
                observed_at=observed_at,
                forced="overview" in forced,
            )

        if metrics_payload:
            frames["metrics"] = ChannelPayload(
                channel="metrics",
                payload=metrics_payload,
                session_id=session.session_id,
                observed_at=observed_at,
                forced="metrics" in forced,
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
