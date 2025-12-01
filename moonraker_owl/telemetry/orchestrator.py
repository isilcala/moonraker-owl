"""High-level orchestration for the telemetry pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Optional, Set

from ..config import TelemetryCadenceConfig
from ..version import __version__
from .events import EventCollector
from .event_types import Event, EventName, PRINT_STATE_TRANSITIONS
from .selectors import EventsSelector, SensorsSelector, StatusSelector
from .state_store import MoonrakerStateStore, MoonrakerStoreState
from .trackers import HeaterMonitor, PrintSessionTracker, SessionInfo

LOGGER = logging.getLogger(__name__)


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
            else self._cadence.status_heartbeat_seconds
        )

        self.store = MoonrakerStateStore(clock=self._clock)
        self.session_tracker = PrintSessionTracker()
        self.heater_monitor = HeaterMonitor()
        self.events = EventCollector()

        self.status_selector = StatusSelector(heartbeat_seconds=heartbeat)
        self.sensors_selector = SensorsSelector()
        self.events_selector = EventsSelector(
            max_per_second=self._cadence.events_max_per_second,
            max_per_minute=self._cadence.events_max_per_minute,
        )

        self._sensors_mode = "idle"
        self._sensors_max_hz = 0.033
        self._watch_window_expires: Optional[datetime] = None

        # State tracking for event detection
        self._last_klippy_state: Optional[str] = None
        self._last_print_state: Optional[str] = None
        self._last_filename: Optional[str] = None

        # Callback for print state changes (used by CommandProcessor)
        self._on_print_state_changed: Optional[Callable[[str], None]] = None

    def set_print_state_callback(
        self, callback: Optional[Callable[[str], None]]
    ) -> None:
        """Set callback for print state changes.

        This callback is invoked whenever the print state changes, allowing
        the CommandProcessor to detect when commands have been executed.

        Args:
            callback: Function accepting the new print state string.
        """
        self._on_print_state_changed = callback

    def reset(self, *, snapshot: Optional[MoonrakerStoreState] = None) -> None:
        self.store = MoonrakerStateStore(clock=self._clock)
        if snapshot is not None:
            self.store.restore_state(snapshot)
        self.status_selector.reset()
        self.sensors_selector.reset()
        self.events.reset()
        self.session_tracker = PrintSessionTracker()
        self.heater_monitor = HeaterMonitor()
        self._sensors_mode = "idle"
        self._sensors_max_hz = 0.033
        self._watch_window_expires = None
        # Reset state tracking but preserve callback
        self._last_klippy_state = None
        self._last_print_state = None
        self._last_filename = None

    def ingest(self, payload: Dict[str, Any]) -> None:
        """Ingest Moonraker payload and detect events.

        This method:
        1. Updates the state store with new data
        2. Refreshes the heater monitor
        3. Detects state changes and generates events

        Args:
            payload: Moonraker websocket message (result or notification)
        """
        self.store.ingest(payload)
        self.heater_monitor.refresh(self.store)

        # Detect state changes and generate events
        self._detect_state_changes()

    def set_sensors_mode(
        self, *, mode: str, max_hz: float, watch_window_expires: Optional[datetime]
    ) -> None:
        self._sensors_mode = mode
        self._sensors_max_hz = max_hz
        self._watch_window_expires = watch_window_expires

    def set_telemetry_mode(
        self, *, mode: str, max_hz: float, watch_window_expires: Optional[datetime]
    ) -> None:
        """Backward-compatible wrapper for the legacy telemetry channel name."""

        self.set_sensors_mode(mode=mode, max_hz=max_hz, watch_window_expires=watch_window_expires)

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
            forced.add("sensors")
        session = self.session_tracker.compute(self.store)

        # Log job progress at INFO level for debugging print lifecycle
        if session.has_active_job:
            progress_int = int(session.progress_percent) if session.progress_percent is not None else 0
            layer_info = ""
            if session.layer_current is not None and session.layer_total is not None:
                layer_info = f" layer={session.layer_current}/{session.layer_total}"
            LOGGER.info(
                "JOB_UPDATE: progress=%d%%%s state=%s session=%s",
                progress_int,
                layer_info,
                session.raw_state,
                session.session_id[:8] if session.session_id else "none",
            )

        status_payload = self.status_selector.build(
            self.store,
            session,
            self.heater_monitor,
            observed_at,
        )
        sensors_payload = self.sensors_selector.build(
            self.store,
            mode=self._sensors_mode,
            max_hz=self._sensors_max_hz,
            watch_window_expires=self._watch_window_expires,
            observed_at=observed_at,
            force_emit="sensors" in forced,
        )
        events_payload = self.events_selector.build(
            events=self.events.drain(),
            observed_at=observed_at,
        )

        frames: Dict[str, ChannelPayload] = {}
        if status_payload:
            cadence = status_payload.setdefault("cadence", {})
            cadence["watchWindowActive"] = self._sensors_mode != "idle"
            status_payload.setdefault("flags", {})["watchWindowActive"] = (
                self._sensors_mode != "idle"
            )
            frames["status"] = ChannelPayload(
                channel="status",
                payload=status_payload,
                session_id=session.session_id,
                observed_at=observed_at,
                forced="status" in forced,
            )

        if sensors_payload:
            frames["sensors"] = ChannelPayload(
                channel="sensors",
                payload=sensors_payload,
                session_id=session.session_id,
                observed_at=observed_at,
                forced="sensors" in forced,
            )

        if events_payload:
            frames["events"] = ChannelPayload(
                channel="events",
                payload=events_payload,
                session_id=session.session_id,
                observed_at=observed_at,
                forced="events" in forced,
            )

        # Harvest alert events (P0/P1 lifecycle) and add to events channel.
        # Events channel bypasses cadence controller entirely - it is purely
        # event-driven with rate limiting handled by EventCollector's token bucket.
        alert_events = self.events.harvest()
        if alert_events:
            alert_payload = self._build_alert_events_payload(alert_events)
            # Merge with existing events or create new channel payload
            if "events" in frames:
                # Extend existing events items with alert events
                existing_items = frames["events"].payload.get("items", [])
                existing_items.extend(alert_payload.get("items", []))
                frames["events"].payload["items"] = existing_items
            else:
                frames["events"] = ChannelPayload(
                    channel="events",
                    payload=alert_payload,
                    session_id=session.session_id,
                    observed_at=observed_at,
                    forced=False,  # No need to force - events bypass cadence entirely
                )

        return frames

    # -------------------------------------------------------------------------
    # Event Detection Methods
    # -------------------------------------------------------------------------

    def _detect_state_changes(self) -> None:
        """Detect state changes from store and record events.

        This method reads from the already-populated MoonrakerStateStore,
        avoiding duplicate parsing of Moonraker data.
        """
        self._detect_klippy_state_change()
        self._detect_print_state_change()

    def _detect_klippy_state_change(self) -> None:
        """Detect Klippy state transitions and record P0 events.

        Monitors webhooks.state for:
        - ready -> error: klippyError event
        - ready -> shutdown: klippyShutdown event
        - ready -> startup/None: klippyDisconnected event
        - error/shutdown/startup -> ready: klippyReady event

        Includes debouncing to prevent rapid duplicate events during
        Moonraker's state oscillations (e.g., shutdown -> error -> shutdown).
        """
        klippy_state = self.store.klippy_state
        state_message = self.store.klippy_state_message or ""

        if klippy_state == self._last_klippy_state:
            return

        old_state = self._last_klippy_state
        self._last_klippy_state = klippy_state

        LOGGER.info("Klippy state changed: %s -> %s", old_state, klippy_state)

        event: Optional[Event] = None

        if klippy_state == "error":
            event = Event(
                event_name=EventName.KLIPPY_ERROR,
                message=(
                    f"Klippy error: {state_message}"
                    if state_message
                    else "Klippy entered error state"
                ),
                data={"stateMessage": state_message} if state_message else {},
            )
        elif klippy_state == "shutdown":
            event = Event(
                event_name=EventName.KLIPPY_SHUTDOWN,
                message=(
                    f"Klippy shutdown: {state_message}"
                    if state_message
                    else "Klippy shutdown"
                ),
                data={"stateMessage": state_message} if state_message else {},
            )
        elif old_state == "ready" and klippy_state in ("startup", None):
            event = Event(
                event_name=EventName.KLIPPY_DISCONNECTED,
                message="Klippy disconnected",
                data={"previousState": old_state} if old_state else {},
            )
        elif klippy_state == "ready" and old_state in ("error", "shutdown", "startup"):
            # Klippy has recovered from an error/shutdown/startup state
            event = Event(
                event_name=EventName.KLIPPY_READY,
                message="Klippy ready",
                data={"previousState": old_state} if old_state else {},
            )

        if event:
            self.events.record(event)

    def _detect_print_state_change(self) -> None:
        """Detect print state transitions and record P1 events.

        Monitors print_stats.state for transitions defined in
        PRINT_STATE_TRANSITIONS mapping.
        """
        current_state = self.store.print_state
        filename = self.store.print_filename

        if current_state == self._last_print_state:
            return

        old_state = self._last_print_state
        self._last_print_state = current_state

        # Track filename for events
        if filename:
            self._last_filename = filename

        LOGGER.info(
            "STATE_CHANGE: %s -> %s (file: %s)",
            old_state,
            current_state,
            self._last_filename,
        )

        # Notify command processor of state change
        if self._on_print_state_changed and current_state:
            try:
                self._on_print_state_changed(current_state)
            except Exception:
                LOGGER.exception("Error in print state callback")

        # Look up event type from transition
        event_name = PRINT_STATE_TRANSITIONS.get((old_state, current_state))
        if not event_name:
            return

        # Get current session info for context
        session = self.session_tracker.compute(self.store)

        # Build event with context data
        event = Event(
            event_name=event_name,
            message=self._build_print_event_message(event_name),
            session_id=session.session_id,
            data=self._build_print_event_data(event_name, session),
        )

        self.events.record(event)

    def _build_print_event_message(self, event_name: EventName) -> str:
        """Build human-readable event message."""
        name = self._last_filename or "Unknown file"
        messages = {
            EventName.PRINT_STARTED: f"Print started: {name}",
            EventName.PRINT_COMPLETED: f"Print completed: {name}",
            EventName.PRINT_FAILED: f"Print failed: {name}",
            EventName.PRINT_CANCELLED: f"Print cancelled: {name}",
            EventName.PRINT_PAUSED: f"Print paused: {name}",
            EventName.PRINT_RESUMED: f"Print resumed: {name}",
        }
        return messages.get(event_name, f"Print event: {name}")

    def _build_print_event_data(
        self,
        event_name: EventName,
        session: SessionInfo,
    ) -> Dict[str, Any]:
        """Build event-specific data payload using session info."""
        print_stats = self.store.get("print_stats")
        print_data = print_stats.data if print_stats else {}

        base_data: Dict[str, Any] = {}
        if self._last_filename:
            base_data["filename"] = self._last_filename

        if event_name == EventName.PRINT_STARTED:
            if session.remaining_seconds is not None:
                base_data["estimatedDuration"] = session.remaining_seconds
            return base_data

        if event_name == EventName.PRINT_COMPLETED:
            if session.elapsed_seconds is not None:
                base_data["printDuration"] = session.elapsed_seconds
            filament_used = print_data.get("filament_used")
            if filament_used is not None:
                base_data["filamentUsed"] = filament_used
            return base_data

        if event_name == EventName.PRINT_FAILED:
            if session.elapsed_seconds is not None:
                base_data["printDuration"] = session.elapsed_seconds
            if session.progress_percent is not None:
                base_data["progress"] = session.progress_percent
            error_message = print_data.get("message", "")
            if error_message:
                base_data["errorMessage"] = error_message
            return base_data

        if event_name == EventName.PRINT_CANCELLED:
            if session.elapsed_seconds is not None:
                base_data["printDuration"] = session.elapsed_seconds
            if session.progress_percent is not None:
                base_data["progress"] = session.progress_percent
            return base_data

        if event_name in (EventName.PRINT_PAUSED, EventName.PRINT_RESUMED):
            if session.progress_percent is not None:
                base_data["progress"] = session.progress_percent
            return base_data

        return base_data

    def _build_alert_events_payload(
        self, events: list[Event]
    ) -> Dict[str, Any]:
        """Build events channel payload from Event objects."""
        return {"items": [e.to_dict() for e in events]}
