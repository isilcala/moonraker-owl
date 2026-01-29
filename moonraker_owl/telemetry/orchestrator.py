"""High-level orchestration for the telemetry pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Optional, Set, TYPE_CHECKING

from ..config import TelemetryCadenceConfig
from ..version import __version__
from .events import EventCollector
from .event_types import Event, EventName, PRINT_STATE_TRANSITIONS
from .selectors import EventsSelector, ObjectsSelector, SensorsSelector, StatusSelector
from .state_store import MoonrakerStateStore, MoonrakerStoreState
from .trackers import HeaterMonitor, PrintSessionTracker, SessionInfo

if TYPE_CHECKING:
    from ..core.job_registry import PrintJobRegistry

LOGGER = logging.getLogger(__name__)


@dataclass
class ChannelPayload:
    channel: str
    payload: Dict[str, Any]
    session_id: str
    observed_at: datetime
    forced: bool
    is_delta: bool = False  # When True, NexusService should merge with previous payload


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
        job_registry: Optional["PrintJobRegistry"] = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._origin = origin or f"moonraker-owl@{__version__}"
        self._job_registry = job_registry

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
        self.objects_selector = ObjectsSelector()
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
        self._last_session_id: Optional[str] = None  # Track session for new-job detection
        self._last_job_status: Optional[str] = None  # Track job_status for terminal events
        self._terminal_event_emitted: bool = False  # Prevent duplicate terminal events

        # Timelapse event deduplication
        self._last_timelapse_filename: Optional[str] = None

        # Track last completed job for timelapse correlation
        # When a job ends, we save its info so timelapse (which renders after job ends)
        # can still be correlated to the correct job
        self._last_completed_job_session_id: Optional[str] = None
        self._last_completed_job_filename: Optional[str] = None
        self._last_completed_moonraker_job_id: Optional[str] = None
        # Print timing from Moonraker history API (for timelapse:ready event)
        # These are set by TelemetryPublisher when print completes
        self._last_completed_print_start_time: Optional[float] = None  # Unix timestamp
        self._last_completed_print_end_time: Optional[float] = None  # Unix timestamp
        # Track moonraker_job_id while printing (before terminal state)
        self._last_moonraker_job_id: Optional[str] = None

        # Timelapse polling fallback: When render:running is detected but no
        # render:success arrives (moonraker-timelapse plugin bug), we poll for
        # new timelapse files to detect render completion.
        self._timelapse_poll_requested: bool = False
        self._timelapse_poll_started_at: Optional[datetime] = None
        self._timelapse_poll_abandoned: bool = False  # True after timeout, prevents restart
        self._timelapse_known_files: Set[str] = set()
        # Track the last processed timelapse event timestamp to avoid re-processing
        # the same event on every ingest() call (state store keeps events until replaced)
        self._last_timelapse_event_observed_at: Optional[datetime] = None

        # Deduplication for job update logging
        self._last_job_update_signature: Optional[tuple] = None

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

    def set_completed_job_timing(
        self, start_time: Optional[float], end_time: Optional[float]
    ) -> None:
        """Set print timing from Moonraker history API for timelapse correlation.

        Called by TelemetryPublisher after fetching job history when a print completes.
        These timestamps are included in the timelapse:ready event for time-window
        based job matching on the cloud side.

        Args:
            start_time: Unix timestamp when the print started (from history API)
            end_time: Unix timestamp when the print ended (from history API)
        """
        self._last_completed_print_start_time = start_time
        self._last_completed_print_end_time = end_time
        LOGGER.debug(
            "Set completed job timing: start_time=%s, end_time=%s",
            start_time,
            end_time,
        )

    # Note: set_thumbnail_url has been removed.
    # Thumbnail URLs are now pushed via SignalR after upload ACK processing.

    def reset(self, *, snapshot: Optional[MoonrakerStoreState] = None) -> None:
        self.store = MoonrakerStateStore(clock=self._clock)
        if snapshot is not None:
            self.store.restore_state(snapshot)
        self.status_selector.reset()
        self.sensors_selector.reset()
        self.objects_selector.reset()
        self.events.reset()
        self.session_tracker = PrintSessionTracker()
        self.heater_monitor = HeaterMonitor()
        self._sensors_mode = "idle"
        self._sensors_max_hz = 0.033
        self._watch_window_expires = None
        # Reset state tracking but preserve callbacks
        # When snapshot is provided (token renewal), initialize last states from
        # the restored store to prevent spurious events (e.g., print:started when
        # already printing). This is critical for token renewal during a print.
        if snapshot is not None:
            self._last_klippy_state = self.store.klippy_state
            self._last_print_state = self.store.print_state
            self._last_filename = self.store.print_filename
            # Compute session to get session_id and job_status
            session = self.session_tracker.compute(self.store)
            self._last_session_id = session.session_id
            self._last_job_status = (
                session.job_status.lower()
                if isinstance(session.job_status, str)
                else None
            )
            self._last_moonraker_job_id = session.moonraker_job_id
            # If we're in a terminal state, mark terminal event as already emitted
            # to prevent duplicate events after token renewal
            self._terminal_event_emitted = self._last_print_state in (
                "complete", "completed", "cancelled", "error"
            )
            LOGGER.debug(
                "Restored orchestrator state from snapshot: print_state=%s, "
                "klippy_state=%s, session_id=%s, terminal_emitted=%s",
                self._last_print_state,
                self._last_klippy_state,
                self._last_session_id,
                self._terminal_event_emitted,
            )
            # Preserve timelapse and job correlation state during token renewal
            # This is critical because timelapse renders AFTER print completion,
            # and we need to maintain polling state and job info across reconnection.
            # Note: All timelapse-related fields are preserved (not reset) here.
        else:
            self._last_klippy_state = None
            self._last_print_state = None
            self._last_filename = None
            self._last_session_id = None
            self._last_job_status = None
            self._terminal_event_emitted = False
            self._last_moonraker_job_id = None
            self._last_timelapse_filename = None
            # Reset completed job tracking (timelapse correlation)
            self._last_completed_job_session_id = None
            self._last_completed_job_filename = None
            self._last_completed_moonraker_job_id = None
            self._last_completed_print_start_time = None
            self._last_completed_print_end_time = None
            # Reset timelapse polling state
            self._timelapse_poll_requested = False
            self._timelapse_poll_started_at = None
            self._timelapse_poll_abandoned = False  # Allow polling for new jobs
            self._timelapse_known_files = set()
            self._last_timelapse_event_observed_at = None
        # Deduplication for job update logging
        self._last_job_update_signature: Optional[tuple] = None
        # Note: _on_print_state_changed is preserved

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

        # Log job progress at DEBUG level (deduplicated by progress/layer/state changes)
        if session.has_active_job:
            progress_int = int(session.progress_percent) if session.progress_percent is not None else 0
            job_signature = (progress_int, session.layer_current, session.layer_total, session.raw_state, session.session_id)
            if job_signature != self._last_job_update_signature:
                self._last_job_update_signature = job_signature
                if LOGGER.isEnabledFor(logging.DEBUG):
                    layer_info = ""
                    if session.layer_current is not None and session.layer_total is not None:
                        layer_info = f" layer={session.layer_current}/{session.layer_total}"
                    LOGGER.debug(
                        "Job: %d%%%s state=%s session=%s",
                        progress_int,
                        layer_info,
                        session.raw_state,
                        session.session_id[:8] if session.session_id else "none",
                    )
        else:
            # Clear signature when job ends
            self._last_job_update_signature = None

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
        objects_payload = self.objects_selector.build(
            self.store,
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

        if objects_payload:
            frames["objects"] = ChannelPayload(
                channel="objects",
                payload=objects_payload.payload,
                session_id=session.session_id,
                observed_at=observed_at,
                forced="objects" in forced,
                is_delta=objects_payload.is_delta,
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
        self._detect_timelapse_ready()

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

        Detection sources (in order of priority):
        1. job_status transitions to terminal states (completed/cancelled/error)
           - This is the most reliable source since history_event is updated
             even when print_stats.state doesn't transition properly
        2. print_stats.state transitions defined in PRINT_STATE_TRANSITIONS

        Aligned with Mainsail's approach: use print_stats.state for print lifecycle,
        use history_event only for terminal status confirmation.
        """
        current_state = self.store.print_state
        filename = self.store.print_filename

        # Track filename for events
        if filename:
            self._last_filename = filename

        # Compute session info (includes job_status from history_event)
        session = self.session_tracker.compute(self.store)
        current_session_id = session.session_id
        current_job_status = (
            session.job_status.lower()
            if isinstance(session.job_status, str)
            else None
        )

        # === Priority 1: Detect terminal events via job_status ===
        # This catches cases where print_stats.state stays 'printing' but
        # the job has actually ended (history_event updates first)
        terminal_event = self._detect_job_status_terminal(
            current_job_status, session
        )
        if terminal_event:
            self._last_job_status = current_job_status
            self._last_print_state = current_state
            if current_session_id:
                self._last_session_id = current_session_id
            if session.moonraker_job_id:
                self._last_moonraker_job_id = session.moonraker_job_id
            return

        # Update job_status tracking
        self._last_job_status = current_job_status

        # Update session tracking
        if current_session_id:
            self._last_session_id = current_session_id
        # Track moonraker_job_id while printing for timelapse correlation
        if session.moonraker_job_id:
            self._last_moonraker_job_id = session.moonraker_job_id

        # === Priority 2: Standard print_stats.state-based detection ===
        # Aligned with Mainsail: only use print_stats.state transitions
        if current_state == self._last_print_state:
            return

        old_state = self._last_print_state
        self._last_print_state = current_state

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

        # === Guard: Don't emit PRINT_STARTED/PRINT_RESUMED during klippy shutdown ===
        # When klippy is shutdown/error, Moonraker may return stale print_stats.state
        # (e.g., "printing") from HTTP queries, but this is not a real state change.
        # Only allow terminal events (PRINT_FAILED) during shutdown.
        klippy_state = self.store.klippy_state
        is_klippy_down = klippy_state in ("shutdown", "error")
        is_start_or_resume = event_name in {
            EventName.PRINT_STARTED,
            EventName.PRINT_RESUMED,
        }
        if is_klippy_down and is_start_or_resume:
            return

        # Check if this is a terminal event and if we've already emitted one
        # This prevents duplicate events when both print_stats.state and
        # history_event.job.status transition to terminal states
        is_terminal = event_name in {
            EventName.PRINT_COMPLETED,
            EventName.PRINT_CANCELLED,
            EventName.PRINT_FAILED,
        }
        if is_terminal:
            if self._terminal_event_emitted:
                return
            # Mark terminal event as emitted
            self._terminal_event_emitted = True
            # Save job info for timelapse correlation
            # Timelapse renders after print ends, so we need to remember the job.
            # IMPORTANT: session.session_id may already be None when state becomes
            # terminal (session_tracker returns empty session for terminal states).
            # Use _last_session_id as fallback - it was saved while still printing.
            effective_session = session.session_id or self._last_session_id
            # Use moonraker_job_id from session or fallback to _last_moonraker_job_id
            effective_job_id = session.moonraker_job_id or self._last_moonraker_job_id
            if effective_session or effective_job_id:
                self._last_completed_job_session_id = effective_session
                self._last_completed_job_filename = self._last_filename
                self._last_completed_moonraker_job_id = effective_job_id
                LOGGER.debug(
                    "Saved completed job info for timelapse (via state): session_id=%s, moonraker_job_id=%s, filename=%s",
                    effective_session,
                    effective_job_id,
                    self._last_filename,
                )

            # Proactively start timelapse polling after SUCCESSFUL print completion only.
            # moonraker-timelapse only renders when print completes successfully,
            # so we don't poll for cancelled/failed prints.
            if (
                event_name == EventName.PRINT_COMPLETED
                and not self._timelapse_poll_requested
                and not self._timelapse_poll_abandoned
            ):
                LOGGER.info(
                    "Print completed successfully (via state) - enabling proactive timelapse polling"
                )
                self._timelapse_poll_requested = True
                self._timelapse_poll_started_at = self._clock()
        elif event_name == EventName.PRINT_STARTED:
            # Reset terminal event flag when a new print starts
            self._terminal_event_emitted = False

        # Build event with context data
        event = Event(
            event_name=event_name,
            message=self._build_print_event_message(event_name),
            session_id=session.session_id,
            data=self._build_print_event_data(event_name, session),
        )

        self.events.record(event)

    def _detect_job_status_terminal(
        self,
        current_job_status: Optional[str],
        session: "SessionInfo",
    ) -> bool:
        """Detect terminal events via job_status transition.

        This is the primary detection method for print completion since
        history_event.status updates reliably even when print_stats.state
        doesn't transition as expected.

        Args:
            current_job_status: Current job_status from history_event (lowercase)
            session: Current session info

        Returns:
            True if a terminal event was detected and recorded
        """
        if not current_job_status:
            return False

        # Skip if job_status hasn't changed
        if current_job_status == self._last_job_status:
            return False

        # Map job_status to event names
        job_status_events = {
            "completed": EventName.PRINT_COMPLETED,
            "complete": EventName.PRINT_COMPLETED,
            "cancelled": EventName.PRINT_CANCELLED,
            "canceled": EventName.PRINT_CANCELLED,
            "error": EventName.PRINT_FAILED,
        }

        event_name = job_status_events.get(current_job_status)
        if not event_name:
            return False

        # Check if we've already emitted a terminal event
        # This prevents duplicate events when both print_stats.state and
        # history_event.job.status transition to terminal states
        if self._terminal_event_emitted:
            return False

        # Only emit if we were previously in an active state or unknown (None)
        # None -> terminal is valid (first completion after agent start or during active print)
        active_states = {"in_progress", "printing", "paused"}
        was_active = self._last_job_status in active_states

        if not was_active and self._last_job_status is not None:
            # If we weren't in an active state, skip (e.g., startup recovery)
            return False

        LOGGER.info(
            "Print terminal event detected via job_status: %s -> %s (file=%s)",
            self._last_job_status,
            current_job_status,
            self._last_filename,
        )

        # Mark terminal event as emitted
        self._terminal_event_emitted = True

        # Save job info for timelapse correlation
        # Timelapse renders after print ends, so we need to remember the job.
        # IMPORTANT: session.session_id may already be None when state becomes
        # terminal (session_tracker returns empty session for terminal states).
        # Use _last_session_id as fallback - it was saved while still printing.
        effective_session = session.session_id or self._last_session_id
        # Use moonraker_job_id from session or fallback to _last_moonraker_job_id
        effective_job_id = session.moonraker_job_id or self._last_moonraker_job_id
        if effective_session or effective_job_id:
            self._last_completed_job_session_id = effective_session
            self._last_completed_job_filename = self._last_filename
            self._last_completed_moonraker_job_id = effective_job_id
            LOGGER.debug(
                "Saved completed job info for timelapse (via job_status): session_id=%s, moonraker_job_id=%s, filename=%s",
                effective_session,
                effective_job_id,
                self._last_filename,
            )

        # Proactively start timelapse polling after SUCCESSFUL print completion only.
        # moonraker-timelapse only renders when print completes successfully,
        # so we don't poll for cancelled/failed prints.
        if (
            event_name == EventName.PRINT_COMPLETED
            and not self._timelapse_poll_requested
            and not self._timelapse_poll_abandoned
        ):
            LOGGER.info(
                "Print completed successfully (via job_status) - enabling proactive timelapse polling"
            )
            self._timelapse_poll_requested = True
            self._timelapse_poll_started_at = self._clock()

        event = Event(
            event_name=event_name,
            message=self._build_print_event_message(event_name),
            session_id=session.session_id,
            data=self._build_print_event_data(event_name, session),
        )
        self.events.record(event)
        return True


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
        
        # Always include moonrakerJobId if available (critical for job correlation)
        if session.moonraker_job_id:
            base_data["moonrakerJobId"] = session.moonraker_job_id

        if event_name == EventName.PRINT_STARTED:
            if session.remaining_seconds is not None:
                base_data["estimatedDuration"] = session.remaining_seconds
            # Note: hasThumbnail is added by TelemetryPublisher after async metadata fetch
            return base_data

        if event_name == EventName.PRINT_COMPLETED:
            if session.elapsed_seconds is not None:
                base_data["printDuration"] = session.elapsed_seconds
            filament_used = print_data.get("filament_used")
            if filament_used is not None:
                base_data["filamentUsedMm"] = filament_used
            # For completed prints, always report 100% progress regardless of what
            # Moonraker reports. The print_stats.state transition to "complete"
            # is the authoritative signal that the print finished successfully.
            base_data["progressPercent"] = 100.0
            return base_data

        if event_name == EventName.PRINT_FAILED:
            if session.elapsed_seconds is not None:
                base_data["printDuration"] = session.elapsed_seconds
            if session.progress_percent is not None:
                base_data["progressPercent"] = session.progress_percent
            filament_used = print_data.get("filament_used")
            if filament_used is not None:
                base_data["filamentUsedMm"] = filament_used
            error_message = print_data.get("message", "")
            if error_message:
                base_data["errorMessage"] = error_message
            return base_data

        if event_name == EventName.PRINT_CANCELLED:
            if session.elapsed_seconds is not None:
                base_data["printDuration"] = session.elapsed_seconds
            if session.progress_percent is not None:
                base_data["progressPercent"] = session.progress_percent
            filament_used = print_data.get("filament_used")
            if filament_used is not None:
                base_data["filamentUsedMm"] = filament_used
            return base_data

        if event_name in (EventName.PRINT_PAUSED, EventName.PRINT_RESUMED):
            if session.progress_percent is not None:
                base_data["progressPercent"] = session.progress_percent
            return base_data

        return base_data

    def _detect_timelapse_ready(self) -> None:
        """Detect timelapse render completion from moonraker-timelapse.

        moonraker-timelapse sends notify_timelapse_event when:
        - action=newframe: Frame captured (we ignore this)
        - action=render, status=started: Render began (we ignore this)
        - action=render, status=running: Render in progress (start polling fallback)
        - action=render, status=success: Render completed (we emit TIMELAPSE_READY)
        - action=saveframes: Frames saved to zip (we ignore this)

        When render completes successfully, we emit TIMELAPSE_READY event with:
        - filename: The rendered video filename (e.g., "timelapse_xxx.mp4")
        - previewImage: The preview image filename (e.g., "timelapse_xxx.jpg")
        - printFile: The original gcode filename
        - moonrakerJobId: The Moonraker job ID for correlation
        - printJobId: The cloud-side PrintJob ID (if available from registry)

        FALLBACK: moonraker-timelapse sometimes fails to send render:success.
        When we detect render:started or render:running, we start polling the 
        timelapse file list to detect when a new .mp4 file appears.
        """
        timelapse_event = self.store.get("timelapse_event")
        if timelapse_event is None:
            return

        # Skip if we already processed this exact event (same observed_at timestamp).
        # The state store keeps events until replaced by a new one, so the same
        # event would be re-read on every ingest() call without this check.
        if timelapse_event.observed_at == self._last_timelapse_event_observed_at:
            return
        self._last_timelapse_event_observed_at = timelapse_event.observed_at

        event_data = timelapse_event.data
        action = event_data.get("action")
        status = event_data.get("status")
        filename = event_data.get("filename")

        # When render starts or is running, request polling fallback
        # Some moonraker-timelapse versions skip "running" and go straight from
        # "started" to completion without sending "success" notification.
        # We start polling as early as possible to catch the new file.
        if action == "render" and status in ("started", "running"):
            # Don't restart polling if it was abandoned due to timeout
            if self._timelapse_poll_abandoned:
                return
            if not self._timelapse_poll_requested:
                LOGGER.info(
                    "Timelapse render %s, enabling polling fallback for completion detection",
                    status,
                )
                self._timelapse_poll_requested = True
                self._timelapse_poll_started_at = self._clock()
            return

        # Only process successful render completions
        if action != "render" or status != "success":
            return

        # Disable polling since we got the success event
        self._timelapse_poll_requested = False
        self._timelapse_poll_started_at = None
        self._timelapse_poll_abandoned = False  # Reset for next timelapse

        # Deduplicate: skip if we already processed this timelapse
        if filename and filename == self._last_timelapse_filename:
            return

        self._last_timelapse_filename = filename

        # Get current session info - session_id may be None if print already ended
        session = self.session_tracker.compute(self.store)

        # Get moonraker_job_id directly from session or fallback to saved value
        # The saved value comes from when the print was still active
        moonraker_job_id: Optional[str] = (
            session.moonraker_job_id or self._last_completed_moonraker_job_id
        )

        # Look up cloud-side PrintJobId from registry
        # The registry is populated by job:registered commands from the server
        print_job_id: Optional[str] = None
        # Use print_file from timelapse event, fallback to last completed job's filename
        print_file = event_data.get("printfile") or self._last_completed_job_filename

        if self._job_registry:
            # Try to find by filename first (most reliable for post-print events)
            if print_file:
                mapping = self._job_registry.find_by_filename(print_file)
                if mapping:
                    print_job_id = mapping.print_job_id
                    # Also get moonraker_job_id from mapping if we didn't have it
                    if not moonraker_job_id:
                        moonraker_job_id = mapping.moonraker_job_id
                    LOGGER.debug(
                        "Found PrintJob mapping by filename '%s': printJobId=%s",
                        print_file, print_job_id,
                    )

            # Fallback: try by moonraker_job_id
            if not print_job_id and moonraker_job_id:
                mapping = self._job_registry.find_by_moonraker_job_id(moonraker_job_id)
                if mapping:
                    print_job_id = mapping.print_job_id
                    LOGGER.debug(
                        "Found PrintJob mapping by moonrakerJobId '%s': printJobId=%s",
                        moonraker_job_id, print_job_id,
                    )

        # Build event data
        data: Dict[str, Any] = {}
        if filename:
            data["filename"] = filename
        preview_image = event_data.get("previewimage")
        if preview_image:
            data["previewImage"] = preview_image
        if print_file:
            data["printFile"] = print_file
        if moonraker_job_id:
            data["moonrakerJobId"] = moonraker_job_id
        if print_job_id:
            data["printJobId"] = print_job_id
        # Include print timing for cloud-side time-window job matching
        if self._last_completed_print_start_time is not None:
            data["printStartTime"] = self._last_completed_print_start_time
        if self._last_completed_print_end_time is not None:
            data["printEndTime"] = self._last_completed_print_end_time

        LOGGER.info(
            "Timelapse render complete: %s (printJobId=%s, moonrakerJobId=%s, "
            "startTime=%s, endTime=%s)",
            filename,
            print_job_id or "none",
            moonraker_job_id or "none",
            self._last_completed_print_start_time,
            self._last_completed_print_end_time,
        )

        event = Event(
            event_name=EventName.TIMELAPSE_READY,
            message=f"Timelapse ready: {filename or 'unknown'}",
            session_id=session.session_id,
            data=data,
        )
        self.events.record(event)

    def should_poll_timelapse(self) -> bool:
        """Check if timelapse polling is requested.

        Returns True if we detected render:running but haven't received
        render:success yet, and polling hasn't timed out (5 minutes max).
        """
        if not self._timelapse_poll_requested:
            return False

        # Timeout after 5 minutes to avoid infinite polling
        if self._timelapse_poll_started_at:
            elapsed = (self._clock() - self._timelapse_poll_started_at).total_seconds()
            if elapsed > 300:  # 5 minutes
                LOGGER.warning(
                    "Timelapse polling timed out after %.0f seconds - abandoning",
                    elapsed,
                )
                self._timelapse_poll_requested = False
                self._timelapse_poll_started_at = None
                self._timelapse_poll_abandoned = True  # Prevent restart from render:running events
                return False

        return True

    def set_known_timelapse_files(self, files: Set[str]) -> None:
        """Set the baseline of known timelapse files for polling detection.

        Called by TelemetryPublisher when starting timelapse polling to
        establish the baseline of existing files.
        """
        self._timelapse_known_files = files

    def check_new_timelapse_file(self, current_files: Set[str]) -> Optional[str]:
        """Check if a new timelapse mp4 file has appeared.

        Compares current files against known files to detect new videos.
        Returns the filename of the new mp4 file if found, None otherwise.
        """
        new_files = current_files - self._timelapse_known_files
        # Look for new .mp4 files (timelapse videos)
        new_videos = [f for f in new_files if f.lower().endswith(".mp4")]

        if new_videos:
            # Return the most recently modified one (by filename, as they include timestamp)
            new_video = sorted(new_videos)[-1]
            LOGGER.info(
                "Timelapse polling detected new video: %s (known=%d, current=%d)",
                new_video,
                len(self._timelapse_known_files),
                len(current_files),
            )
            return new_video

        return None

    def emit_timelapse_from_polling(self, filename: str) -> None:
        """Emit TIMELAPSE_READY event from polling detection.

        Called when polling detects a new timelapse video file,
        bypassing the unreliable notify_timelapse_event mechanism.
        """
        # Stop polling and reset all timelapse state
        self._timelapse_poll_requested = False
        self._timelapse_poll_started_at = None
        self._timelapse_poll_abandoned = False  # Reset for next timelapse

        # Deduplicate
        if filename == self._last_timelapse_filename:
            return

        self._last_timelapse_filename = filename
        self._timelapse_known_files.add(filename)

        # Get session info for job correlation
        session = self.session_tracker.compute(self.store)

        # Get moonraker_job_id directly from session or fallback to saved value
        moonraker_job_id: Optional[str] = (
            session.moonraker_job_id or self._last_completed_moonraker_job_id
        )

        # Look up cloud-side PrintJobId from registry
        print_job_id: Optional[str] = None
        print_file = self._last_completed_job_filename

        if self._job_registry:
            if print_file:
                mapping = self._job_registry.find_by_filename(print_file)
                if mapping:
                    print_job_id = mapping.print_job_id
                    if not moonraker_job_id:
                        moonraker_job_id = mapping.moonraker_job_id

            if not print_job_id and moonraker_job_id:
                mapping = self._job_registry.find_by_moonraker_job_id(moonraker_job_id)
                if mapping:
                    print_job_id = mapping.print_job_id

        # Build event data
        data: Dict[str, Any] = {"filename": filename}

        # Derive preview image filename (same name but .jpg extension)
        if filename.lower().endswith(".mp4"):
            preview_image = filename[:-4] + ".jpg"
            data["previewImage"] = preview_image

        if print_file:
            data["printFile"] = print_file
        if moonraker_job_id:
            data["moonrakerJobId"] = moonraker_job_id
        if print_job_id:
            data["printJobId"] = print_job_id
        # Include print timing for cloud-side time-window job matching
        if self._last_completed_print_start_time is not None:
            data["printStartTime"] = self._last_completed_print_start_time
        if self._last_completed_print_end_time is not None:
            data["printEndTime"] = self._last_completed_print_end_time

        LOGGER.info(
            "Timelapse detected via polling: %s (printJobId=%s, moonrakerJobId=%s, "
            "startTime=%s, endTime=%s)",
            filename,
            print_job_id or "none",
            moonraker_job_id or "none",
            self._last_completed_print_start_time,
            self._last_completed_print_end_time,
        )

        event = Event(
            event_name=EventName.TIMELAPSE_READY,
            message=f"Timelapse ready: {filename}",
            session_id=session.session_id,
            data=data,
        )
        self.events.record(event)

    def _build_alert_events_payload(
        self, events: list[Event]
    ) -> Dict[str, Any]:
        """Build events channel payload from Event objects."""
        return {"items": [e.to_dict() for e in events]}
