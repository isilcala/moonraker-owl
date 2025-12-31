"""Moonraker state store for the telemetry pipeline."""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Sequence

from ..core import deep_merge

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SectionSnapshot:
    """Immutable snapshot of a Moonraker section update."""

    name: str
    observed_at: datetime
    data: Mapping[str, Any]


@dataclass(frozen=True)
class MoonrakerStoreState:
    """Serializable view of the state store used for hand-offs.
    
    Simplified to track only the essential shutdown state. The shutdown_active
    flag indicates whether Klippy is in a non-ready state (shutdown, error, 
    disconnected). We trust Moonraker notifications directly rather than trying
    to infer state from multiple sources.
    """

    sections: Dict[str, SectionSnapshot]
    shutdown_active: bool
    latest_observed_at: Optional[datetime]


class MoonrakerStateStore:
    """Caches the latest Moonraker state per section.
    
    This store follows the moonraker-obico pattern of trusting Moonraker
    notifications directly. When we receive notify_klippy_shutdown or
    notify_klippy_disconnected, we mark the store as shutdown. When we
    receive notify_klippy_ready, we clear the shutdown state.
    
    We do NOT try to infer klippy state from print_stats, gcode responses,
    or other indirect sources. This keeps the logic simple and predictable.
    """

    def __init__(self, *, clock: Optional[Callable[[], datetime]] = None) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sections: Dict[str, SectionSnapshot] = {}
        self._latest_observed_at: Optional[datetime] = None
        self._shutdown_active = False

    def ingest(self, payload: Mapping[str, Any]) -> None:
        observed_at = self._clock()

        result = payload.get("result") if isinstance(payload, Mapping) else None
        if isinstance(result, Mapping):
            status = result.get("status")
            self._ingest_status(status, observed_at)

        method = payload.get("method") if isinstance(payload, Mapping) else None
        if isinstance(method, str):
            params = payload.get("params")
            self._ingest_notification(method, params, observed_at)

    def get(self, name: str) -> Optional[SectionSnapshot]:
        return self._sections.get(name)

    def iter_sections(self) -> Iterator[SectionSnapshot]:
        return iter(self._sections.values())

    def as_dict(self) -> Dict[str, Mapping[str, Any]]:
        return {name: snapshot.data for name, snapshot in self._sections.items()}

    def latest_observed_at(self) -> Optional[datetime]:
        return self._latest_observed_at

    @property
    def klippy_state(self) -> Optional[str]:
        """Get the current Klippy state from webhooks section.

        Returns:
            The current Klippy state (e.g., 'ready', 'shutdown', 'error')
            or None if webhooks data is not available.
        """
        webhooks = self.get("webhooks")
        if webhooks is None:
            return None
        return webhooks.data.get("state")

    @property
    def klippy_state_message(self) -> Optional[str]:
        """Get the current Klippy state message.

        The state message provides additional context for shutdown/error states,
        such as the specific error that caused the shutdown.

        Returns:
            The state message string, or None if not available.
        """
        webhooks = self.get("webhooks")
        if webhooks is None:
            return None
        return webhooks.data.get("state_message")

    @property
    def print_state(self) -> Optional[str]:
        """Get the current print state from print_stats section.

        Returns:
            The current print state normalized to lowercase
            (e.g., 'standby', 'printing', 'paused', 'complete', 'cancelled', 'error')
            or None if not available.
            
        Note:
            Klipper officially uses lowercase states, but we normalize defensively
            to ensure consistent matching with PRINT_STATE_TRANSITIONS lookups.
        """
        print_stats = self.get("print_stats")
        if print_stats is None:
            return None
        raw_state = print_stats.data.get("state")
        if raw_state is None:
            return None
        # Normalize to lowercase for consistent PRINT_STATE_TRANSITIONS matching
        return raw_state.strip().lower() if isinstance(raw_state, str) else None

    @property
    def print_filename(self) -> Optional[str]:
        """Get the current print filename from print_stats section.

        Returns:
            The filename of the current/last print, or None if not available.
        """
        print_stats = self.get("print_stats")
        if print_stats is None:
            return None
        return print_stats.data.get("filename")

    def export_state(self) -> MoonrakerStoreState:
        sections = {
            name: SectionSnapshot(
                name=snapshot.name,
                observed_at=snapshot.observed_at,
                data=copy.deepcopy(snapshot.data),
            )
            for name, snapshot in self._sections.items()
        }

        return MoonrakerStoreState(
            sections=sections,
            shutdown_active=self._shutdown_active,
            latest_observed_at=self._latest_observed_at,
        )

    def restore_state(self, state: MoonrakerStoreState) -> None:
        self._sections = {
            name: SectionSnapshot(
                name=snapshot.name,
                observed_at=snapshot.observed_at,
                data=copy.deepcopy(snapshot.data),
            )
            for name, snapshot in state.sections.items()
        }
        self._latest_observed_at = state.latest_observed_at
        self._shutdown_active = state.shutdown_active

    def _ingest_status(
        self, status: Any, observed_at: datetime
    ) -> None:  # pragma: no cover - defensive branch
        if not isinstance(status, Mapping):
            return
        for name, section in status.items():
            self._store_section(name, section, observed_at)

    def _ingest_notification(
        self, method: str, params: Any, observed_at: datetime
    ) -> None:
        if method == "notify_status_update":
            for entry in _iter_dicts(params):
                # Log print_stats updates for debugging state transitions
                if isinstance(entry, Mapping) and "print_stats" in entry:
                    ps = entry["print_stats"]
                    if isinstance(ps, Mapping) and "state" in ps:
                        LOGGER.debug(
                            "WS: print_stats.state=%s",
                            ps.get("state"),
                        )
                self._ingest_status(entry, observed_at)
            return

        if method in {
            "notify_klippy_disconnected",
            "notify_klippy_shutdown",
            "notify_mcu_shutdown",
        }:
            detail = _extract_notification_detail(params)
            LOGGER.debug("Received %s notification: %s", method, detail)
            self._mark_shutdown(detail, observed_at)
            return

        if method == "notify_klippy_ready":
            detail = _extract_notification_detail(params) or "Klippy ready"
            LOGGER.debug("Received notify_klippy_ready: %s", detail)
            # Don't reset print_stats here - let the HTTP query refresh it.
            # Resetting to 'standby' causes spurious printStarted events when
            # recovering from emergency stop (the job may still be active).
            # The TelemetryPublisher will query Moonraker for actual state.
            self._mark_ready(detail, observed_at, reset_print_stats=False)
            return

        if method == "notify_klippy_state":
            state, detail = _extract_klippy_state(params)
            if not state:
                return

            normalized_state = state.lower()
            LOGGER.debug(
                "Received notify_klippy_state: state=%s detail=%s",
                normalized_state,
                detail,
            )
            if normalized_state in {"shutdown", "error"}:
                self._mark_shutdown(detail, observed_at)
                return

            if normalized_state == "ready":
                # Don't reset print_stats here - let the HTTP query refresh it.
                # See comment above for notify_klippy_ready.
                self._mark_ready(detail or "Klippy ready", observed_at, reset_print_stats=False)
                return

        if method == "notify_print_stats_update":
            for entry in _iter_dicts(params):
                print_stats = (
                    entry.get("print_stats") if "print_stats" in entry else entry
                )
                if isinstance(print_stats, Mapping):
                    self._store_section("print_stats", print_stats, observed_at)
            return

        # Ignore notify_gcode_response - we don't try to infer shutdown from gcode

        if method == "notify_history_changed":
            for entry in _iter_dicts(params):
                if isinstance(entry, Mapping):
                    # history_event should REPLACE not merge when action changes
                    # This is critical: action:added has no status field, but
                    # action:finished has status. If we merge, status from previous
                    # print would persist into the next print's action:added event.
                    self._store_history_event(entry, observed_at)
            return

        # Handle moonraker-timelapse events (render complete, etc.)
        if method == "notify_timelapse_event":
            for entry in _iter_dicts(params):
                if isinstance(entry, Mapping):
                    self._store_timelapse_event(entry, observed_at)
            return

        for entry in _iter_dicts(params):
            self._ingest_status(entry, observed_at)

    def _store_section(self, name: str, section: Any, observed_at: datetime) -> None:
        if not isinstance(section, Mapping):
            return

        incoming = dict(section)
        existing = self._sections.get(name)
        if existing is not None:
            baseline_dict = dict(existing.data)
            base: Dict[str, Any] = copy.deepcopy(baseline_dict)
            deep_merge(base, incoming)

            if base == baseline_dict:
                return
        else:
            base = copy.deepcopy(incoming)

        if name == "print_stats":
            # Debug: log what we received vs what we're storing
            if "state" not in incoming and existing is not None:
                old_state = existing.data.get("state")
                new_state = base.get("state")
                if old_state != new_state:
                    LOGGER.warning(
                        "print_stats state changed without receiving state field! "
                        "incoming_keys=%s existing_state=%s new_state=%s",
                        list(incoming.keys()),
                        old_state,
                        new_state,
                    )
            self._log_print_state_transition(existing, base)

        snapshot = SectionSnapshot(
            name=name,
            observed_at=observed_at,
            data=base,
        )
        self._sections[name] = snapshot
        self._latest_observed_at = observed_at

    def _store_history_event(
        self, event: Mapping[str, Any], observed_at: datetime
    ) -> None:
        """Store history_event with proper replacement semantics.

        Unlike other sections that use deep_merge, history_event from
        notify_history_changed should REPLACE the job data when action changes.

        Key insight from Mainsail:
        - action:added = new print started (no status field)
        - action:finished = print ended (has status: completed/cancelled/error)

        If we deep_merge, the status from a previous print's action:finished
        would persist into the next print's action:added, causing:
        1. job_status=completed instead of None during new print
        2. No printCompleted event when new print finishes

        The fix: Replace the entire history_event when a new action arrives,
        rather than merging. This ensures status is cleared when action:added
        arrives for a new print.
        """
        incoming = dict(event)
        existing = self._sections.get("history_event")

        # DEBUG: Log incoming history_event to understand structure
        action = incoming.get("action")
        job = incoming.get("job", {})
        job_id = job.get("job_id") if isinstance(job, dict) else None
        LOGGER.debug(
            "history_event received: action=%s, job_id=%s, keys=%s",
            action,
            job_id,
            list(job.keys()) if isinstance(job, dict) else "no job",
        )

        # Always replace when action changes (different event type)
        if existing is not None:
            old_action = existing.data.get("action")
            new_action = incoming.get("action")
            if old_action != new_action:
                # Action changed (e.g., finished -> added), replace entirely
                LOGGER.debug(
                    "history_event action changed: %s -> %s, replacing",
                    old_action,
                    new_action,
                )
                base = copy.deepcopy(incoming)
            else:
                # Same action type, merge to accumulate updates
                baseline_dict = dict(existing.data)
                base: Dict[str, Any] = copy.deepcopy(baseline_dict)
                deep_merge(base, incoming)
                if base == baseline_dict:
                    return
        else:
            base = copy.deepcopy(incoming)

        snapshot = SectionSnapshot(
            name="history_event",
            observed_at=observed_at,
            data=base,
        )
        self._sections["history_event"] = snapshot
        self._latest_observed_at = observed_at

    def _store_timelapse_event(
        self, event: Mapping[str, Any], observed_at: datetime
    ) -> None:
        """Store moonraker-timelapse event for render detection.

        moonraker-timelapse sends notify_timelapse_event with data like:
        {
            "action": "render",
            "status": "success",
            "filename": "timelapse_xxx.mp4",
            "previewimage": "timelapse_xxx.jpg",
            "printfile": "original_gcode.gcode"
        }

        We store this as timelapse_event section and let the orchestrator
        detect when action=render and status=success to emit TIMELAPSE_READY.
        
        Unlike history_event, we always replace since each timelapse event
        is a discrete notification (not incremental state updates).
        """
        incoming = dict(event)
        action = incoming.get("action", "unknown")
        status = incoming.get("status", "unknown")
        
        LOGGER.debug(
            "timelapse_event: action=%s status=%s filename=%s",
            action,
            status,
            incoming.get("filename"),
        )

        snapshot = SectionSnapshot(
            name="timelapse_event",
            observed_at=observed_at,
            data=incoming,
        )
        self._sections["timelapse_event"] = snapshot
        self._latest_observed_at = observed_at

    def _log_print_state_transition(
        self,
        existing: Optional[SectionSnapshot],
        updated: Mapping[str, Any],
    ) -> None:
        previous_state = (existing.data.get("state") if existing else None)
        current_state = updated.get("state")

        if previous_state == current_state:
            return

        LOGGER.debug(
            "print_stats state transition: %s -> %s",
            previous_state,
            current_state,
        )

        if not isinstance(current_state, str):  # pragma: no cover - defensive guard
            return

        terminal_states = {
            "cancelled",
            "canceled",
            "complete",
            "completed",
            "error",
            "failed",
            "aborted",
        }
        if current_state.lower() not in terminal_states:
            return

        sd_snapshot = self._sections.get("virtual_sdcard")
        sd_data = sd_snapshot.data if sd_snapshot else {}
        is_active = sd_data.get("is_active")
        is_printing = sd_data.get("is_printing")
        progress = sd_data.get("progress")

        LOGGER.warning(
            "Terminal state '%s' while virtual_sdcard=%s (active=%s printing=%s progress=%s)",
            current_state,
            "present" if sd_snapshot else "missing",
            is_active,
            is_printing,
            progress,
        )

    def _mark_shutdown(self, detail: Optional[str], observed_at: datetime) -> None:
        """Mark klippy as shutdown.
        
        Called when we receive notify_klippy_shutdown, notify_klippy_disconnected,
        or notify_mcu_shutdown. We trust these notifications directly.
        """
        message = (detail or "Klippy reported shutdown").strip() or "Klippy reported shutdown"
        self._shutdown_active = True

        offline_sections = {
            "webhooks": {
                "state": "shutdown",
                "state_message": message,
            },
            "printer": {
                "state": "shutdown",
                "is_shutdown": True,
                "state_message": message,
            },
            "print_stats": {
                "state": "error",
                "message": message,
            },
            "display_status": {
                "message": message,
            },
        }

        for name, section in offline_sections.items():
            self._store_section(name, section, observed_at)

    def _mark_ready(
        self,
        detail: Optional[str],
        observed_at: datetime,
        *,
        reset_print_stats: bool = False,
    ) -> None:
        """Mark klippy as ready.
        
        Called when we receive notify_klippy_ready. We trust this notification
        directly and clear the shutdown state.
        """
        message = (detail or "Klippy ready").strip() or "Klippy ready"
        self._shutdown_active = False

        ready_sections = {
            "webhooks": {
                "state": "ready",
                "state_message": message,
            },
            "printer": {
                "state": "ready",
                "is_shutdown": False,
                "state_message": message,
            },
            "display_status": {
                "message": "",
            },
        }

        if reset_print_stats:
            ready_sections["print_stats"] = {
                "state": "standby",
                "message": "",
            }

        for name, section in ready_sections.items():
            self._store_section(name, section, observed_at)


def _iter_dicts(params: Any) -> Iterable[Mapping[str, Any]]:
    if params is None:
        return []
    if isinstance(params, Mapping):
        return [params]
    if isinstance(params, Iterable) and not isinstance(params, (str, bytes)):
        results = []
        for entry in params:
            if isinstance(entry, Mapping):
                results.append(entry)
            elif isinstance(entry, Sequence):
                if (
                    len(entry) == 2
                    and isinstance(entry[0], str)
                    and isinstance(entry[1], Mapping)
                ):
                    results.append({entry[0]: entry[1]})
        return results
    return []


def _extract_notification_detail(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, str):
        detail = value.strip()
        return detail or None

    if isinstance(value, Mapping):
        for key in ("message", "detail", "reason", "state_message"):
            maybe = value.get(key)
            detail = _extract_notification_detail(maybe)
            if detail:
                return detail
        return None

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for entry in value:
            detail = _extract_notification_detail(entry)
            if detail:
                return detail

    return None


def _extract_klippy_state(value: Any) -> tuple[Optional[str], Optional[str]]:
    if value is None:
        return (None, None)

    if isinstance(value, Mapping):
        state = value.get("state")
        detail = _extract_notification_detail(value)
        return (state if isinstance(state, str) else None, detail)

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        state: Optional[str] = None
        detail: Optional[str] = None
        for entry in value:
            if isinstance(entry, str):
                if state is None:
                    state = entry
                else:
                    detail = detail or entry
                continue

            if isinstance(entry, Mapping):
                if state is None:
                    maybe_state = entry.get("state")
                    if isinstance(maybe_state, str):
                        state = maybe_state
                detail = detail or _extract_notification_detail(entry)
                continue

            detail = detail or _extract_notification_detail(entry)

        return (state, detail)

    if isinstance(value, str):
        return (value, None)

    return (None, None)
