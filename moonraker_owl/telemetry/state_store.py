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
    """Serializable view of the state store used for hand-offs."""

    sections: Dict[str, SectionSnapshot]
    shutdown_active: bool
    last_shutdown_detail: Optional[str]
    pending_shutdown_hint: Optional[str]
    latest_observed_at: Optional[datetime]


class MoonrakerStateStore:
    """Caches the latest Moonraker state per section."""

    def __init__(self, *, clock: Optional[Callable[[], datetime]] = None) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sections: Dict[str, SectionSnapshot] = {}
        self._latest_observed_at: Optional[datetime] = None
        self._shutdown_active = False
        self._last_shutdown_detail: Optional[str] = None
        self._pending_shutdown_hint: Optional[str] = None

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
            last_shutdown_detail=self._last_shutdown_detail,
            pending_shutdown_hint=self._pending_shutdown_hint,
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
        self._last_shutdown_detail = state.last_shutdown_detail
        self._pending_shutdown_hint = state.pending_shutdown_hint

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
            self._mark_ready(detail, observed_at, reset_print_stats=True)
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
                self._mark_ready(detail or "Klippy ready", observed_at, reset_print_stats=True)
                return

        if method == "notify_print_stats_update":
            for entry in _iter_dicts(params):
                print_stats = (
                    entry.get("print_stats") if "print_stats" in entry else entry
                )
                if isinstance(print_stats, Mapping):
                    self._store_section("print_stats", print_stats, observed_at)
            return

        if method == "notify_gcode_response":
            hint = _extract_gcode_shutdown_hint(params)
            if hint:
                LOGGER.debug(
                    "Captured gcode response shutdown hint: %s",
                    hint,
                )
                self._pending_shutdown_hint = hint
            return

        if method == "notify_history_changed":
            for entry in _iter_dicts(params):
                if isinstance(entry, Mapping):
                    self._store_section("history_event", entry, observed_at)
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
            self._enforce_shutdown_print_state(base)
            self._maybe_recover_from_shutdown(base, observed_at)
            self._log_print_state_transition(existing, base)

        if name == "webhooks":
            self._update_shutdown_state_from_webhooks(base)

        if name == "printer":
            self._update_shutdown_state_from_printer(base)

        snapshot = SectionSnapshot(
            name=name,
            observed_at=observed_at,
            data=base,
        )
        self._sections[name] = snapshot
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
        message = (
            (detail or self._pending_shutdown_hint or "Klippy reported shutdown")
            .strip()
            or "Klippy reported shutdown"
        )
        self._shutdown_active = True
        self._last_shutdown_detail = message
        self._pending_shutdown_hint = None

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
        message = (detail or "Klippy ready").strip() or "Klippy ready"
        self._shutdown_active = False
        self._last_shutdown_detail = None
        self._pending_shutdown_hint = None

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

    def _maybe_recover_from_shutdown(
        self,
        section: Mapping[str, Any],
        observed_at: datetime,
    ) -> None:
        if not self._shutdown_active:
            return

        state = section.get("state")
        if not isinstance(state, str):
            return

        normalized = state.strip().lower()
        if normalized in {"", "error", "shutdown", "startup", "offline", "unknown"}:
            return

        message = section.get("message")
        resolved = message.strip() if isinstance(message, str) else ""
        if not resolved or resolved == (self._last_shutdown_detail or ""):
            resolved = "Klippy ready"

        self._mark_ready(resolved, observed_at)

    def _enforce_shutdown_print_state(self, section: Dict[str, Any]) -> None:
        if not self._shutdown_active:
            return

        state = section.get("state")
        normalized = state.strip().lower() if isinstance(state, str) else ""
        if normalized in {"error", "shutdown"}:
            if (not section.get("message")) and self._last_shutdown_detail:
                section["message"] = self._last_shutdown_detail
            return

        detail = section.get("message")
        if not isinstance(detail, str) or not detail.strip():
            detail = self._last_shutdown_detail

        section["state"] = "error"
        if detail:
            section["message"] = detail

    def _update_shutdown_state_from_webhooks(self, section: Mapping[str, Any]) -> None:
        state = section.get("state")
        if not isinstance(state, str):
            return

        normalized = state.strip().lower()
        if normalized in {"shutdown", "error"}:
            self._shutdown_active = True
            detail = section.get("state_message")
            if isinstance(detail, str) and detail.strip():
                self._last_shutdown_detail = detail.strip()
            return

        if normalized in {"ready", "standby", "printing", "paused", "idle"}:
            was_shutdown = self._shutdown_active
            self._shutdown_active = False
            self._last_shutdown_detail = None
            self._pending_shutdown_hint = None
            if was_shutdown:
                self._lift_print_stats_after_ready(self._clock())

    def _update_shutdown_state_from_printer(self, section: Mapping[str, Any]) -> None:
        if "is_shutdown" not in section:
            return

        is_shutdown = section.get("is_shutdown")
        if not isinstance(is_shutdown, bool):
            return

        if is_shutdown:
            self._shutdown_active = True
            detail = section.get("state_message")
            if isinstance(detail, str) and detail.strip():
                self._last_shutdown_detail = detail.strip()
            return

        was_shutdown = self._shutdown_active
        self._shutdown_active = False
        self._last_shutdown_detail = None
        self._pending_shutdown_hint = None
        if was_shutdown:
            self._lift_print_stats_after_ready(self._clock())

    def _lift_print_stats_after_ready(self, observed_at: datetime) -> None:
        snapshot = self._sections.get("print_stats")
        existing = dict(snapshot.data) if snapshot else {}

        state = existing.get("state")
        reset_required = True
        if isinstance(state, str):
            normalized = state.strip().lower()
            if normalized not in {"", "error", "shutdown"}:
                reset_required = False

        if not reset_required:
            detail = existing.get("message")
            if isinstance(detail, str) and detail.strip() == "Klippy reported shutdown":
                updated = dict(existing)
                updated["message"] = ""
                self._store_section("print_stats", updated, observed_at)
            return

        ready_section = {
            "state": "standby",
            "message": "",
        }
        self._store_section("print_stats", ready_section, observed_at)


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


def _extract_gcode_shutdown_hint(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, str):
        normalized = " ".join(value.strip().split())
        if not normalized:
            return None

        lowered = normalized.lower()
        if "emergency stop" in lowered:
            return "Emergency stop"
        if "printer is shutdown" in lowered:
            return "Printer is shutdown"
        if lowered.startswith("firmware restart"):
            return normalized
        return None

    if isinstance(value, Mapping):
        for entry in value.values():
            hint = _extract_gcode_shutdown_hint(entry)
            if hint:
                return hint
        return None

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for entry in value:
            hint = _extract_gcode_shutdown_hint(entry)
            if hint:
                return hint
        return None

    return None


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
