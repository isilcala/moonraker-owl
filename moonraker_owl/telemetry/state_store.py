"""Moonraker state store for the telemetry pipeline."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Sequence

from ..core import deep_merge


@dataclass(frozen=True)
class SectionSnapshot:
    """Immutable snapshot of a Moonraker section update."""

    name: str
    observed_at: datetime
    data: Mapping[str, Any]


class MoonrakerStateStore:
    """Caches the latest Moonraker state per section."""

    def __init__(self, *, clock: Optional[Callable[[], datetime]] = None) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sections: Dict[str, SectionSnapshot] = {}

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

        if method == "notify_print_stats_update":
            for entry in _iter_dicts(params):
                print_stats = (
                    entry.get("print_stats") if "print_stats" in entry else entry
                )
                if isinstance(print_stats, Mapping):
                    self._store_section("print_stats", print_stats, observed_at)
            return

        if method == "notify_gcode_response":
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
            base: Dict[str, Any] = copy.deepcopy(dict(existing.data))
            deep_merge(base, incoming)
        else:
            base = copy.deepcopy(incoming)

        snapshot = SectionSnapshot(
            name=name,
            observed_at=observed_at,
            data=base,
        )
        self._sections[name] = snapshot


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
