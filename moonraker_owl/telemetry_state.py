"""Telemetry state cache and hashing utilities."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
from threading import Lock

LOGGER_SAFE_FLOAT_DIGITS = 4


def _normalise_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalise_value(value[key]) for key in sorted(value.keys())}
    if isinstance(value, list):
        return [_normalise_value(item) for item in value]
    if isinstance(value, float):
        return round(value, LOGGER_SAFE_FLOAT_DIGITS)
    return value


class TelemetryHasher:
    """Produce deterministic hashes for telemetry payloads."""

    def hash_payload(self, payload: Any) -> str:
        normalised = _normalise_value(payload)
        blob = json.dumps(normalised, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        return hashlib.md5(blob).hexdigest()


@dataclass
class ChannelSnapshot:
    payload: Optional[Any] = None
    payload_hash: Optional[str] = None
    sequence: int = 0
    force_publish: bool = False
    forced_reason: Optional[str] = None


@dataclass
class PublishDecision:
    should_publish: bool
    sequence: int
    diff_summary: str
    previous_hash: Optional[str]
    current_hash: Optional[str]
    changed: bool
    reason: Optional[str] = None


class TelemetryStateCache:
    """Track per-channel telemetry state and deduplication flags."""

    def __init__(self, channels: Iterable[str], hasher: TelemetryHasher) -> None:
        self._states: Dict[str, ChannelSnapshot] = {
            channel: ChannelSnapshot() for channel in channels
        }
        self._hasher = hasher
        self._lock = Lock()

    def force_next_publish(self, channel: str, *, reason: Optional[str] = None) -> None:
        with self._lock:
            snapshot = self._states.get(channel)
            if snapshot is None:
                snapshot = ChannelSnapshot()
                self._states[channel] = snapshot
            snapshot.force_publish = True
            snapshot.forced_reason = reason

    def evaluate(
        self,
        channel: str,
        payload: Optional[Any],
        *,
        force_publish: bool = False,
        diff_callback=None,
    ) -> PublishDecision:
        with self._lock:
            snapshot = self._states.setdefault(channel, ChannelSnapshot())
            previous_payload = snapshot.payload
            previous_hash = snapshot.payload_hash

            if payload is None:
                current_hash = None
            else:
                current_hash = self._hasher.hash_payload(payload)

            changed = current_hash != previous_hash
            forced = snapshot.force_publish or force_publish
            should_publish = forced or changed

            if not should_publish:
                return PublishDecision(
                    should_publish=False,
                    sequence=snapshot.sequence,
                    diff_summary="unchanged",
                    previous_hash=previous_hash,
                    current_hash=current_hash,
                    changed=False,
                    reason=None,
                )

            snapshot.sequence += 1
            snapshot.payload = copy.deepcopy(payload) if payload is not None else None
            snapshot.payload_hash = current_hash
            forced_reason = snapshot.forced_reason if snapshot.force_publish else None
            snapshot.force_publish = False
            snapshot.forced_reason = None

            reason = forced_reason or (
                "forced" if force_publish and not changed else None
            )

            diff_summary = "initial snapshot"
            if previous_payload is not None and payload is not None:
                if diff_callback:
                    diff_summary = diff_callback(previous_payload, payload)
                elif changed:
                    diff_summary = "payload changed"
            elif previous_payload is not None or payload is not None:
                diff_summary = "payload changed"

            return PublishDecision(
                should_publish=True,
                sequence=snapshot.sequence,
                diff_summary=diff_summary,
                previous_hash=previous_hash,
                current_hash=current_hash,
                changed=changed,
                reason=reason,
            )
