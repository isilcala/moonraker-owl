"""Telemetry cadence control for rate limiting and deduplication.

This module provides centralized cadence evaluation for telemetry channels,
handling:
- Minimum interval enforcement between publishes
- Force-publish timers for liveness (ensures UI gets periodic updates)
- Payload deduplication via hashing
- Force-publish overrides with optional rate limiting

Design follows the principle of separation of concerns - cadence logic
is isolated here rather than scattered through the TelemetryPublisher.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

LOGGER = logging.getLogger(__name__)

# Precision for float normalization in hash computation
HASH_FLOAT_PRECISION = 4


class TelemetryCadenceError(RuntimeError):
    """Raised when cadence configuration is invalid."""


@dataclass(frozen=True)
class ChannelSchedule:
    """Configuration for a single channel's cadence behavior.
    
    Attributes:
        interval: Minimum seconds between regular publishes (None = no limit).
            This is the "rate limit" - prevents publishing too fast.
        max_interval: Maximum seconds between publishes (None = no heartbeat).
            This is the "heartbeat guarantee" - ensures periodic updates even
            when payload is unchanged. When elapsed time exceeds this value,
            a publish is forced regardless of deduplication.
        forced_interval: Minimum seconds between forced publishes (None = no limit)
        force_publish_seconds: Maximum seconds without publish before forcing one (None = disabled).
            Similar to max_interval but only triggers when allow_force_publish=True.
            Deprecated: prefer max_interval for clearer semantics.
    """
    interval: Optional[float]
    max_interval: Optional[float] = None
    forced_interval: Optional[float] = None
    force_publish_seconds: Optional[float] = None


@dataclass
class ChannelRuntimeState:
    """Mutable runtime state for a channel's cadence tracking."""
    hash: Optional[str] = None
    last_publish: float = 0.0


@dataclass(frozen=True)
class ChannelDecision:
    """Result of cadence evaluation for a publish attempt.
    
    Attributes:
        should_publish: True if the payload should be published
        delay_seconds: If not publishing, suggested delay before retry (None = skip entirely)
        reason: Human-readable reason for the decision (None when should_publish=True)
    """
    should_publish: bool
    delay_seconds: Optional[float]
    reason: Optional[str] = None


def _normalize_for_hash(value: Any) -> Any:
    """Normalize a value for deterministic hashing."""
    if isinstance(value, dict):
        return {key: _normalize_for_hash(value[key]) for key in sorted(value.keys())}
    if isinstance(value, list):
        return [_normalize_for_hash(item) for item in value]
    if isinstance(value, float):
        return round(value, HASH_FLOAT_PRECISION)
    return value


def compute_payload_hash(payload: Any) -> str:
    """Compute a deterministic hash for a telemetry payload."""
    normalized = _normalize_for_hash(payload)
    blob = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.md5(blob).hexdigest()


# Protocol for hasher compatibility with TelemetryHasher
class _HasherProtocol:
    def hash_payload(self, payload: Any) -> str: ...


class ChannelCadenceController:
    """Centralized cadence evaluation for telemetry channels.
    
    This controller manages publish timing for multiple channels, enforcing:
    - Minimum intervals between publishes
    - Deduplication via payload hashing
    - Force-publish timers for liveness guarantees
    - Rate limiting for forced publishes
    
    Thread-safety: This class is NOT thread-safe. All calls should occur
    on the same event loop thread.
    """

    def __init__(
        self,
        *,
        monotonic: Optional[Callable[[], float]] = None,
        hasher: Optional[_HasherProtocol] = None,
    ) -> None:
        """Initialize the cadence controller.
        
        Args:
            monotonic: Clock function returning monotonic seconds. Defaults to time.monotonic.
            hasher: Optional hasher with hash_payload method. If not provided, uses built-in.
        """
        self._monotonic = monotonic or time.monotonic
        self._hasher = hasher
        self._schedules: Dict[str, ChannelSchedule] = {}
        self._state: Dict[str, ChannelRuntimeState] = {}

    def _compute_hash(self, payload: Dict[str, Any]) -> str:
        """Compute hash using injected hasher or built-in function."""
        if self._hasher is not None:
            return self._hasher.hash_payload(payload)
        return compute_payload_hash(payload)

    def configure(
        self,
        channel: str,
        *,
        interval: Optional[float] = None,
        max_interval: Optional[float] = None,
        forced_interval: Optional[float] = None,
        force_publish_seconds: Optional[float] = None,
    ) -> None:
        """Configure cadence parameters for a channel.
        
        Args:
            channel: Channel name (e.g., "status", "sensors", "events")
            interval: Minimum seconds between regular publishes (rate limit)
            max_interval: Maximum seconds between publishes (heartbeat guarantee).
                When elapsed time exceeds this, forces a publish even if payload unchanged.
            forced_interval: Minimum seconds between forced publishes
            force_publish_seconds: Maximum seconds without publish before forcing.
                Similar to max_interval but only triggers with allow_force_publish=True.
        """
        self._schedules[channel] = ChannelSchedule(
            interval=interval if interval and interval > 0 else None,
            max_interval=max_interval if max_interval and max_interval > 0 else None,
            forced_interval=forced_interval if forced_interval and forced_interval > 0 else None,
            force_publish_seconds=force_publish_seconds if force_publish_seconds and force_publish_seconds > 0 else None,
        )
        self._state.setdefault(channel, ChannelRuntimeState())

    def reset_channel(self, channel: str) -> None:
        """Reset runtime state for a single channel."""
        if channel in self._state:
            self._state[channel] = ChannelRuntimeState()

    def reset_all(self) -> None:
        """Reset runtime state for all channels."""
        for channel in list(self._state.keys()):
            self._state[channel] = ChannelRuntimeState()

    def get_schedule(self, channel: str) -> ChannelSchedule:
        """Get the configured schedule for a channel.
        
        Raises:
            TelemetryCadenceError: If channel is not configured
        """
        schedule = self._schedules.get(channel)
        if schedule is None:
            raise TelemetryCadenceError(
                f"Cadence schedule not configured for channel '{channel}'"
            )
        return schedule

    def evaluate(
        self,
        channel: str,
        payload: Dict[str, Any],
        *,
        explicit_force: bool = False,
        respect_cadence: bool = False,
        allow_force_publish: bool = False,
    ) -> ChannelDecision:
        """Evaluate whether a payload should be published.
        
        Args:
            channel: Target channel name
            payload: Payload to evaluate
            explicit_force: True if caller explicitly requests publish
            respect_cadence: True to apply forced_interval even when forcing
            allow_force_publish: True to allow force-publish timer to trigger publish
            
        Returns:
            CadenceDecision indicating whether to publish and why
            
        Raises:
            TelemetryCadenceError: If channel is not configured
        """
        schedule = self._schedules.get(channel)
        if schedule is None:
            raise TelemetryCadenceError(
                f"Cadence schedule not configured for channel '{channel}'"
            )

        state = self._state.setdefault(channel, ChannelRuntimeState())
        now = self._monotonic()

        # Check max_interval (heartbeat guarantee) - always force publish when exceeded
        # This is the primary mechanism for ensuring periodic updates
        heartbeat_due = False
        if schedule.max_interval:
            if state.last_publish == 0.0:
                heartbeat_due = True
            else:
                elapsed = now - state.last_publish
                if elapsed >= schedule.max_interval:
                    heartbeat_due = True

        # Check force-publish timer (legacy, prefer max_interval for new code)
        # Only triggers when allow_force_publish=True
        forced_for_dedup = explicit_force or heartbeat_due
        if allow_force_publish and schedule.force_publish_seconds:
            if state.last_publish == 0.0:
                forced_for_dedup = True
            else:
                elapsed = now - state.last_publish
                if elapsed >= schedule.force_publish_seconds:
                    forced_for_dedup = True

        # Apply forced_interval rate limiting when explicitly forcing
        if explicit_force and respect_cadence:
            constrained_interval = (
                schedule.forced_interval
                if schedule.forced_interval is not None
                else schedule.interval
            )
            if constrained_interval and state.last_publish != 0.0:
                elapsed = now - state.last_publish
                if elapsed < constrained_interval:
                    return ChannelDecision(
                        should_publish=False,
                        delay_seconds=constrained_interval - elapsed,
                        reason="forced-rate",
                    )

        # Apply regular interval rate limiting
        if schedule.interval and state.last_publish != 0.0 and not forced_for_dedup:
            elapsed = now - state.last_publish
            if elapsed < schedule.interval:
                return ChannelDecision(
                    should_publish=False,
                    delay_seconds=schedule.interval - elapsed,
                    reason="cadence",
                )

        # Check deduplication
        payload_hash = self._compute_hash(payload)
        if not forced_for_dedup and payload_hash == state.hash:
            return ChannelDecision(
                should_publish=False,
                delay_seconds=None,
                reason="dedup",
            )

        # Publish allowed - update state
        state.hash = payload_hash
        state.last_publish = now

        return ChannelDecision(
            should_publish=True,
            delay_seconds=None,
            reason="ready",
        )

    def mark_published(self, channel: str, payload_hash: Optional[str] = None) -> None:
        """Manually mark a channel as having just published.
        
        Useful when publish happens outside normal evaluate() flow.
        
        Args:
            channel: Channel that was published
            payload_hash: Hash of published payload (computed if not provided)
        """
        state = self._state.setdefault(channel, ChannelRuntimeState())
        state.last_publish = self._monotonic()
        if payload_hash is not None:
            state.hash = payload_hash

    def get_next_heartbeat_delay(self) -> Optional[float]:
        """Calculate seconds until the next heartbeat is due for any channel.
        
        This allows the main loop to schedule a timer for proactive heartbeat
        checking, rather than relying solely on external events to trigger checks.
        
        Returns:
            Seconds until next heartbeat due, or None if no channels have max_interval.
        """
        now = self._monotonic()
        min_delay: Optional[float] = None
        
        for channel, schedule in self._schedules.items():
            if schedule.max_interval is None:
                continue
            
            state = self._state.get(channel)
            if state is None or state.last_publish == 0.0:
                # Never published - heartbeat due immediately
                return 0.0
            
            elapsed = now - state.last_publish
            remaining = schedule.max_interval - elapsed
            
            if remaining <= 0:
                # Already overdue
                return 0.0
            
            if min_delay is None or remaining < min_delay:
                min_delay = remaining
        
        return min_delay

    def get_channels_needing_heartbeat(self) -> list[str]:
        """Get list of channels that currently need a heartbeat publish.
        
        Returns:
            List of channel names where max_interval has been exceeded.
        """
        now = self._monotonic()
        result: list[str] = []
        
        for channel, schedule in self._schedules.items():
            if schedule.max_interval is None:
                continue
            
            state = self._state.get(channel)
            if state is None or state.last_publish == 0.0:
                result.append(channel)
                continue
            
            elapsed = now - state.last_publish
            if elapsed >= schedule.max_interval:
                result.append(channel)
        
        return result
