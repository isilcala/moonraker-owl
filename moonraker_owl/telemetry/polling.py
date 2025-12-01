"""Polling scheduler for Moonraker objects not covered by subscriptions.

This module provides declarative polling for Moonraker objects that are
not available via the websocket subscription API (e.g., system stats,
environment sensors).

Design principles:
- Declarative polling specs rather than imperative loops
- Isolated from the main telemetry publisher
- Graceful cancellation on shutdown
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional, Sequence

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PollSpec:
    """Declarative definition for a Moonraker polling schedule.
    
    Attributes:
        name: Human-readable name for this poll group
        fields: Moonraker object names or "object.attribute" specs to fetch
        interval_seconds: Seconds between polling attempts
        initial_delay_seconds: Seconds to wait before first poll
        force_poll: If True, poll even if objects are already subscribed via WebSocket.
                   Use for critical state like print_stats that must stay synchronized.
    """
    name: str
    fields: Sequence[str]
    interval_seconds: float
    initial_delay_seconds: float = 0.0
    force_poll: bool = False


@dataclass
class _PollGroup:
    """Internal representation of a resolved poll group."""
    name: str
    objects: Dict[str, Optional[list[str]]]
    interval: float
    initial_delay: float


# Default polling specs for common Moonraker objects
DEFAULT_POLL_SPECS: tuple[PollSpec, ...] = (
    # NOTE: print-state-sync polling DISABLED for testing.
    # We're verifying if the state normalization fix in state_store.py
    # resolves the missed state transition issue without needing polling.
    # Re-enable after confirming root cause.
    #
    # PollSpec(
    #     name="print-state-sync",
    #     fields=(
    #         "print_stats.state",
    #         "print_stats.filename",
    #         "print_stats.message",
    #         "print_stats.print_duration",
    #         "virtual_sdcard.is_active",
    #         "virtual_sdcard.progress",
    #     ),
    #     interval_seconds=15.0,
    #     initial_delay_seconds=5.0,
    #     force_poll=True,
    # ),
    PollSpec(
        name="environment-sensors",
        fields=(
            "temperature_sensor ambient",
            "temperature_sensor chamber",
        ),
        interval_seconds=30.0,
        initial_delay_seconds=5.0,
    ),
    PollSpec(
        name="diagnostics",
        fields=(
            "moonraker_stats",
            "system_cpu_usage",
            "system_memory",
            "network",
            "websocket_connections",
            "cpu_temp",
        ),
        interval_seconds=60.0,
        initial_delay_seconds=10.0,
    ),
)


class PollingScheduler:
    """Manages polling tasks for Moonraker objects.
    
    This scheduler runs independent polling loops for objects that are
    not available via websocket subscriptions. Each poll group runs
    on its own schedule and feeds results into the main telemetry queue.
    """

    def __init__(
        self,
        *,
        fetch_state: Callable[[Dict[str, Optional[list[str]]]], Awaitable[Dict[str, Any]]],
        enqueue: Callable[[Dict[str, Any]], Awaitable[None]],
        stop_event: asyncio.Event,
        poll_specs: Optional[Iterable[PollSpec]] = None,
        subscribed_objects: Optional[set[str]] = None,
    ) -> None:
        """Initialize the polling scheduler.
        
        Args:
            fetch_state: Async function to fetch Moonraker state for given objects
            enqueue: Async function to enqueue fetched payloads
            stop_event: Event signaling shutdown
            poll_specs: Polling specifications (defaults to DEFAULT_POLL_SPECS)
            subscribed_objects: Objects already subscribed via websocket (excluded from polling)
        """
        self._fetch_state = fetch_state
        self._enqueue = enqueue
        self._stop_event = stop_event
        self._poll_specs = tuple(poll_specs or DEFAULT_POLL_SPECS)
        self._subscribed_objects = subscribed_objects or set()
        self._poll_groups: list[_PollGroup] = []
        self._poll_tasks: list[asyncio.Task[None]] = []

    def build_poll_groups(
        self,
        build_manifest: Callable[[Iterable[str], tuple[()]], Dict[str, Optional[list[str]]]],
    ) -> None:
        """Build poll groups from specs, excluding already-subscribed objects.
        
        Args:
            build_manifest: Function to convert field specs to subscription manifest
        """
        self._poll_groups.clear()

        for spec in self._poll_specs:
            manifest = build_manifest(spec.fields, ())
            filtered = {
                key: value
                for key, value in manifest.items()
                if key and key not in self._subscribed_objects
            }

            if not filtered:
                continue

            interval = spec.interval_seconds if spec.interval_seconds > 0 else 60.0
            initial_delay = (
                spec.initial_delay_seconds if spec.initial_delay_seconds > 0 else 0.0
            )

            self._poll_groups.append(
                _PollGroup(
                    name=spec.name,
                    objects=filtered,
                    interval=interval,
                    initial_delay=initial_delay,
                )
            )

    def start(self) -> None:
        """Start all polling tasks."""
        if self._poll_tasks:
            return

        for group in self._poll_groups:
            task = asyncio.create_task(self._poll_loop(group))
            self._poll_tasks.append(task)

    async def stop(self) -> None:
        """Stop all polling tasks."""
        if not self._poll_tasks:
            return

        for task in self._poll_tasks:
            task.cancel()

        for task in self._poll_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._poll_tasks.clear()

    async def _poll_loop(self, group: _PollGroup) -> None:
        """Run polling loop for a single group."""
        if not group.objects:
            return

        # Initial delay
        delay = max(group.initial_delay, 0.0)
        if delay:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                return  # Stop event was set
            except asyncio.TimeoutError:
                pass

        while not self._stop_event.is_set():
            # Fetch state
            try:
                payload = await self._fetch_state(group.objects)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass  # Polling failure handled by circuit breaker
            else:
                await self._enqueue(payload)

            # Wait for next interval
            interval = max(group.interval, 0.1)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                break  # Stop event was set
            except asyncio.TimeoutError:
                continue
