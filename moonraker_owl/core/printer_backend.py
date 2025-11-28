"""Protocol definitions for printer backend abstraction.

This module defines the contract that printer backends must implement,
enabling support for different 3D printer control systems (Moonraker,
OctoPrint, Duet, etc.) without modifying the core Owl agent logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Optional,
    Protocol,
    runtime_checkable,
)

if TYPE_CHECKING:
    from ..commands import CommandProcessor
    from ..config import OwlConfig
    from ..telemetry import TelemetryPublisher


# Type alias for status update callbacks
StatusCallback = Callable[[dict[str, Any]], Awaitable[None]]


def _utcnow() -> datetime:
    """Return current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


@dataclass(slots=True, frozen=True)
class PrinterHealthAssessment:
    """Result of a printer health check.

    Attributes:
        healthy: Whether the printer is in a healthy operational state.
        detail: Human-readable description of the health status.
        force_trip: If unhealthy, whether to immediately trip the circuit breaker
                   (vs. allowing gradual failure accumulation).
        timestamp: When this assessment was made.
    """

    healthy: bool
    detail: Optional[str] = None
    force_trip: bool = False
    timestamp: datetime = field(default_factory=_utcnow)


@runtime_checkable
class PrinterBackend(Protocol):
    """Contract for printer backend implementations.

    A PrinterBackend abstracts the specific printer control system
    (e.g., Moonraker, OctoPrint) and provides a unified interface for:
    - Connection management
    - Health monitoring
    - State retrieval
    - Component factories

    Implementations should be stateful and manage their own internal
    resources (HTTP clients, WebSocket connections, etc.).
    """

    async def start(self, on_status: StatusCallback) -> None:
        """Start the printer backend and begin streaming status updates.

        This method should:
        1. Establish connection to the printer control system
        2. Subscribe to status/event streams
        3. Begin calling on_status with status payloads

        Args:
            on_status: Async callback invoked with each status update.
                      The payload format is backend-specific but should
                      be consistent for the telemetry pipeline.

        Raises:
            ConnectionError: If initial connection fails.
        """
        ...

    async def stop(self) -> None:
        """Stop the printer backend and release resources.

        This method should:
        1. Unsubscribe from status streams
        2. Close connections gracefully
        3. Cancel any pending operations
        """
        ...

    async def assess_health(self) -> PrinterHealthAssessment:
        """Poll the printer and return a health assessment.

        This is used for periodic health monitoring independent of
        the status stream. Implementations should make a lightweight
        API call to verify printer accessibility.

        Returns:
            PrinterHealthAssessment indicating current health status.
        """
        ...

    async def fetch_state(
        self,
        objects: Optional[dict[str, Optional[list[str]]]] = None,
    ) -> dict[str, Any]:
        """Fetch current printer state snapshot.

        Args:
            objects: Optional filter for specific state objects.
                    Format is backend-specific.

        Returns:
            Dictionary containing current printer state.
        """
        ...

    def create_telemetry_publisher(
        self,
        config: "OwlConfig",
        mqtt_client: Any,
    ) -> "TelemetryPublisher":
        """Factory method for creating a telemetry publisher.

        Each backend may have specific telemetry formatting requirements.
        This factory allows backends to customize telemetry behavior.

        Args:
            config: Application configuration.
            mqtt_client: MQTT client for publishing telemetry.

        Returns:
            Configured TelemetryPublisher instance.
        """
        ...

    def create_command_processor(
        self,
        config: "OwlConfig",
        mqtt_client: Any,
        telemetry: "TelemetryPublisher",
    ) -> "CommandProcessor":
        """Factory method for creating a command processor.

        Each backend may have specific command handling requirements.
        This factory allows backends to customize command processing.

        Args:
            config: Application configuration.
            mqtt_client: MQTT client for receiving commands.
            telemetry: Telemetry publisher for status updates.

        Returns:
            Configured CommandProcessor instance.
        """
        ...


@runtime_checkable
class PrinterBackendFactory(Protocol):
    """Factory protocol for creating printer backends.

    This allows the agent to instantiate backends without knowing
    the concrete implementation details.
    """

    def create(self, config: "OwlConfig") -> PrinterBackend:
        """Create a printer backend instance.

        Args:
            config: Application configuration containing backend-specific
                   settings (e.g., Moonraker URL, API key).

        Returns:
            Configured PrinterBackend instance.
        """
        ...


__all__ = [
    "PrinterBackend",
    "PrinterBackendFactory",
    "PrinterHealthAssessment",
    "StatusCallback",
]
