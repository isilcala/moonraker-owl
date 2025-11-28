"""Moonraker printer backend implementation.

This module provides the PrinterBackend implementation for Klipper printers
running Moonraker. It handles:
- WebSocket connection for real-time status streaming
- Health assessment via status snapshot analysis
- Factory methods for Moonraker-specific telemetry and command handling
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any, Optional

from ..adapters import MoonrakerClient
from ..commands import CommandProcessor
from ..core.printer_backend import (
    PrinterBackend,
    PrinterHealthAssessment,
    StatusCallback,
)
from ..telemetry import TelemetryPublisher

if TYPE_CHECKING:
    from ..adapters.mqtt import MQTTClient
    from ..config import MoonrakerConfig, OwlConfig

LOGGER = logging.getLogger(__name__)


class MoonrakerBackend(PrinterBackend):
    """PrinterBackend implementation for Moonraker/Klipper printers.

    This backend wraps MoonrakerClient and provides the unified interface
    expected by the Owl agent. It handles Moonraker-specific details like:
    - Snapshot format parsing
    - Health state interpretation
    - Error state detection
    """

    def __init__(
        self,
        config: "MoonrakerConfig",
        *,
        client: Optional[MoonrakerClient] = None,
    ) -> None:
        """Initialize the Moonraker backend.

        Args:
            config: Moonraker connection configuration.
            client: Optional pre-configured MoonrakerClient (for testing).
        """
        self._config = config
        self._client = client or MoonrakerClient(config)
        self._on_status: Optional[StatusCallback] = None
        self._started = False

    @property
    def client(self) -> MoonrakerClient:
        """Access the underlying MoonrakerClient."""
        return self._client

    async def start(self, on_status: StatusCallback) -> None:
        """Start the Moonraker connection and status streaming.

        Args:
            on_status: Callback for status updates from Moonraker WebSocket.

        Raises:
            ConnectionError: If initial connection to Moonraker fails.
        """
        if self._started:
            LOGGER.warning("MoonrakerBackend already started")
            return

        self._on_status = on_status
        await self._client.start(self._dispatch_status)
        self._started = True
        LOGGER.info("MoonrakerBackend started, connected to %s", self._config.url)

    async def stop(self) -> None:
        """Stop the Moonraker connection and release resources."""
        if not self._started:
            return

        self._started = False
        await self._client.stop()
        self._on_status = None
        LOGGER.info("MoonrakerBackend stopped")

    async def _dispatch_status(self, payload: dict[str, Any]) -> None:
        """Internal callback to dispatch status to registered handler."""
        if self._on_status is not None:
            await self._on_status(payload)

    async def assess_health(self) -> PrinterHealthAssessment:
        """Assess Moonraker/printer health by fetching status snapshot.

        Returns:
            PrinterHealthAssessment based on current printer state.
        """
        try:
            snapshot = await self._client.fetch_printer_state(
                {
                    "webhooks": ["state"],
                    "printer": ["state", "is_shutdown"],
                    "print_stats": ["state", "message"],
                }
            )
        except Exception as exc:
            LOGGER.debug("Health check failed: %s", exc)
            return PrinterHealthAssessment(
                healthy=False,
                detail=f"connection error: {exc}",
                force_trip=False,
            )

        return self._analyse_snapshot(snapshot)

    async def fetch_state(
        self,
        objects: Optional[dict[str, Optional[list[str]]]] = None,
    ) -> dict[str, Any]:
        """Fetch current printer state from Moonraker.

        Args:
            objects: Moonraker objects to query (None = default set).

        Returns:
            Raw Moonraker status response.
        """
        return await self._client.fetch_printer_state(objects or {})

    def create_telemetry_publisher(
        self,
        config: "OwlConfig",
        mqtt_client: "MQTTClient",
    ) -> TelemetryPublisher:
        """Create a TelemetryPublisher configured for Moonraker.

        Args:
            config: Application configuration.
            mqtt_client: MQTT client for publishing.

        Returns:
            TelemetryPublisher instance.
        """
        return TelemetryPublisher(config, self._client, mqtt_client)

    def create_command_processor(
        self,
        config: "OwlConfig",
        mqtt_client: "MQTTClient",
        telemetry: TelemetryPublisher,
    ) -> CommandProcessor:
        """Create a CommandProcessor configured for Moonraker.

        Args:
            config: Application configuration.
            mqtt_client: MQTT client for receiving commands.
            telemetry: Telemetry publisher for status updates.

        Returns:
            CommandProcessor instance.
        """
        return CommandProcessor(
            config,
            self._client,
            mqtt_client,
            telemetry=telemetry,
        )

    # -------------------------------------------------------------------------
    # Moonraker-specific health analysis
    # -------------------------------------------------------------------------

    def _analyse_snapshot(self, snapshot: dict[str, Any]) -> PrinterHealthAssessment:
        """Analyse a Moonraker status snapshot for health indicators.

        This implements the Moonraker-specific logic for determining whether
        the printer is in a healthy state, based on webhooks, printer, and
        print_stats states.

        Args:
            snapshot: Raw Moonraker status response.

        Returns:
            PrinterHealthAssessment with health determination.
        """
        if not isinstance(snapshot, dict):
            return PrinterHealthAssessment(
                healthy=False,
                detail="moonraker response not a mapping",
                force_trip=True,
            )

        status = snapshot.get("result", {}).get("status", {})
        if not isinstance(status, dict):
            status = {}

        webhooks_state = _normalise_state(status.get("webhooks"), "state")
        printer_state = _normalise_state(status.get("printer"), "state")
        printer_shutdown = _extract_bool(status.get("printer"), "is_shutdown")
        print_stats_state = _normalise_state(status.get("print_stats"), "state")
        print_stats_message = _extract_str(status.get("print_stats"), "message")

        healthy_print_states = {
            "standby",
            "ready",
            "idle",
            "printing",
            "paused",
            "complete",
            "completed",
            "cancelled",
            "canceled",
        }
        failure_print_states = {
            "error",
            "failed",
            "aborted",
            "shutdown",
        }

        failure_detail: Optional[str] = None
        force_trip = False

        if printer_shutdown:
            failure_detail = print_stats_message or "moonraker reports printer shutdown"
            force_trip = True
        elif print_stats_state in failure_print_states:
            failure_detail = (
                print_stats_message or f"print_stats state {print_stats_state}"
            )
            force_trip = True
        elif printer_state in {"shutdown", "error"}:
            failure_detail = print_stats_message or f"printer state {printer_state}"
            force_trip = True
        elif webhooks_state in {"shutdown", "error"}:
            if print_stats_state in healthy_print_states:
                failure_detail = None
                force_trip = False
            else:
                failure_detail = (
                    print_stats_message or f"webhooks state {webhooks_state}"
                )
                force_trip = True

        if failure_detail is not None:
            return PrinterHealthAssessment(
                healthy=False,
                detail=failure_detail,
                force_trip=force_trip,
            )

        return PrinterHealthAssessment(healthy=True)


# -----------------------------------------------------------------------------
# Helper functions for snapshot parsing
# -----------------------------------------------------------------------------


def _normalise_state(node: object, field: str) -> Optional[str]:
    """Extract and normalise a state field from a status node."""
    if not isinstance(node, dict):
        return None
    value = node.get(field)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped.lower()
    return None


def _extract_str(node: object, field: str) -> Optional[str]:
    """Extract a string field from a status node."""
    if not isinstance(node, dict):
        return None
    value = node.get(field)
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _extract_bool(node: object, field: str) -> bool:
    """Extract a boolean field from a status node."""
    if not isinstance(node, dict):
        return False
    value = node.get(field)
    if isinstance(value, bool):
        return value
    return False
