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
from ..adapters.camera import CameraClient
from ..adapters.camera_discovery import CameraDiscovery
from ..adapters.image_preprocessor import ImagePreprocessor
from ..adapters.s3_upload import S3UploadClient
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
    from ..core.job_registry import PrintJobRegistry

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
        self._camera_discovery: Optional[CameraDiscovery] = None

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
        *,
        job_registry: Optional["PrintJobRegistry"] = None,
    ) -> TelemetryPublisher:
        """Create a TelemetryPublisher configured for Moonraker.

        Args:
            config: Application configuration.
            mqtt_client: MQTT client for publishing.
            job_registry: Optional registry for PrintJob ID mapping.

        Returns:
            TelemetryPublisher instance.
        """
        return TelemetryPublisher(
            config, self._client, mqtt_client, job_registry=job_registry
        )

    async def create_command_processor(
        self,
        config: "OwlConfig",
        mqtt_client: "MQTTClient",
        telemetry: TelemetryPublisher,
        *,
        job_registry: Optional["PrintJobRegistry"] = None,
        metadata_reporter: Optional[Any] = None,
    ) -> CommandProcessor:
        """Create a CommandProcessor configured for Moonraker.

        Args:
            config: Application configuration.
            mqtt_client: MQTT client for receiving commands.
            telemetry: Telemetry publisher for status updates.
            job_registry: Optional registry for PrintJob ID mapping.

        Returns:
            CommandProcessor instance.
        """
        # Create S3 upload client for agent-initiated uploads (thumbnail, timelapse, capture)
        s3_upload = S3UploadClient()

        # Create camera client for task:capture-image command (ADR-0021)
        # Camera is always initialised when a snapshot URL is available;
        # cloud-side CaptureConfig controls whether captures are scheduled.
        camera: CameraClient | None = None
        image_preprocessor: ImagePreprocessor | None = None
        snapshot_url = await self._resolve_snapshot_url(config)

        if snapshot_url:
            camera = CameraClient(
                snapshot_url=snapshot_url,
                timeout=config.camera.capture_timeout_seconds,
                max_retries=config.camera.max_retries,
            )
            LOGGER.info("Camera client ready: %s", snapshot_url)

            # Create image preprocessor for resizing/compressing captures (ADR-0024)
            if config.camera.preprocess_enabled:
                image_preprocessor = ImagePreprocessor(
                    target_width=config.camera.preprocess_target_width,
                    jpeg_quality=config.camera.preprocess_jpeg_quality,
                    enabled=True,
                )
                LOGGER.info(
                    "Image preprocessing enabled: target_width=%d, jpeg_quality=%d",
                    config.camera.preprocess_target_width,
                    config.camera.preprocess_jpeg_quality,
                )
        else:
            LOGGER.info("No snapshot URL available - camera capture unavailable")

        return CommandProcessor(
            config,
            self._client,
            mqtt_client,
            telemetry=telemetry,
            s3_upload=s3_upload,
            camera=camera,
            image_preprocessor=image_preprocessor,
            job_registry=job_registry,
            metadata_reporter=metadata_reporter,
        )

    # -------------------------------------------------------------------------
    # Camera auto-discovery
    # -------------------------------------------------------------------------

    async def _resolve_snapshot_url(self, config: "OwlConfig") -> Optional[str]:
        """Resolve the camera snapshot URL, using auto-discovery if configured.

        Args:
            config: Application configuration.

        Returns:
            Resolved snapshot URL, or None if discovery fails.
        """
        camera_config = config.camera

        # If explicitly configured, use as-is
        if camera_config.snapshot_url.lower() != "auto":
            return camera_config.snapshot_url

        # Auto-discovery via Moonraker webcams API
        if self._camera_discovery is None:
            self._camera_discovery = CameraDiscovery(
                moonraker_base_url=self._config.url,
            )

        # Use camera_name from config (defaults to "auto" for auto-selection)
        camera_name = camera_config.camera_name or "auto"

        discovered_url = await self._camera_discovery.discover_snapshot_url(
            camera_name=camera_name
        )

        if discovered_url:
            LOGGER.info(
                "Camera auto-discovery successful: %s (camera: %s)",
                discovered_url,
                camera_name or "default",
            )
        else:
            LOGGER.warning(
                "Camera auto-discovery failed - no webcam found (camera: %s)",
                camera_name or "default",
            )

        return discovered_url

    def invalidate_camera_cache(self) -> None:
        """Invalidate the camera discovery cache.

        Call this when the Moonraker connection is re-established, as webcam
        configuration may have changed.
        """
        if self._camera_discovery is not None:
            self._camera_discovery.invalidate()
            LOGGER.debug("Camera discovery cache invalidated")

    # -------------------------------------------------------------------------
    # Moonraker-specific health analysis
    # -------------------------------------------------------------------------

    def _analyse_snapshot(self, snapshot: dict[str, Any]) -> PrinterHealthAssessment:
        """Analyse a Moonraker status snapshot for connectivity issues.

        Only protocol-level issues (malformed responses) indicate Moonraker
        connectivity problems.  Klipper firmware states (shutdown, error) are
        normal operational states — Moonraker remains fully functional and
        must stay reachable for recovery commands (e.g. firmware-restart).

        Tripping the Moonraker breaker on Klipper errors causes a deadlock:
        commands get unsubscribed, preventing firmware-restart from reaching
        the agent to recover the printer.  Actual Moonraker connectivity
        failures are caught separately by assess_health()'s HTTP exception
        handler.

        Args:
            snapshot: Raw Moonraker status response.

        Returns:
            PrinterHealthAssessment — unhealthy only for malformed responses.
        """
        if not isinstance(snapshot, dict):
            return PrinterHealthAssessment(
                healthy=False,
                detail="moonraker response not a mapping",
                force_trip=True,
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
