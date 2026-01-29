"""Metadata reporter for uploading device capabilities to the cloud.

The MetadataReporter orchestrates capability detection from multiple providers
and uploads the aggregated metadata to the PrinterService API.

See ADR-0032 for architecture details and data model.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import aiohttp

from .providers import (
    BaseProvider,
    CameraProvider,
    KlipperProvider,
    MoonrakerProvider,
    SystemInfoProvider,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class MetadataReporterConfig:
    """Configuration for MetadataReporter."""

    # Cloud API endpoint (will append /api/v1/devices/{deviceId}/metadata)
    api_base_url: str

    # Moonraker base URL for capability detection
    moonraker_url: str

    # Device ID for the API path
    device_id: str

    # Refresh interval in seconds (default: 24 hours)
    refresh_interval_seconds: float = 86400.0

    # Initial retry delay in seconds
    initial_retry_delay: float = 30.0

    # Maximum retry delay in seconds
    max_retry_delay: float = 600.0  # 10 minutes

    # Maximum retry attempts (0 for infinite)
    max_retry_attempts: int = 0

    # Request timeout in seconds
    request_timeout: float = 30.0


class MetadataReporter:
    """Orchestrates metadata collection and cloud upload.

    MetadataReporter manages the lifecycle of capability providers,
    aggregates their results, and uploads metadata to PrinterService.

    Usage:
        reporter = MetadataReporter(config, token_provider)
        await reporter.start()  # Non-blocking, schedules periodic reporting
        ...
        await reporter.stop()

    The reporter handles failures gracefully:
    - Individual provider failures don't affect other providers
    - Upload failures trigger exponential backoff retry
    - Reporting doesn't block MQTT connection
    """

    def __init__(
        self,
        config: MetadataReporterConfig,
        token_provider: Callable[[], Optional[str]],
        *,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        """Initialize the metadata reporter.

        Args:
            config: Reporter configuration.
            token_provider: Callable returning current JWT token (or None if unavailable).
            session: Optional aiohttp session to share.
        """
        self._config = config
        self._token_provider = token_provider
        self._session = session
        self._owns_session = session is None

        self._providers: List[BaseProvider] = []
        self._report_task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._last_report_success = False
        self._retry_delay = config.initial_retry_delay

    async def start(self) -> None:
        """Start the metadata reporter.

        This method starts a background task that:
        1. Immediately reports metadata
        2. Schedules periodic refresh at configured interval

        This method is non-blocking and returns immediately.
        """
        if self._report_task is not None:
            LOGGER.warning("MetadataReporter already started")
            return

        self._stop_event.clear()
        self._initialize_providers()
        self._report_task = asyncio.create_task(self._report_loop())
        LOGGER.info(
            "MetadataReporter started (refresh_interval=%ds)",
            self._config.refresh_interval_seconds,
        )

    async def stop(self) -> None:
        """Stop the metadata reporter and clean up resources."""
        if self._report_task is None:
            return

        self._stop_event.set()

        # Cancel and wait for task
        self._report_task.cancel()
        try:
            await self._report_task
        except asyncio.CancelledError:
            pass

        self._report_task = None

        # Clean up providers
        await self._cleanup_providers()

        # Clean up session if we own it
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

        LOGGER.info("MetadataReporter stopped")

    def _initialize_providers(self) -> None:
        """Initialize capability providers."""
        moonraker_url = self._config.moonraker_url

        self._providers = [
            SystemInfoProvider(),
            MoonrakerProvider(moonraker_url),
            KlipperProvider(moonraker_url),
            CameraProvider(moonraker_url),
        ]

    async def _cleanup_providers(self) -> None:
        """Clean up provider resources."""
        for provider in self._providers:
            if hasattr(provider, "close"):
                try:
                    await provider.close()
                except Exception as exc:
                    LOGGER.debug(
                        "Error closing provider %s: %s",
                        provider.name,
                        exc,
                    )

        self._providers.clear()

    async def _report_loop(self) -> None:
        """Background loop for periodic metadata reporting."""
        try:
            # Initial report
            await self._report_with_retry()

            # Periodic refresh
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._config.refresh_interval_seconds,
                    )
                    # If we get here, stop was requested
                    break
                except asyncio.TimeoutError:
                    # Timeout expired, time for refresh
                    pass

                await self._report_with_retry()
        except asyncio.CancelledError:
            LOGGER.debug("Report loop cancelled")
            raise
        except Exception as exc:
            LOGGER.error("Unexpected error in report loop: %s", exc, exc_info=True)

    async def _report_with_retry(self) -> None:
        """Report metadata with exponential backoff retry."""
        attempt = 0
        self._retry_delay = self._config.initial_retry_delay

        while True:
            attempt += 1

            if self._stop_event.is_set():
                LOGGER.debug("Stop requested, aborting report")
                return

            try:
                await self._report_once()
                self._last_report_success = True
                return
            except Exception as exc:
                self._last_report_success = False
                max_attempts = self._config.max_retry_attempts

                if max_attempts > 0 and attempt >= max_attempts:
                    LOGGER.error(
                        "Metadata report failed after %d attempts: %s",
                        attempt,
                        exc,
                    )
                    return

                LOGGER.warning(
                    "Metadata report failed (attempt %d), retrying in %.0fs: %s",
                    attempt,
                    self._retry_delay,
                    exc,
                )

                # Wait before retry
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._retry_delay,
                    )
                    # Stop requested during wait
                    return
                except asyncio.TimeoutError:
                    pass

                # Exponential backoff
                self._retry_delay = min(
                    self._retry_delay * 2,
                    self._config.max_retry_delay,
                )

    async def _report_once(self) -> None:
        """Collect and upload metadata once."""
        LOGGER.debug("Collecting device metadata...")

        metadata = await self._collect_metadata()

        if not metadata:
            LOGGER.warning("No metadata collected, skipping upload")
            return

        LOGGER.debug("Uploading metadata: %s", metadata)
        await self._upload_metadata(metadata)

        LOGGER.info("Device metadata reported successfully")

    async def _collect_metadata(self) -> Dict[str, Any]:
        """Collect metadata from all providers.

        Runs all providers concurrently and aggregates results.
        Provider failures are logged but don't fail the collection.

        Returns:
            Aggregated metadata dictionary.
        """
        # Run all providers concurrently
        results = await asyncio.gather(
            *(provider.detect() for provider in self._providers),
            return_exceptions=True,
        )

        metadata: Dict[str, Any] = {
            "system": {},
            "components": {},
        }

        for provider, result in zip(self._providers, results):
            if isinstance(result, Exception):
                LOGGER.warning(
                    "Provider '%s' failed: %s",
                    provider.name,
                    result,
                )
                continue

            if not isinstance(result, dict):
                LOGGER.warning(
                    "Provider '%s' returned invalid type: %s",
                    provider.name,
                    type(result).__name__,
                )
                continue

            # Merge system info
            if "system" in result:
                metadata["system"].update(result["system"])

            # Merge components
            if "components" in result:
                metadata["components"].update(result["components"])

        return metadata

    async def _upload_metadata(self, metadata: Dict[str, Any]) -> None:
        """Upload metadata to PrinterService API.

        Args:
            metadata: Aggregated metadata dictionary.

        Raises:
            Exception: If upload fails.
        """
        # Get token
        token = self._token_provider()
        if not token:
            raise RuntimeError("No JWT token available for metadata upload")

        # Build URL
        url = (
            f"{self._config.api_base_url.rstrip('/')}"
            f"/api/v1/devices/{self._config.device_id}/metadata"
        )

        # Ensure session
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self._config.request_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._owns_session = True

        # Build request payload - server derives hasTimelapse from components.timelapse
        # schemaVersion is required by the API validator
        payload = {
            "metadata": {
                "schemaVersion": "1.0",
                **metadata,
            },
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        try:
            async with asyncio.timeout(self._config.request_timeout):
                async with self._session.put(
                    url,
                    json=payload,
                    headers=headers,
                ) as response:
                    if response.status == 200:
                        LOGGER.debug("Metadata upload successful")
                        return
                    elif response.status == 401:
                        raise RuntimeError("Authentication failed (401)")
                    elif response.status == 403:
                        raise RuntimeError("Authorization failed (403)")
                    elif response.status == 404:
                        raise RuntimeError(f"Device not found: {self._config.device_id}")
                    else:
                        body = await response.text()
                        raise RuntimeError(
                            f"Upload failed with status {response.status}: {body[:200]}"
                        )
        except asyncio.TimeoutError:
            raise RuntimeError("Metadata upload timed out")
        except aiohttp.ClientError as exc:
            raise RuntimeError(f"HTTP error during upload: {exc}")

    @property
    def last_report_success(self) -> bool:
        """Whether the last report attempt succeeded."""
        return self._last_report_success

    @property
    def is_running(self) -> bool:
        """Whether the reporter is currently running."""
        return self._report_task is not None and not self._report_task.done()
