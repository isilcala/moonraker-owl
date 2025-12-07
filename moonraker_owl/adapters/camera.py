"""Camera capture adapter for webcam snapshot capture.

This module provides an async HTTP client for capturing webcam snapshots
from Moonraker's webcam service or compatible MJPEG/JPEG endpoints.

Most Klipper setups use crowsnest or ustreamer for webcam streaming,
which typically expose a snapshot endpoint at /webcam/?action=snapshot
or similar URLs.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiohttp

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class CaptureResult:
    """Result of a camera capture operation."""

    success: bool
    """Whether the capture succeeded."""

    image_data: Optional[bytes] = None
    """Raw image bytes if capture succeeded."""

    content_type: Optional[str] = None
    """MIME type of the captured image."""

    captured_at: Optional[datetime] = None
    """UTC timestamp when capture completed."""

    error_code: Optional[str] = None
    """Error code if capture failed."""

    error_message: Optional[str] = None
    """Human-readable error message if capture failed."""


class CameraClient:
    """Async client for capturing webcam snapshots.

    This client handles webcam snapshot capture from various sources:
    - Moonraker's built-in webcam proxy
    - crowsnest/ustreamer snapshot endpoints
    - Generic MJPEG snapshot URLs

    Features:
    - Automatic retry with exponential backoff
    - Content-Type validation
    - Timeout handling
    """

    def __init__(
        self,
        snapshot_url: str,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        max_retries: int = 2,
        base_retry_delay: float = 0.5,
        timeout: float = 10.0,
    ) -> None:
        """Initialize the camera capture client.

        Args:
            snapshot_url: URL for the webcam snapshot endpoint.
            session: Optional aiohttp session to use. If None, creates one.
            max_retries: Maximum number of retry attempts for failed captures.
            base_retry_delay: Base delay between retries (exponential backoff).
            timeout: Request timeout in seconds.
        """
        self._snapshot_url = snapshot_url
        self._session = session
        self._owns_session = session is None
        self._max_retries = max_retries
        self._base_retry_delay = base_retry_delay
        self._timeout = timeout

    @property
    def snapshot_url(self) -> str:
        """Get the configured snapshot URL."""
        return self._snapshot_url

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        """Close the HTTP session if we own it."""
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def capture(self) -> CaptureResult:
        """Capture a snapshot from the webcam.

        Returns:
            CaptureResult with image data if successful, or error details.
        """
        session = await self._ensure_session()
        last_error: Optional[Exception] = None

        for attempt in range(self._max_retries + 1):
            if attempt > 0:
                delay = self._base_retry_delay * (2 ** (attempt - 1))
                LOGGER.debug(
                    "Retry %d/%d for camera capture after %.1fs",
                    attempt,
                    self._max_retries,
                    delay,
                )
                await asyncio.sleep(delay)

            try:
                async with session.get(self._snapshot_url) as response:
                    if response.status == 200:
                        content_type = response.headers.get("Content-Type", "image/jpeg")

                        # Validate content type is an image
                        if not content_type.startswith("image/"):
                            return CaptureResult(
                                success=False,
                                error_code="invalid_content_type",
                                error_message=f"Expected image, got {content_type}",
                            )

                        image_data = await response.read()

                        if not image_data:
                            return CaptureResult(
                                success=False,
                                error_code="empty_response",
                                error_message="Camera returned empty image data",
                            )

                        LOGGER.debug(
                            "Captured %d bytes from camera (%s)",
                            len(image_data),
                            content_type,
                        )

                        return CaptureResult(
                            success=True,
                            image_data=image_data,
                            content_type=content_type,
                            captured_at=datetime.now(timezone.utc),
                        )

                    elif response.status == 404:
                        return CaptureResult(
                            success=False,
                            error_code="camera_not_found",
                            error_message=f"Webcam endpoint not found: {self._snapshot_url}",
                        )

                    elif response.status >= 500:
                        # Server error - worth retrying
                        last_error = Exception(f"Server error: {response.status}")
                        continue

                    else:
                        return CaptureResult(
                            success=False,
                            error_code="capture_failed",
                            error_message=f"HTTP {response.status} from webcam",
                        )

            except asyncio.TimeoutError:
                last_error = asyncio.TimeoutError("Camera capture timed out")
                LOGGER.warning("Camera capture timeout (attempt %d)", attempt + 1)
                continue

            except aiohttp.ClientError as e:
                last_error = e
                LOGGER.warning(
                    "Camera capture error (attempt %d): %s",
                    attempt + 1,
                    str(e),
                )
                continue

        # All retries exhausted
        error_msg = str(last_error) if last_error else "Unknown error"
        return CaptureResult(
            success=False,
            error_code="capture_failed",
            error_message=f"Camera capture failed after {self._max_retries + 1} attempts: {error_msg}",
        )

    async def is_available(self) -> bool:
        """Check if the camera is available by attempting a capture.

        Returns:
            True if camera responds successfully, False otherwise.
        """
        result = await self.capture()
        return result.success
