"""Camera auto-discovery for Moonraker-configured webcams.

This module provides automatic camera URL detection by querying
Moonraker's webcam API. It supports:

1. Moonraker webcam list API (/server/webcams/list)
2. Fallback probing of common webcam ports (80, 8080)
3. Manual URL configuration (bypass auto-discovery)

Usage:
    discovery = CameraDiscovery(moonraker_client)
    url = await discovery.discover_snapshot_url()
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, List
from urllib.parse import urljoin, urlparse

import aiohttp

LOGGER = logging.getLogger(__name__)

# Common snapshot URL patterns to probe as fallback
FALLBACK_PROBE_URLS = [
    "http://127.0.0.1/webcam/?action=snapshot",      # crowsnest via nginx (port 80)
    "http://127.0.0.1:8080/?action=snapshot",        # mjpg-streamer direct
    "http://127.0.0.1/webcam/snapshot",              # Alternative crowsnest path
    "http://127.0.0.1:8080/snapshot",                # ustreamer
]


@dataclass(slots=True)
class WebcamInfo:
    """Information about a discovered webcam."""

    name: str
    """Webcam name as configured in Moonraker."""

    snapshot_url: str
    """Resolved absolute URL for snapshot capture."""

    stream_url: Optional[str] = None
    """Optional stream URL."""

    location: Optional[str] = None
    """Webcam location description."""


class CameraDiscovery:
    """Auto-discovers webcam configuration from Moonraker.

    This class queries Moonraker's webcam API to find configured cameras
    and resolves relative URLs to absolute URLs for direct access.

    The discovery result is cached until invalidate() is called, which
    should happen on Moonraker reconnection.
    """

    def __init__(
        self,
        moonraker_base_url: str,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        probe_timeout: float = 3.0,
    ) -> None:
        """Initialize the camera discovery.

        Args:
            moonraker_base_url: Base URL for Moonraker API (e.g., http://127.0.0.1:7125)
            session: Optional aiohttp session to use.
            probe_timeout: Timeout for probe requests in seconds.
        """
        self._moonraker_base_url = moonraker_base_url.rstrip("/")
        self._session = session
        self._owns_session = session is None
        self._probe_timeout = probe_timeout

        # Cache
        self._cached_webcams: Optional[List[WebcamInfo]] = None
        self._cached_snapshot_url: Optional[str] = None
        self._discovery_attempted: bool = False

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self._probe_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        """Close the HTTP session if we own it."""
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    def invalidate(self) -> None:
        """Invalidate the cached discovery results.

        Call this when Moonraker reconnects to ensure fresh webcam list.
        """
        self._cached_webcams = None
        self._cached_snapshot_url = None
        self._discovery_attempted = False
        LOGGER.debug("Camera discovery cache invalidated")

    async def get_webcams(self) -> List[WebcamInfo]:
        """Get list of configured webcams from Moonraker.

        Returns cached results if available. Call invalidate() to refresh.

        Returns:
            List of WebcamInfo objects, empty if no webcams configured.
        """
        if self._cached_webcams is not None:
            return self._cached_webcams

        webcams = await self._fetch_webcams_from_moonraker()
        self._cached_webcams = webcams
        return webcams

    async def discover_snapshot_url(
        self,
        camera_name: str = "auto",
    ) -> Optional[str]:
        """Discover the snapshot URL for camera capture.

        Discovery priority:
        1. Moonraker webcam API
        2. Fallback port probing (80, 8080)

        Args:
            camera_name: Specific camera name to use, or "auto" for first available.

        Returns:
            Absolute snapshot URL if found, None if no camera available.
        """
        if self._cached_snapshot_url is not None:
            return self._cached_snapshot_url

        if self._discovery_attempted:
            # Already tried and failed
            return None

        self._discovery_attempted = True

        # Step 1: Try Moonraker webcam API
        url = await self._discover_from_moonraker(camera_name)
        if url:
            self._cached_snapshot_url = url
            return url

        # Step 2: Fallback to port probing
        url = await self._probe_fallback_urls()
        if url:
            self._cached_snapshot_url = url
            return url

        LOGGER.warning(
            "No camera found via Moonraker API or fallback probing. "
            "Camera capture will be disabled."
        )
        return None

    async def _fetch_webcams_from_moonraker(self) -> List[WebcamInfo]:
        """Fetch webcam list from Moonraker API."""
        session = await self._ensure_session()
        url = f"{self._moonraker_base_url}/server/webcams/list"

        try:
            async with asyncio.timeout(self._probe_timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        LOGGER.debug(
                            "Moonraker webcam API returned %d",
                            response.status,
                        )
                        return []

                    data = await response.json()

            result = data.get("result", {})
            webcams_data = result.get("webcams", [])

            webcams: List[WebcamInfo] = []
            for cam in webcams_data:
                name = cam.get("name", "unknown")
                snapshot_url = cam.get("snapshot_url", "")

                if not snapshot_url:
                    LOGGER.debug("Webcam '%s' has no snapshot_url, skipping", name)
                    continue

                # Resolve relative URL to absolute
                resolved_url = self._resolve_webcam_url(snapshot_url)

                webcams.append(
                    WebcamInfo(
                        name=name,
                        snapshot_url=resolved_url,
                        stream_url=self._resolve_webcam_url(cam.get("stream_url", "")),
                        location=cam.get("location"),
                    )
                )

            LOGGER.debug(
                "Discovered %d webcam(s) from Moonraker: %s",
                len(webcams),
                [w.name for w in webcams],
            )
            return webcams

        except asyncio.TimeoutError:
            LOGGER.debug("Moonraker webcam API timed out")
            return []
        except Exception as e:
            LOGGER.debug("Failed to fetch webcams from Moonraker: %s", e)
            return []

    def _resolve_webcam_url(self, url: str) -> str:
        """Resolve a webcam URL to an absolute URL.

        Moonraker returns URLs in various formats:
        - Absolute: http://192.168.1.100:8080/snapshot
        - Relative to nginx (port 80): /webcam/?action=snapshot
        - Relative to Moonraker: webcam/?action=snapshot

        For relative URLs, we assume they're served via nginx on port 80,
        which is the standard Klipper setup.
        """
        if not url:
            return ""

        parsed = urlparse(url)

        # Already absolute
        if parsed.scheme and parsed.netloc:
            return url

        # Relative URL - resolve against localhost:80 (nginx)
        # This is the standard Klipper setup with crowsnest/nginx
        base_url = "http://127.0.0.1"

        if url.startswith("/"):
            return f"{base_url}{url}"
        else:
            return f"{base_url}/{url}"

    async def _discover_from_moonraker(self, camera_name: str) -> Optional[str]:
        """Discover snapshot URL from Moonraker webcam API."""
        webcams = await self.get_webcams()

        if not webcams:
            LOGGER.debug("No webcams found in Moonraker configuration")
            return None

        # Handle None or empty camera_name as "auto"
        effective_name = camera_name.lower() if camera_name else "auto"

        # Find the requested camera
        if effective_name == "auto":
            # Use first available
            selected = webcams[0]
            LOGGER.info(
                "Auto-selected webcam '%s': %s",
                selected.name,
                selected.snapshot_url,
            )
        else:
            # Find by name
            selected = next(
                (w for w in webcams if w.name.lower() == effective_name),
                None,
            )
            if selected is None:
                available = [w.name for w in webcams]
                LOGGER.warning(
                    "Webcam '%s' not found. Available: %s. Using first available.",
                    effective_name,
                    available,
                )
                selected = webcams[0]

        # Verify the URL is reachable
        if await self._probe_url(selected.snapshot_url):
            return selected.snapshot_url

        LOGGER.warning(
            "Webcam '%s' URL not reachable: %s",
            selected.name,
            selected.snapshot_url,
        )
        return None

    async def _probe_fallback_urls(self) -> Optional[str]:
        """Probe common webcam URLs as fallback."""
        LOGGER.debug("Probing fallback webcam URLs...")

        for url in FALLBACK_PROBE_URLS:
            if await self._probe_url(url):
                LOGGER.info("Found webcam via fallback probe: %s", url)
                return url

        return None

    async def _probe_url(self, url: str) -> bool:
        """Probe a URL to check if it returns a valid image.

        Args:
            url: URL to probe.

        Returns:
            True if URL returns HTTP 200 with image content-type.
        """
        session = await self._ensure_session()

        try:
            async with asyncio.timeout(self._probe_timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        return False

                    content_type = response.headers.get("Content-Type", "")
                    if content_type.startswith("image/"):
                        LOGGER.debug("Probe successful: %s (%s)", url, content_type)
                        return True

                    LOGGER.debug(
                        "Probe failed (wrong content-type): %s (%s)",
                        url,
                        content_type,
                    )
                    return False

        except asyncio.TimeoutError:
            LOGGER.debug("Probe timed out: %s", url)
            return False
        except Exception as e:
            LOGGER.debug("Probe failed: %s (%s)", url, e)
            return False
