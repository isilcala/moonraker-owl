"""Capability providers for device metadata collection.

Each provider is responsible for detecting a specific category of capabilities
from the device. Providers are isolated from each other - one provider's failure
should not affect others.

Providers implement the CapabilityProvider protocol and return partial metadata
dictionaries that are merged by MetadataReporter.
"""

from __future__ import annotations

import asyncio
import logging
import platform
import socket
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol

import aiohttp

from ..version import __version__

LOGGER = logging.getLogger(__name__)


class CapabilityProvider(Protocol):
    """Protocol for capability detection providers."""

    @property
    def name(self) -> str:
        """Provider name for logging and debugging."""
        ...

    async def detect(self) -> Dict[str, Any]:
        """Detect capabilities and return partial metadata.

        Returns:
            Dictionary with optional keys:
            - "system": system-level info to merge
            - "components": component entries to add

        Raises:
            Exception: If detection fails (will be caught by MetadataReporter)
        """
        ...


class BaseProvider(ABC):
    """Base class for capability providers with common functionality."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name for logging."""
        ...

    @abstractmethod
    async def detect(self) -> Dict[str, Any]:
        """Detect capabilities."""
        ...


class SystemInfoProvider(BaseProvider):
    """Provides system-level information about the agent host."""

    @property
    def name(self) -> str:
        return "system"

    async def detect(self) -> Dict[str, Any]:
        """Detect system information.

        Returns:
            Dictionary with 'system' key containing agent and OS info.
        """
        hostname = socket.gethostname()
        os_name = platform.system()
        os_release = platform.release()

        return {
            "system": {
                "agentVersion": __version__,
                "os": os_name,
                "osRelease": os_release,
                "hostname": hostname,
                "pythonVersion": platform.python_version(),
            }
        }


@dataclass(slots=True)
class MoonrakerEndpoints:
    """Moonraker API endpoints for capability detection."""

    server_info: str = "/server/info"
    printer_info: str = "/printer/info"
    webcams_list: str = "/server/webcams/list"


class MoonrakerProvider(BaseProvider):
    """Provides Moonraker server capabilities.

    Detects:
    - Moonraker version
    - Installed components/plugins (timelapse, spoolman, etc.)
    """

    def __init__(
        self,
        base_url: str,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: float = 5.0,
    ) -> None:
        """Initialize the Moonraker provider.

        Args:
            base_url: Moonraker base URL (e.g., http://127.0.0.1:7125)
            session: Optional aiohttp session to use.
            timeout: Request timeout in seconds.
        """
        self._base_url = base_url.rstrip("/")
        self._session = session
        self._owns_session = session is None
        self._timeout = timeout
        self._endpoints = MoonrakerEndpoints()

    @property
    def name(self) -> str:
        return "moonraker"

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

    async def detect(self) -> Dict[str, Any]:
        """Detect Moonraker capabilities.

        Returns:
            Dictionary with 'components' key containing moonraker info
            and any detected plugin components.
        """
        session = await self._ensure_session()
        url = f"{self._base_url}{self._endpoints.server_info}"

        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        LOGGER.warning(
                            "Moonraker server info returned %d",
                            response.status,
                        )
                        return {}

                    data = await response.json()
        except asyncio.TimeoutError:
            LOGGER.warning("Moonraker server info request timed out")
            raise
        except aiohttp.ClientError as exc:
            LOGGER.warning("Failed to fetch Moonraker server info: %s", exc)
            raise

        result = data.get("result", {})
        components = result.get("components", [])
        moonraker_version = result.get("moonraker_version", "unknown")

        output: Dict[str, Any] = {
            "components": {
                "moonraker": {
                    "domain": "service",
                    "version": moonraker_version,
                    "plugins": self._extract_plugins(components),
                }
            }
        }

        # Add timelapse component if present
        if "timelapse" in components:
            output["components"]["timelapse"] = {
                "domain": "plugin",
                "enabled": True,
            }

        # Add spoolman component if present
        if "spoolman" in components:
            output["components"]["spoolman"] = {
                "domain": "plugin",
                "enabled": True,
            }

        return output

    def _extract_plugins(self, components: List[str]) -> List[str]:
        """Extract plugin names from component list.

        Moonraker components include both core components and plugins.
        We filter for known plugin patterns.
        """
        # Known optional plugins/components
        plugin_names = {
            "timelapse",
            "spoolman",
            "octoprint_compat",
            "mqtt",
            "power",
            "update_manager",
            "announcements",
            "sensor",
        }
        return [c for c in components if c in plugin_names]


class KlipperProvider(BaseProvider):
    """Provides Klipper firmware capabilities.

    Detects:
    - Klipper version
    - Enabled features (exclude_object, input_shaper, etc.)
    """

    def __init__(
        self,
        base_url: str,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: float = 5.0,
    ) -> None:
        """Initialize the Klipper provider.

        Args:
            base_url: Moonraker base URL (e.g., http://127.0.0.1:7125)
            session: Optional aiohttp session to use.
            timeout: Request timeout in seconds.
        """
        self._base_url = base_url.rstrip("/")
        self._session = session
        self._owns_session = session is None
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "klipper"

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

    async def detect(self) -> Dict[str, Any]:
        """Detect Klipper capabilities.

        Returns:
            Dictionary with 'components' key containing klipper info.
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/printer/info"

        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        LOGGER.warning(
                            "Klipper printer info returned %d",
                            response.status,
                        )
                        return {}

                    data = await response.json()
        except asyncio.TimeoutError:
            LOGGER.warning("Klipper printer info request timed out")
            raise
        except aiohttp.ClientError as exc:
            LOGGER.warning("Failed to fetch Klipper printer info: %s", exc)
            raise

        result = data.get("result", {})
        software_version = result.get("software_version", "unknown")
        hostname = result.get("hostname", "")

        # Detect features from printer objects
        features = await self._detect_features(session)

        output: Dict[str, Any] = {
            "components": {
                "klipper": {
                    "domain": "firmware",
                    "version": software_version,
                    "features": features,
                }
            }
        }

        # Include hostname in system info if available
        if hostname:
            output["system"] = {"klipperHostname": hostname}

        return output

    async def _detect_features(
        self, session: aiohttp.ClientSession
    ) -> List[str]:
        """Detect Klipper features from available printer objects.

        Common features to detect:
        - exclude_object: Object exclusion during print
        - input_shaper: Input shaping for vibration reduction
        - bed_mesh: Automatic bed leveling
        - pressure_advance: Extruder pressure advance
        """
        features: List[str] = []

        # Query printer objects to detect features
        url = f"{self._base_url}/printer/objects/list"

        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        return features

                    data = await response.json()
        except Exception:
            return features

        result = data.get("result", {})
        objects = result.get("objects", [])

        # Map object names to features
        feature_objects = {
            "exclude_object": "exclude_object",
            "input_shaper": "input_shaper",
            "bed_mesh": "bed_mesh",
            "resonance_tester": "resonance_tester",
            "adxl345": "accelerometer",
        }

        for obj_name, feature_name in feature_objects.items():
            if obj_name in objects:
                features.append(feature_name)

        return features


class CameraProvider(BaseProvider):
    """Provides camera capability information.

    Detects webcams configured in Moonraker.
    """

    def __init__(
        self,
        base_url: str,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: float = 5.0,
    ) -> None:
        """Initialize the camera provider.

        Args:
            base_url: Moonraker base URL (e.g., http://127.0.0.1:7125)
            session: Optional aiohttp session to use.
            timeout: Request timeout in seconds.
        """
        self._base_url = base_url.rstrip("/")
        self._session = session
        self._owns_session = session is None
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "camera"

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

    async def detect(self) -> Dict[str, Any]:
        """Detect camera capabilities.

        Returns:
            Dictionary with 'components' key containing camera entries.
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/server/webcams/list"

        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        LOGGER.debug(
                            "Webcam list API returned %d",
                            response.status,
                        )
                        return {}

                    data = await response.json()
        except asyncio.TimeoutError:
            LOGGER.debug("Webcam list request timed out")
            return {}
        except aiohttp.ClientError as exc:
            LOGGER.debug("Failed to fetch webcam list: %s", exc)
            return {}

        result = data.get("result", {})
        webcams = result.get("webcams", [])

        if not webcams:
            return {}

        output: Dict[str, Any] = {"components": {}}

        for i, cam in enumerate(webcams):
            name = cam.get("name", f"camera_{i}")
            component_key = f"camera.{name}" if name != "default" else "camera.main"

            output["components"][component_key] = {
                "domain": "camera",
                "name": name,
                "service": cam.get("service", "unknown"),
                "features": self._extract_camera_features(cam),
            }

        return output

    def _extract_camera_features(self, cam: Dict[str, Any]) -> List[str]:
        """Extract available features from webcam config."""
        features: List[str] = []

        if cam.get("snapshot_url"):
            features.append("snapshot")
        if cam.get("stream_url"):
            features.append("stream")

        return features
