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


class SensorInventoryProvider(BaseProvider):
    """Provides a type-classified inventory of all available printer sensors.

    Discovers sensors from Moonraker's heaters endpoint and printer objects
    list, classifying each by type (heater, sensor, fan) and whether it
    supports target temperature control.

    This data is consumed by the cloud to populate the sensor configuration
    UI, allowing users to select which sensors to monitor.
    """

    # Sensor name prefixes that indicate temperature-related objects
    _HEATER_PREFIXES = ("extruder", "heater_generic", "heater_bed")
    _SENSOR_PREFIXES = ("temperature_sensor", "temperature_fan")
    _FAN_PREFIXES = ("fan", "heater_fan", "controller_fan", "fan_generic")

    def __init__(
        self,
        base_url: str,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: float = 5.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._session = session
        self._owns_session = session is None
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "sensors"

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def detect(self) -> Dict[str, Any]:
        """Detect all available sensors and return a classified inventory.

        Returns:
            Dictionary with 'sensors' key containing:
            - available: list of sensor descriptors with name, type, hasTarget
        """
        session = await self._ensure_session()

        heater_info, objects = await asyncio.gather(
            self._fetch_heaters(session),
            self._fetch_objects(session),
        )

        sensors: List[Dict[str, Any]] = []
        seen: set[str] = set()

        # Process heaters (have target temperature)
        for heater in heater_info.get("available_heaters", []):
            if not heater or heater.startswith("_"):
                continue
            if heater not in seen:
                sensors.append(self._classify_sensor(heater, has_target=True))
                seen.add(heater)

        # Process temperature sensors (no target)
        for sensor in heater_info.get("available_sensors", []):
            if not sensor or sensor.startswith("_"):
                continue
            # Skip hidden sensors (prefixed with _ after the type prefix)
            if " " in sensor and sensor.split(" ", 1)[1].startswith("_"):
                continue
            if sensor in seen or sensor in heater_info.get("available_heaters", []):
                continue
            has_target = sensor.startswith("temperature_fan")
            sensors.append(self._classify_sensor(sensor, has_target=has_target))
            seen.add(sensor)

        # Discover fans from printer objects list
        for obj_name in objects:
            if not obj_name or obj_name.startswith("_"):
                continue
            if obj_name in seen:
                continue
            if self._is_fan_object(obj_name):
                sensors.append(self._classify_sensor(obj_name, has_target=False))
                seen.add(obj_name)

        return {"sensors": {"available": sensors}}

    def _classify_sensor(self, name: str, *, has_target: bool) -> Dict[str, str | bool]:
        """Classify a sensor by its Klipper object name."""
        return {
            "name": name,
            "type": self._infer_type(name),
            "hasTarget": has_target,
        }

    def _infer_type(self, name: str) -> str:
        """Infer sensor type from Klipper object name."""
        if name in ("extruder",) or name.startswith("extruder"):
            return "extruder"
        if name == "heater_bed":
            return "heater_bed"
        if name.startswith("heater_generic"):
            return "heater"
        if name.startswith("temperature_fan"):
            return "temperature_fan"
        if name.startswith("temperature_sensor"):
            return "temperature_sensor"
        if self._is_fan_object(name):
            return "fan"
        return "unknown"

    def _is_fan_object(self, name: str) -> bool:
        """Check if a Klipper object name represents a fan."""
        return name == "fan" or any(
            name.startswith(prefix) for prefix in self._FAN_PREFIXES
        )

    async def _fetch_heaters(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """Query the Moonraker heaters endpoint."""
        url = f"{self._base_url}/printer/objects/query?heaters"
        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        return {}
                    data = await response.json()
                    return data.get("result", {}).get("status", {}).get("heaters", {})
        except Exception as exc:
            LOGGER.warning("Failed to query heaters for sensor inventory: %s", exc)
            return {}

    async def _fetch_objects(self, session: aiohttp.ClientSession) -> List[str]:
        """Query the Moonraker printer objects list."""
        url = f"{self._base_url}/printer/objects/list"
        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        return []
                    data = await response.json()
                    return data.get("result", {}).get("objects", [])
        except Exception as exc:
            LOGGER.warning("Failed to query objects for sensor inventory: %s", exc)
            return []
