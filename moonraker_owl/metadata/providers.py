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
from ..config import extruder_index, is_extruder_object

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


class MotionProvider(BaseProvider):
    """Detects the printer's toolhead / motion profile (multi-toolhead support).

    Classifies the machine into a toolhead archetype using **standard Moonraker
    printer objects** (vendor-agnostic — see proposal
    ``multi-toolhead-klipper-support.md``):

    - ``extruderCount``: number of ``extruder`` / ``extruderN`` objects
    - ``idex``: presence of ``dual_carriage``
    - ``toolchanger``: presence of ``toolchanger`` or ``tool <name>`` objects
    - ``mmu``: presence of ``mmu`` (Happy Hare / ERCF / Box Turtle)
    - ``kinematics``: best-effort from the ``configfile`` settings

    The result is merged under ``components.motion`` of the ADR-0032 metadata
    document and consumed by PrinterService to derive ``ToolheadKind`` /
    ``ExtruderCount`` on the printer.
    """

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
        return "motion"

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
        """Detect the toolhead/motion profile.

        Returns:
            Dictionary with a ``components.motion`` entry, or ``{}`` if the
            printer objects could not be queried (Klipper not ready).
        """
        session = await self._ensure_session()
        objects = await self._fetch_objects(session)
        if not objects:
            return {}

        extruders = sorted(
            (obj for obj in objects if is_extruder_object(obj)),
            key=extruder_index,
        )
        idex = "dual_carriage" in objects
        toolchanger = "toolchanger" in objects or any(
            obj == "tool" or obj.startswith("tool ") for obj in objects
        )
        mmu = "mmu" in objects

        motion: Dict[str, Any] = {
            "domain": "motion",
            "extruderCount": len(extruders),
            "idex": idex,
            "toolchanger": toolchanger,
            "mmu": mmu,
            "tools": [
                {"id": name, "index": extruder_index(name)} for name in extruders
            ],
        }

        kinematics = await self._fetch_kinematics(session)
        if kinematics:
            motion["kinematics"] = kinematics

        return {"components": {"motion": motion}}

    async def _fetch_objects(self, session: aiohttp.ClientSession) -> List[str]:
        """Query the Moonraker printer objects list."""
        url = f"{self._base_url}/printer/objects/list"
        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        LOGGER.warning(
                            "Moonraker objects query returned HTTP %d — "
                            "Klipper may not be ready yet",
                            response.status,
                        )
                        return []
                    data = await response.json()
                    return data.get("result", {}).get("objects", [])
        except Exception as exc:
            LOGGER.warning("Failed to query objects for motion profile: %s", exc)
            return []

    async def _fetch_kinematics(
        self, session: aiohttp.ClientSession
    ) -> Optional[str]:
        """Best-effort fetch of ``[printer] kinematics`` from the configfile.

        Kinematics is display-only metadata, so any failure here is non-fatal
        and simply omits the field.
        """
        url = f"{self._base_url}/printer/objects/query?configfile=settings"
        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        return None
                    data = await response.json()
        except Exception as exc:
            LOGGER.debug("Failed to query configfile for kinematics: %s", exc)
            return None

        settings = (
            data.get("result", {})
            .get("status", {})
            .get("configfile", {})
            .get("settings", {})
        )
        kinematics = settings.get("printer", {}).get("kinematics")
        if isinstance(kinematics, str) and kinematics.strip():
            return kinematics.strip()
        return None


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

    # Non-thermal object prefixes discoverable from printer objects list.
    # Maps prefix → sensor type string.
    _OBJECT_TYPE_MAP: tuple[tuple[str, str], ...] = (
        ("heater_fan ",               "fan"),
        ("controller_fan ",           "fan"),
        ("fan_generic ",              "fan"),
        ("neopixel ",                 "led"),
        ("dotstar ",                  "led"),
        ("led ",                      "led"),
        ("output_pin ",               "output_pin"),
        ("filament_switch_sensor ",   "filament_sensor"),
        ("filament_motion_sensor ",   "filament_sensor"),
    )

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

        # Discover non-thermal devices from printer objects list
        for obj_name in objects:
            if not obj_name or obj_name.startswith("_"):
                continue
            # Skip hidden objects (prefixed with _ after the type prefix)
            if " " in obj_name and obj_name.split(" ", 1)[1].startswith("_"):
                continue
            if obj_name in seen:
                continue
            obj_type = self._object_type(obj_name)
            if obj_type is not None or obj_name == "fan":
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
        # Check non-thermal object type map (fans, LEDs, pins, filament)
        obj_type = self._object_type(name)
        if obj_type is not None:
            return obj_type
        # Bare "fan" exact match
        if name == "fan":
            return "fan"
        return "unknown"

    def _object_type(self, name: str) -> Optional[str]:
        """Return the sensor type for a non-thermal object, or None."""
        for prefix, sensor_type in self._OBJECT_TYPE_MAP:
            if name.startswith(prefix):
                return sensor_type
        return None

    async def _fetch_heaters(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """Query the Moonraker heaters endpoint."""
        url = f"{self._base_url}/printer/objects/query?heaters"
        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        LOGGER.warning(
                            "Moonraker heaters query returned HTTP %d — "
                            "Klipper may not be ready yet",
                            response.status,
                        )
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
                        LOGGER.warning(
                            "Moonraker objects query returned HTTP %d — "
                            "Klipper may not be ready yet",
                            response.status,
                        )
                        return []
                    data = await response.json()
                    return data.get("result", {}).get("objects", [])
        except Exception as exc:
            LOGGER.warning("Failed to query objects for sensor inventory: %s", exc)
            return []


class GCodeMacroProvider(BaseProvider):
    """Discovers user-defined GCode macros from Klipper.

    Queries Moonraker for ``gcode_macro *`` printer objects and the
    ``/printer/gcode/help`` endpoint to collect macro names with their
    help-text descriptions.  Internal macros (prefixed with ``_``) and
    macros that use ``rename_existing`` (wrappers for built-in commands)
    are excluded.

    The cloud consumes this data to populate the macro configuration UI,
    allowing users to select which macros appear on the dashboard panel.
    """

    # Macros commonly known to be destructive / safety-critical.
    _DANGEROUS_MACROS: frozenset[str] = frozenset({
        "M112",
        "FIRMWARE_RESTART",
        "RESTART",
        "TURN_OFF_HEATERS",
        "EMERGENCY_STOP",
    })

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
        return "macros"

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
        """Detect all user-defined GCode macros.

        Returns:
            Dictionary with 'macros' key containing:
            - available: list of macro descriptors with name, description,
              dangerous flag.
        """
        session = await self._ensure_session()

        objects, gcode_help = await asyncio.gather(
            self._fetch_objects(session),
            self._fetch_gcode_help(session),
        )

        # Collect macro names from printer objects (gcode_macro <NAME>)
        macro_names: List[str] = []
        for obj_name in objects:
            if not obj_name.startswith("gcode_macro "):
                continue
            macro_name = obj_name[len("gcode_macro "):]
            # Skip internal macros (prefixed with _)
            if macro_name.startswith("_"):
                continue
            macro_names.append(macro_name)

        # Filter out rename_existing wrappers by querying macro configs
        wrapper_names = await self._detect_wrappers(session, macro_names)

        macros: List[Dict[str, Any]] = []
        for name in sorted(macro_names):
            if name in wrapper_names:
                continue
            macros.append({
                "name": name,
                "description": gcode_help.get(name, ""),
                "dangerous": name.upper() in self._DANGEROUS_MACROS,
            })

        return {"macros": {"available": macros}}

    async def _fetch_objects(self, session: aiohttp.ClientSession) -> List[str]:
        """Query Moonraker printer objects list."""
        url = f"{self._base_url}/printer/objects/list"
        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        LOGGER.warning(
                            "Moonraker objects query returned HTTP %d",
                            response.status,
                        )
                        return []
                    data = await response.json()
                    return data.get("result", {}).get("objects", [])
        except Exception as exc:
            LOGGER.warning("Failed to query objects for macro discovery: %s", exc)
            return []

    async def _fetch_gcode_help(
        self, session: aiohttp.ClientSession
    ) -> Dict[str, str]:
        """Query /printer/gcode/help for macro descriptions.

        Returns a mapping of uppercase macro name → help text.
        """
        url = f"{self._base_url}/printer/gcode/help"
        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        LOGGER.warning(
                            "Moonraker gcode/help returned HTTP %d",
                            response.status,
                        )
                        return {}
                    data = await response.json()
                    return data.get("result", {})
        except Exception as exc:
            LOGGER.warning("Failed to query gcode/help for macros: %s", exc)
            return {}

    async def _detect_wrappers(
        self,
        session: aiohttp.ClientSession,
        macro_names: List[str],
    ) -> set[str]:
        """Detect macros that use rename_existing (built-in wrappers).

        Queries the gcode_macro object config for each macro. Macros with
        ``rename_existing`` set are wrappers around built-in commands and
        should not be exposed to users as runnable macros.
        """
        if not macro_names:
            return set()

        # Build query for all macro configs in one request
        query_parts = "&".join(
            f"gcode_macro {name}" for name in macro_names
        )
        url = f"{self._base_url}/printer/objects/query?{query_parts}"
        try:
            async with asyncio.timeout(self._timeout):
                async with session.get(url) as response:
                    if response.status != 200:
                        return set()
                    data = await response.json()
                    status = data.get("result", {}).get("status", {})
        except Exception:
            return set()

        wrappers: set[str] = set()
        for name in macro_names:
            obj_key = f"gcode_macro {name}"
            macro_info = status.get(obj_key, {})
            if macro_info.get("rename_existing"):
                wrappers.add(name)
        return wrappers
