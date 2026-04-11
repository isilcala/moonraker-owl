"""Moonraker-specific telemetry bridge.

Isolates Moonraker-specific domain knowledge (heater naming patterns,
sensor discovery, print event enrichment helpers) from the generic
TelemetryPublisher pipeline.

The ``TelemetrySource`` protocol defines the data-source contract that
TelemetryPublisher depends on; ``MoonrakerBridge`` implements it by
wrapping a ``PrinterAdapter``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Mapping, Optional, Protocol

from .selectors import SensorFilter

LOGGER = logging.getLogger(__name__)

# ── Type aliases ────────────────────────────────────────────────────────────

CallbackType = Callable[[dict[str, Any]], Awaitable[None] | None]


# ── TelemetrySource Protocol ───────────────────────────────────────────────


class TelemetrySource(Protocol):
    """Minimal contract for a telemetry data source.

    TelemetryPublisher depends on this protocol rather than on the
    concrete Moonraker adapter, allowing alternative backends or
    in-memory test doubles.
    """

    async def start(self, callback: CallbackType) -> None: ...

    def remove_callback(self, callback: CallbackType) -> None: ...

    def set_subscription_objects(
        self, objects: Mapping[str, Optional[list[str]]] | None
    ) -> None: ...

    async def fetch_printer_state(
        self,
        objects: Optional[Mapping[str, Optional[list[str]]]] = None,
        timeout: float = 5.0,
    ) -> dict[str, Any]: ...

    async def fetch_available_heaters(
        self, timeout: float = 5.0
    ) -> dict[str, list[str]]: ...

    async def fetch_registered_objects(
        self, timeout: float = 5.0
    ) -> list[str]: ...

    async def fetch_most_recent_job(
        self, timeout: float = 5.0
    ) -> Optional[dict[str, Any]]: ...

    async def resubscribe(self) -> None: ...

    # Extended methods (available on MoonrakerBridge, not required by all
    # implementations – publisher guards calls with hasattr()).

    async def fetch_gcode_metadata(
        self, filename: str, timeout: float = 5.0
    ) -> Optional[dict[str, Any]]: ...

    async def list_timelapse_files(
        self, timeout: float = 5.0
    ) -> list[dict[str, Any]]: ...


# ── Moonraker-specific helpers ─────────────────────────────────────────────


def is_heater_object(obj_name: str) -> bool:
    """Return True when the Moonraker object represents a temperature device."""
    return (
        obj_name in ("extruder", "heater_bed")
        or obj_name.startswith("extruder")
        or obj_name.startswith("heater_generic")
        or obj_name.startswith("temperature_sensor")
        or obj_name.startswith("temperature_fan")
    )


def heater_has_target(obj_name: str) -> bool:
    """Return True when the temperature object supports setting a target."""
    if obj_name.startswith("temperature_sensor"):
        return False
    return is_heater_object(obj_name)


# Mapping from Moonraker object prefix → subscription fields for non-thermal devices.
_NON_THERMAL_PREFIXES: tuple[tuple[str, list[str]], ...] = (
    ("neopixel ",                 ["color_data"]),
    ("dotstar ",                  ["color_data"]),
    ("led ",                      ["color_data"]),
    ("heater_fan ",               ["speed"]),
    ("controller_fan ",           ["speed"]),
    ("fan_generic ",              ["speed"]),
    ("filament_switch_sensor ",   ["filament_detected", "enabled"]),
    ("filament_motion_sensor ",   ["filament_detected", "enabled"]),
    ("output_pin ",               ["value"]),
)


def is_discoverable_object(obj_name: str) -> bool:
    """Return True for any dynamically-discovered sensor (thermal or non-thermal)."""
    if is_heater_object(obj_name) or obj_name == "fan":
        return True
    return any(obj_name.startswith(prefix) for prefix, _ in _NON_THERMAL_PREFIXES)


def _subscription_fields_for(obj_name: str) -> Optional[list[str]]:
    """Return subscription fields for a non-thermal object, or None."""
    for prefix, fields in _NON_THERMAL_PREFIXES:
        if obj_name.startswith(prefix):
            return list(fields)
    return None


def discover_moonraker_sensors(
    source: TelemetrySource,
    heater_info: dict[str, list[str]],
    current_objects: dict[str, Optional[list[str]]],
    sensor_filter: SensorFilter,
    *,
    registered_objects: Optional[list[str]] = None,
    static_objects: frozenset[str] = frozenset(),
) -> list[str]:
    """Build updated subscription objects from Moonraker heater discovery.

    Examines ``heater_info`` (as returned by ``source.fetch_available_heaters()``)
    and ``registered_objects`` (as returned by ``source.fetch_registered_objects()``)
    and adds any new heaters/sensors/devices to *current_objects* **in-place**,
    honouring the *sensor_filter* allowlist/denylist rules.  Also removes
    dynamically-discovered sensors that Moonraker no longer reports.

    Args:
        registered_objects: Full list of Moonraker printer objects, used to
            discover non-thermal devices (LED, filament sensors, output pins).
        static_objects: Keys from the initial manifest that should never be
            removed by stale-sensor cleanup (user-configured subscriptions).

    Returns:
        List of newly added object names.
    """
    available_heaters = heater_info.get("available_heaters", [])
    available_sensors = heater_info.get("available_sensors", [])

    # Set of all sensors currently reported by Moonraker
    reported: set[str] = set(available_heaters) | set(available_sensors) | {"fan"}

    added_objects: list[str] = []

    # Always subscribe to main fan (part cooling fan) if not already present
    if "fan" not in current_objects:
        current_objects["fan"] = ["speed"]
        added_objects.append("fan")

    # Add heaters (extruder, heater_bed, heater_generic xxx)
    for heater in available_heaters:
        if heater and heater not in current_objects:
            if heater.startswith("_") or (
                " " in heater and heater.split(" ", 1)[1].startswith("_")
            ):
                continue
            if not sensor_filter.is_allowed(heater):
                continue
            current_objects[heater] = ["temperature", "target"]
            added_objects.append(heater)

    # Add sensors (temperature_sensor xxx, temperature_fan xxx, etc.)
    for sensor in available_sensors:
        if sensor and sensor not in current_objects:
            if sensor.startswith("_") or (
                " " in sensor and sensor.split(" ", 1)[1].startswith("_")
            ):
                continue
            if sensor in available_heaters:
                continue
            if not sensor_filter.is_allowed(sensor):
                continue
            if sensor.startswith("temperature_fan"):
                current_objects[sensor] = ["temperature", "target", "speed"]
            else:
                current_objects[sensor] = ["temperature"]
            added_objects.append(sensor)

    # Discover non-thermal devices from the full registered objects list
    if registered_objects:
        for obj in registered_objects:
            if not obj or obj in current_objects:
                continue
            if obj.startswith("_") or (
                " " in obj and obj.split(" ", 1)[1].startswith("_")
            ):
                continue
            fields = _subscription_fields_for(obj)
            if fields is None:
                continue
            if not sensor_filter.is_allowed(obj):
                continue
            current_objects[obj] = fields
            added_objects.append(obj)
            reported.add(obj)

        # Also mark all currently-known non-thermal objects as reported
        for obj in registered_objects:
            if _subscription_fields_for(obj) is not None:
                reported.add(obj)

    # Remove dynamically-discovered sensors no longer reported by Moonraker.
    # Static manifest entries (user-configured) are never removed.
    stale = [
        key
        for key in current_objects
        if is_discoverable_object(key)
        and key not in reported
        and key not in static_objects
    ]
    for key in stale:
        del current_objects[key]

    return added_objects


async def fetch_output_pin_pwm_config(
    source: TelemetrySource,
    pin_objects: list[str],
) -> dict[str, bool]:
    """Query Moonraker configfile to determine PWM capability of output pins.

    Klipper default for ``output_pin`` is ``pwm: false`` (binary).
    Returns a mapping of Moonraker object name → supports PWM.
    """
    if not pin_objects:
        return {}

    try:
        result = await source.fetch_printer_state(
            {"configfile": ["settings"]}, timeout=5.0
        )
    except (OSError, asyncio.TimeoutError) as exc:
        LOGGER.warning("Failed to fetch configfile for pin PWM config: %s", exc)
        return {}

    settings = (
        result.get("result", {})
        .get("status", {})
        .get("configfile", {})
        .get("settings", {})
    )
    config: dict[str, bool] = {}
    for obj_name in pin_objects:
        section = settings.get(obj_name, {})
        config[obj_name] = bool(section.get("pwm", False))
    return config
