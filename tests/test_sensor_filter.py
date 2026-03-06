"""Tests for SensorFilter policy.

Validates allowlist/denylist semantics, core-sensor guarantees, and edge cases.
"""

from __future__ import annotations

import pytest

from moonraker_owl.config import CORE_SENSORS
from moonraker_owl.telemetry.selectors import SensorFilter


class TestSensorFilterDefaults:
    """No allowlist, no denylist — everything passes."""

    def test_core_sensors_allowed(self):
        sf = SensorFilter()
        for name in CORE_SENSORS:
            assert sf.is_allowed(name), f"core sensor {name!r} should be allowed"

    def test_arbitrary_sensor_allowed(self):
        sf = SensorFilter()
        assert sf.is_allowed("temperature_sensor chamber")
        assert sf.is_allowed("temperature_fan exhaust")
        assert sf.is_allowed("heater_generic chamber_heater")


class TestSensorFilterAllowlistOnly:
    """Allowlist restricts to core + explicitly listed sensors."""

    def test_core_sensors_always_pass(self):
        sf = SensorFilter(allowlist=["temperature_sensor chamber"])
        for name in CORE_SENSORS:
            assert sf.is_allowed(name), f"core sensor {name!r} blocked by allowlist"

    def test_allowed_sensor_passes(self):
        sf = SensorFilter(allowlist=["temperature_sensor chamber"])
        assert sf.is_allowed("temperature_sensor chamber")

    def test_unlisted_sensor_blocked(self):
        sf = SensorFilter(allowlist=["temperature_sensor chamber"])
        assert not sf.is_allowed("temperature_sensor mcu_temp")
        assert not sf.is_allowed("temperature_fan exhaust")


class TestSensorFilterDenylistOnly:
    """Denylist excludes specific sensors."""

    def test_denied_sensor_blocked(self):
        sf = SensorFilter(denylist=["temperature_sensor mcu_temp"])
        assert not sf.is_allowed("temperature_sensor mcu_temp")

    def test_non_denied_sensor_passes(self):
        sf = SensorFilter(denylist=["temperature_sensor mcu_temp"])
        assert sf.is_allowed("temperature_sensor chamber")
        assert sf.is_allowed("extruder")

    def test_denylist_can_exclude_core_sensors(self):
        sf = SensorFilter(denylist=["fan"])
        assert not sf.is_allowed("fan")
        # Other core sensors still pass
        assert sf.is_allowed("extruder")
        assert sf.is_allowed("heater_bed")


class TestSensorFilterCombined:
    """Denylist wins over both core and allowlist."""

    def test_denylist_overrides_allowlist(self):
        sf = SensorFilter(
            allowlist=["temperature_sensor chamber", "temperature_sensor mcu_temp"],
            denylist=["temperature_sensor mcu_temp"],
        )
        assert sf.is_allowed("temperature_sensor chamber")
        assert not sf.is_allowed("temperature_sensor mcu_temp")

    def test_denylist_overrides_core(self):
        sf = SensorFilter(
            allowlist=["temperature_sensor chamber"],
            denylist=["extruder"],
        )
        assert not sf.is_allowed("extruder")
        assert sf.is_allowed("heater_bed")
        assert sf.is_allowed("temperature_sensor chamber")


class TestSensorFilterEdgeCases:
    def test_empty_lists_equivalent_to_default(self):
        sf = SensorFilter(allowlist=[], denylist=[])
        assert sf.is_allowed("extruder")
        assert sf.is_allowed("temperature_sensor random")

    def test_unknown_sensor_with_allowlist(self):
        sf = SensorFilter(allowlist=["temperature_sensor chamber"])
        assert not sf.is_allowed("totally_unknown_device")

    def test_unknown_sensor_without_allowlist(self):
        sf = SensorFilter()
        assert sf.is_allowed("totally_unknown_device")
