"""Tests for SensorFilter policy.

Validates allowlist/denylist semantics, core-sensor guarantees, and edge cases.
"""

from __future__ import annotations

import pytest

from moonraker_owl.config import CORE_SENSORS
from moonraker_owl.telemetry.selectors import SensorFilter


class TestSensorFilterDefaults:
    """No allowlist, no denylist — core sensors only."""

    def test_core_sensors_allowed(self):
        sf = SensorFilter()
        for name in CORE_SENSORS:
            assert sf.is_allowed(name), f"core sensor {name!r} should be allowed"

    def test_non_core_sensors_blocked_by_default(self):
        """Empty allowlist = core only; custom sensors require explicit opt-in."""
        sf = SensorFilter()
        assert not sf.is_allowed("temperature_sensor chamber")
        assert not sf.is_allowed("temperature_fan exhaust")
        assert not sf.is_allowed("heater_generic chamber_heater")


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


class TestSensorFilterAllowlistEnablesCustom:
    """Allowlist opt-in makes non-core sensors pass."""

    def test_custom_sensors_pass_when_allowlisted(self):
        sf = SensorFilter(allowlist=["temperature_sensor chamber", "temperature_fan exhaust"])
        assert sf.is_allowed("temperature_sensor chamber")
        assert sf.is_allowed("temperature_fan exhaust")

    def test_unlisted_custom_sensor_still_blocked(self):
        sf = SensorFilter(allowlist=["temperature_sensor chamber"])
        assert not sf.is_allowed("temperature_sensor mcu_temp")

    def test_core_always_passes_with_allowlist(self):
        sf = SensorFilter(allowlist=["temperature_sensor chamber"])
        for name in CORE_SENSORS:
            assert sf.is_allowed(name)


class TestSensorFilterDenylistOnly:
    """Denylist excludes specific sensors."""

    def test_denied_sensor_blocked(self):
        sf = SensorFilter(denylist=["temperature_sensor mcu_temp"])
        assert not sf.is_allowed("temperature_sensor mcu_temp")

    def test_non_denied_core_sensor_passes(self):
        sf = SensorFilter(denylist=["temperature_sensor mcu_temp"])
        assert sf.is_allowed("extruder")

    def test_non_denied_custom_still_blocked_without_allowlist(self):
        """Denylist alone doesn't enable non-core sensors."""
        sf = SensorFilter(denylist=["temperature_sensor mcu_temp"])
        assert not sf.is_allowed("temperature_sensor chamber")

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
        assert not sf.is_allowed("temperature_sensor random")

    def test_unknown_sensor_with_allowlist(self):
        sf = SensorFilter(allowlist=["temperature_sensor chamber"])
        assert not sf.is_allowed("totally_unknown_device")

    def test_unknown_sensor_without_allowlist(self):
        sf = SensorFilter()
        assert not sf.is_allowed("totally_unknown_device")


def _make_sensor(channel: str, source_object: str | None = None) -> dict:
    """Helper to build a minimal sensor dict for hard cap tests."""
    return {
        "channel": channel,
        "type": "sensor",
        "unit": "celsius",
        "value": 25.0,
        "sourceObject": source_object or channel,
        "status": "ok",
        "lastUpdated": "2025-01-01T00:00:00",
    }


class TestSensorFilterHardCapCustomLimit:
    """max_custom_sensors limits non-core sensors."""

    def test_free_tier_zero_custom_sensors(self):
        sf = SensorFilter(max_custom_sensors=0, max_sensor_count=6)
        sensors = [
            _make_sensor("extruder", "extruder"),
            _make_sensor("heater_bed", "heater_bed"),
            _make_sensor("fan", "fan"),
            _make_sensor("chamber", "temperature_sensor chamber"),
            _make_sensor("mcu_temp", "temperature_sensor mcu_temp"),
        ]
        result = sf.apply_hard_cap(sensors)
        channels = [s["channel"] for s in result]
        assert channels == ["extruder", "heater_bed", "fan"]

    def test_plus_tier_three_custom_sensors(self):
        sf = SensorFilter(max_custom_sensors=3, max_sensor_count=9)
        sensors = [
            _make_sensor("extruder", "extruder"),
            _make_sensor("heater_bed", "heater_bed"),
            _make_sensor("fan", "fan"),
            _make_sensor("chamber", "temperature_sensor chamber"),
            _make_sensor("exhaust", "temperature_fan exhaust"),
            _make_sensor("hotend_fan", "heater_fan hotend_fan"),
            _make_sensor("mcu_temp", "temperature_sensor mcu_temp"),
            _make_sensor("psu_temp", "temperature_sensor psu_temp"),
        ]
        result = sf.apply_hard_cap(sensors)
        channels = [s["channel"] for s in result]
        # 3 core kept + 3 custom kept (alphabetical: chamber, exhaust, hotend_fan)
        assert channels == [
            "extruder", "heater_bed", "fan",
            "chamber", "exhaust", "hotend_fan",
        ]

    def test_pro_tier_eight_custom_sensors(self):
        sf = SensorFilter(max_custom_sensors=8, max_sensor_count=14)
        customs = [_make_sensor(f"custom_{i}", f"temperature_sensor custom_{i}") for i in range(10)]
        sensors = [
            _make_sensor("extruder", "extruder"),
            _make_sensor("heater_bed", "heater_bed"),
            _make_sensor("fan", "fan"),
        ] + customs
        result = sf.apply_hard_cap(sensors)
        # 3 core + 8 custom = 11, well under max_sensor_count=14
        assert len(result) == 11
        custom_channels = [s["channel"] for s in result if s["sourceObject"] not in CORE_SENSORS]
        assert len(custom_channels) == 8


class TestSensorFilterHardCapTotalLimit:
    """max_sensor_count caps overall count."""

    def test_total_limit_further_trims_custom(self):
        # max_custom_sensors=5 but max_sensor_count=6 with 3 core → only 3 custom fit
        sf = SensorFilter(max_custom_sensors=5, max_sensor_count=6)
        sensors = [
            _make_sensor("extruder", "extruder"),
            _make_sensor("heater_bed", "heater_bed"),
            _make_sensor("fan", "fan"),
            _make_sensor("a_sensor", "temperature_sensor a_sensor"),
            _make_sensor("b_sensor", "temperature_sensor b_sensor"),
            _make_sensor("c_sensor", "temperature_sensor c_sensor"),
            _make_sensor("d_sensor", "temperature_sensor d_sensor"),
            _make_sensor("e_sensor", "temperature_sensor e_sensor"),
        ]
        result = sf.apply_hard_cap(sensors)
        assert len(result) == 6
        custom_channels = [s["channel"] for s in result if s["sourceObject"] not in CORE_SENSORS]
        assert len(custom_channels) == 3

    def test_core_sensors_never_dropped(self):
        # Even with max_sensor_count < number of core sensors, core sensors are kept
        sf = SensorFilter(max_custom_sensors=0, max_sensor_count=2)
        sensors = [
            _make_sensor("extruder", "extruder"),
            _make_sensor("heater_bed", "heater_bed"),
            _make_sensor("fan", "fan"),
        ]
        result = sf.apply_hard_cap(sensors)
        # All 3 core sensors kept — hard cap never drops core
        assert len(result) == 3


class TestSensorFilterHardCapOrderPreservation:
    """Hard cap preserves alphabetical order of custom sensors."""

    def test_alphabetical_order_preserved(self):
        sf = SensorFilter(max_custom_sensors=2, max_sensor_count=10)
        sensors = [
            _make_sensor("extruder", "extruder"),
            _make_sensor("alpha", "temperature_sensor alpha"),
            _make_sensor("bravo", "temperature_sensor bravo"),
            _make_sensor("charlie", "temperature_sensor charlie"),
        ]
        result = sf.apply_hard_cap(sensors)
        custom_channels = [s["channel"] for s in result if s["sourceObject"] not in CORE_SENSORS]
        # First two alphabetically: alpha, bravo
        assert custom_channels == ["alpha", "bravo"]


class TestSensorFilterHardCapNoop:
    """When sensors are within limits, no truncation occurs."""

    def test_under_limits_no_change(self):
        sf = SensorFilter(max_custom_sensors=3, max_sensor_count=9)
        sensors = [
            _make_sensor("extruder", "extruder"),
            _make_sensor("heater_bed", "heater_bed"),
            _make_sensor("chamber", "temperature_sensor chamber"),
        ]
        result = sf.apply_hard_cap(sensors)
        assert len(result) == 3

    def test_empty_list(self):
        sf = SensorFilter(max_custom_sensors=0, max_sensor_count=6)
        result = sf.apply_hard_cap([])
        assert result == []
