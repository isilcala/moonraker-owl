"""Tests for CloudConfigManager."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, patch

import pytest

from moonraker_owl.cloud_config import (
    CloudConfigManager,
    CloudConfigState,
    _snake_dict,
    apply_cloud_config,
)
from moonraker_owl.config import OwlConfig, load_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_CLOUD_RESPONSE: Dict[str, Any] = {
    "telemetry": {
        "sensorsIntervalSeconds": 5,
        "includeFields": ["print_stats", "extruder"],
        "excludeFields": ["fan"],
        "includeRawPayload": True,
        "sensorAllowlist": ["temperature_sensor chamber"],
        "sensorDenylist": ["temperature_sensor mcu_temp"],
        "maxCustomSensors": 3,
        "maxSensorCount": 9,
    },
    "cadence": {
        "statusHeartbeatSeconds": 30,
        "statusIdleIntervalSeconds": 120,
        "statusActiveIntervalSeconds": 5,
        "statusMinIntervalSeconds": 3,
        "sensorsForcePublishSeconds": 600,
        "eventsMaxPerSecond": 2,
        "eventsMaxPerMinute": 40,
        "thumbnailFetchTimeoutMs": 10000,
        "timelapsePollIntervalSeconds": 10,
    },
    "camera": {
        "snapshotUrl": "http://cam:8080/snap",
        "cameraName": "nozzle_cam",
        "captureTimeoutSeconds": 15,
        "maxRetries": 3,
        "preprocessEnabled": False,
        "preprocessTargetWidth": 640,
        "preprocessJpegQuality": 70,
    },
    "compression": {
        "enabled": False,
        "channels": ["sensors"],
        "minSizeBytes": 2048,
    },
    "commands": {
        "ackTimeoutSeconds": 60,
    },
}


def _make_config(tmp_path: Path) -> OwlConfig:
    return load_config(tmp_path / "moonraker-owl.toml")


def _make_manager(
    config: OwlConfig,
    tmp_path: Path,
    token: str = "test-jwt",
) -> CloudConfigManager:
    return CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: token,
        lkg_path=tmp_path / "cloud-config.json",
    )


# ---------------------------------------------------------------------------
# _snake_dict
# ---------------------------------------------------------------------------


def test_snake_dict_converts_keys():
    assert _snake_dict({"fooBar": 1, "bazQux": {"nestedKey": 2}}) == {
        "foo_bar": 1,
        "baz_qux": {"nested_key": 2},
    }


# ---------------------------------------------------------------------------
# apply_cloud_config
# ---------------------------------------------------------------------------


def test_apply_cloud_config_updates_all_sections(tmp_path: Path):
    config = _make_config(tmp_path)
    snake = _snake_dict(SAMPLE_CLOUD_RESPONSE)
    apply_cloud_config(config, snake)

    # Telemetry: sensorsIntervalSeconds=5 → sensors_interval_seconds=5.0
    assert config.telemetry.sensors_interval_seconds == 5.0
    assert config.telemetry.include_fields == ["print_stats", "extruder"]
    assert config.telemetry.exclude_fields == ["fan"]
    assert config.telemetry.include_raw_payload is True
    assert config.telemetry.sensor_allowlist == ["temperature_sensor chamber"]
    assert config.telemetry.sensor_denylist == ["temperature_sensor mcu_temp"]
    assert config.telemetry.max_custom_sensors == 3
    assert config.telemetry.max_sensor_count == 9

    # Cadence
    assert config.telemetry_cadence.status_heartbeat_seconds == 30
    assert config.telemetry_cadence.status_idle_interval_seconds == 120
    assert config.telemetry_cadence.status_active_interval_seconds == 5
    assert config.telemetry_cadence.status_min_interval_seconds == 3
    assert config.telemetry_cadence.sensors_force_publish_seconds == 600
    assert config.telemetry_cadence.events_max_per_second == 2
    assert config.telemetry_cadence.events_max_per_minute == 40
    assert config.telemetry_cadence.thumbnail_fetch_timeout_ms == 10000
    assert config.telemetry_cadence.timelapse_poll_interval_seconds == 10

    # Camera
    assert config.camera.snapshot_url == "http://cam:8080/snap"
    assert config.camera.camera_name == "nozzle_cam"
    assert config.camera.capture_timeout_seconds == 15
    assert config.camera.max_retries == 3
    assert config.camera.preprocess_enabled is False
    assert config.camera.preprocess_target_width == 640
    assert config.camera.preprocess_jpeg_quality == 70

    # Compression
    assert config.compression.enabled is False
    assert config.compression.channels == ["sensors"]
    assert config.compression.min_size_bytes == 2048

    # Commands
    assert config.commands.ack_timeout_seconds == 60


def test_apply_cloud_config_partial_update(tmp_path: Path):
    config = _make_config(tmp_path)
    original_interval = config.telemetry.sensors_interval_seconds

    apply_cloud_config(config, {"camera": {"snapshot_url": "http://new:8080/snap"}})

    # Only camera should change
    assert config.camera.snapshot_url == "http://new:8080/snap"
    assert config.telemetry.sensors_interval_seconds == original_interval


def test_apply_cloud_config_sensors_interval_drives_idle_rate(tmp_path: Path):
    """sensorsIntervalSeconds is the primary source for sensors_interval_seconds."""
    config = _make_config(tmp_path)
    apply_cloud_config(config, {
        "telemetry": {
            "sensors_interval_seconds": 5,
        }
    })
    assert config.telemetry.sensors_interval_seconds == 5.0


def test_apply_cloud_config_cadence_status_idle_takes_effect(tmp_path: Path):
    """statusIdleIntervalSeconds in cadence section is the real control."""
    config = _make_config(tmp_path)
    apply_cloud_config(config, {
        "cadence": {
            "status_idle_interval_seconds": 30,
        }
    })
    assert config.telemetry_cadence.status_idle_interval_seconds == pytest.approx(30.0)



# ---------------------------------------------------------------------------
# LKG load / write
# ---------------------------------------------------------------------------


def test_load_lkg_populates_config(tmp_path: Path):
    config = _make_config(tmp_path)
    mgr = _make_manager(config, tmp_path)

    lkg_data = dict(SAMPLE_CLOUD_RESPONSE)
    lkg_data["_meta"] = {
        "etag": "sha256-abc",
        "fetchedAt": "2026-01-01T00:00:00+00:00",
        "ttlHours": 72,
        "agentVersion": "0.9.0",
    }
    (tmp_path / "cloud-config.json").write_text(
        json.dumps(lkg_data), encoding="utf-8"
    )

    assert mgr.load_lkg() is True
    assert mgr._state.etag == "sha256-abc"
    assert config.camera.snapshot_url == "http://cam:8080/snap"
    assert config.commands.ack_timeout_seconds == 60


def test_load_lkg_returns_false_when_missing(tmp_path: Path):
    config = _make_config(tmp_path)
    mgr = _make_manager(config, tmp_path)
    assert mgr.load_lkg() is False


def test_load_lkg_handles_corrupt_file(tmp_path: Path):
    config = _make_config(tmp_path)
    mgr = _make_manager(config, tmp_path)
    (tmp_path / "cloud-config.json").write_text("not json!", encoding="utf-8")
    assert mgr.load_lkg() is False


# ---------------------------------------------------------------------------
# fetch() — using aiohttp mock
# ---------------------------------------------------------------------------


class FakeResponse:
    """Minimal aiohttp response stand-in."""

    def __init__(
        self, status: int, data: Any = None, headers: Optional[Dict[str, str]] = None
    ):
        self.status = status
        self._data = data
        self.headers = headers or {}

    async def json(self) -> Any:
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class FakeSession:
    """Minimal aiohttp.ClientSession replacement for tests."""

    def __init__(self):
        self.responses: list[FakeResponse] = []
        self.requests: list[tuple[str, Dict[str, Any]]] = []
        self._closed = False

    def add_response(self, resp: FakeResponse) -> None:
        self.responses.append(resp)

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.requests.append((url, kwargs))
        return self.responses.pop(0)

    async def close(self) -> None:
        self._closed = True


@pytest.mark.asyncio
async def test_fetch_200_updates_config_and_writes_lkg(tmp_path: Path):
    config = _make_config(tmp_path)
    session = FakeSession()
    session.add_response(
        FakeResponse(200, SAMPLE_CLOUD_RESPONSE, {"ETag": "sha256-new"})
    )

    mgr = CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: "jwt-tok",
        lkg_path=tmp_path / "cloud-config.json",
        http_session=session,
    )

    result = await mgr.fetch(force=True)
    assert result is True
    assert config.camera.snapshot_url == "http://cam:8080/snap"
    assert mgr._state.etag == "sha256-new"

    # LKG should be written
    lkg = json.loads((tmp_path / "cloud-config.json").read_text(encoding="utf-8"))
    assert "_meta" in lkg
    assert lkg["_meta"]["etag"] == "sha256-new"


@pytest.mark.asyncio
async def test_fetch_304_no_update(tmp_path: Path):
    config = _make_config(tmp_path)
    session = FakeSession()
    session.add_response(FakeResponse(304))

    mgr = CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: "jwt-tok",
        lkg_path=tmp_path / "cloud-config.json",
        http_session=session,
    )
    mgr._state.etag = "sha256-old"

    result = await mgr.fetch(force=True)
    assert result is False
    assert not (tmp_path / "cloud-config.json").exists()


@pytest.mark.asyncio
async def test_fetch_error_returns_false(tmp_path: Path):
    config = _make_config(tmp_path)
    session = FakeSession()
    session.add_response(FakeResponse(500))

    mgr = CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: "jwt-tok",
        lkg_path=tmp_path / "cloud-config.json",
        http_session=session,
    )

    result = await mgr.fetch(force=True)
    assert result is False


@pytest.mark.asyncio
async def test_fetch_skips_when_recent(tmp_path: Path):
    config = _make_config(tmp_path)
    session = FakeSession()

    mgr = CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: "jwt-tok",
        lkg_path=tmp_path / "cloud-config.json",
        http_session=session,
    )
    # Pretend we fetched just now
    mgr._state.last_fetched_at = time.monotonic()

    result = await mgr.fetch()  # force=False (default)
    assert result is False
    # No HTTP request was made
    assert len(session.requests) == 0


@pytest.mark.asyncio
async def test_fetch_sends_if_none_match_header(tmp_path: Path):
    config = _make_config(tmp_path)
    session = FakeSession()
    session.add_response(FakeResponse(304))

    mgr = CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: "jwt-tok",
        lkg_path=tmp_path / "cloud-config.json",
        http_session=session,
    )
    mgr._state.etag = "sha256-existing"

    await mgr.fetch(force=True)

    assert len(session.requests) == 1
    _, kwargs = session.requests[0]
    headers = kwargs.get("headers", {})
    assert headers.get("If-None-Match") == "sha256-existing"


@pytest.mark.asyncio
async def test_fetch_no_token_skips(tmp_path: Path):
    config = _make_config(tmp_path)

    mgr = CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: None,
        lkg_path=tmp_path / "cloud-config.json",
    )

    result = await mgr.fetch(force=True)
    assert result is False


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_200_fires_callbacks(tmp_path: Path):
    config = _make_config(tmp_path)
    session = FakeSession()
    session.add_response(
        FakeResponse(200, SAMPLE_CLOUD_RESPONSE, {"ETag": "sha256-v2"})
    )

    mgr = CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: "jwt-tok",
        lkg_path=tmp_path / "cloud-config.json",
        http_session=session,
    )

    received: list[OwlConfig] = []

    async def on_change(cfg: OwlConfig) -> None:
        received.append(cfg)

    mgr.register_callback(on_change)
    await mgr.fetch(force=True)

    assert len(received) == 1
    assert received[0] is config


@pytest.mark.asyncio
async def test_fetch_304_does_not_fire_callbacks(tmp_path: Path):
    config = _make_config(tmp_path)
    session = FakeSession()
    session.add_response(FakeResponse(304))

    mgr = CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: "jwt-tok",
        lkg_path=tmp_path / "cloud-config.json",
        http_session=session,
    )
    mgr._state.etag = "sha256-old"

    called = False

    async def on_change(cfg: OwlConfig) -> None:
        nonlocal called
        called = True

    mgr.register_callback(on_change)
    await mgr.fetch(force=True)
    assert called is False


# ---------------------------------------------------------------------------
# schedule_fetch + coalesce
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schedule_fetch_coalesces(tmp_path: Path):
    """Multiple rapid schedule_fetch calls should result in a single fetch."""
    config = _make_config(tmp_path)
    session = FakeSession()
    # Only one response needed — only one fetch should happen
    session.add_response(
        FakeResponse(200, SAMPLE_CLOUD_RESPONSE, {"ETag": "sha256-coalesced"})
    )

    mgr = CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: "jwt-tok",
        lkg_path=tmp_path / "cloud-config.json",
        http_session=session,
    )

    # Fire 3 rapid notifications — should coalesce to 1 fetch
    mgr.schedule_fetch(jitter_seconds=0.05)
    mgr.schedule_fetch(jitter_seconds=0.05)
    mgr.schedule_fetch(jitter_seconds=0.05)

    # Wait for the final fetch to complete
    await asyncio.sleep(0.2)

    assert len(session.requests) == 1


# ---------------------------------------------------------------------------
# stop()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_cancels_pending(tmp_path: Path):
    config = _make_config(tmp_path)

    mgr = CloudConfigManager(
        config=config,
        device_id="dev-1",
        base_url="https://owl.example.com",
        token_provider=lambda: "jwt-tok",
        lkg_path=tmp_path / "cloud-config.json",
    )

    mgr.schedule_fetch(jitter_seconds=10)
    assert mgr._pending_fetch_task is not None

    await mgr.stop()
    assert mgr._stopped is True


# ---------------------------------------------------------------------------
# TelemetryPublisher.refresh_base_config
# ---------------------------------------------------------------------------


def test_refresh_base_config_updates_idle_rate():
    """refresh_base_config propagates new cloud config values to publisher."""
    from helpers import build_config
    from test_telemetry import FakeMoonrakerClient, FakeMQTTClient
    from moonraker_owl.telemetry import TelemetryPublisher

    config = build_config(sensors_interval_seconds=30.0)
    moonraker = FakeMoonrakerClient({"result": {"status": {}}})
    mqtt = FakeMQTTClient()
    publisher = TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    assert publisher._idle_interval == pytest.approx(30.0)

    # Simulate cloud config changing the sensors interval
    config.telemetry.sensors_interval_seconds = 2.0
    config.telemetry_cadence.status_idle_interval_seconds = 120
    config.telemetry_cadence.sensors_force_publish_seconds = 600

    publisher.refresh_base_config()

    assert publisher._idle_interval == pytest.approx(2.0)
    assert publisher._idle_interval == pytest.approx(2.0)
    assert publisher._status_idle_interval == 120
    assert publisher._sensors_force_publish_seconds == 600
    # In idle mode, sensors_interval should track the new idle interval
    assert publisher._sensors_interval == pytest.approx(2.0)
