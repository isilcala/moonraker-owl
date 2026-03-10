"""Shared test utilities for moonraker_owl tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from moonraker_owl.config import (
    CameraConfig,
    CloudConfig,
    CommandConfig,
    CompressionConfig,
    LoggingConfig,
    MetadataConfig,
    MoonrakerConfig,
    OwlConfig,
    ResilienceConfig,
    TelemetryConfig,
    TelemetryCadenceConfig,
    DEFAULT_TELEMETRY_FIELDS,
)


def build_config(
    *,
    device_id: str = "device-123",
    tenant_id: str = "tenant-99",
    printer_id: str = "printer-17",
    include_fields: Optional[list[str]] = None,
    sensors_interval_seconds: float = 0.2,
    include_raw_payload: bool = False,
    breaker_threshold: int = 2,
) -> OwlConfig:
    """Build a minimal OwlConfig for tests.

    Shared across test modules to avoid repeating the same boilerplate.
    """
    username = f"{tenant_id}:{device_id}"
    raw: Dict[str, Any] = {
        "cloud": {
            "device_id": device_id,
            "tenant_id": tenant_id,
            "printer_id": printer_id,
            "username": username,
        }
    }

    return OwlConfig(
        cloud=CloudConfig(
            base_url="https://api.owl.dev",
            broker_host="broker.owl.dev",
            broker_port=1883,
            device_id=device_id,
            tenant_id=tenant_id,
            printer_id=printer_id,
            username=username,
            password="token",
        ),
        moonraker=MoonrakerConfig(url="http://localhost:7125"),
        telemetry=TelemetryConfig(
            sensors_interval_seconds=sensors_interval_seconds,
            include_raw_payload=include_raw_payload,
            include_fields=include_fields or list(DEFAULT_TELEMETRY_FIELDS),
        ),
        telemetry_cadence=TelemetryCadenceConfig(),
        commands=CommandConfig(),
        logging=LoggingConfig(),
        resilience=ResilienceConfig(moonraker_breaker_threshold=breaker_threshold),
        compression=CompressionConfig(),
        camera=CameraConfig(),
        metadata=MetadataConfig(),
        raw=raw,
        path=Path("moonraker-owl.toml"),
    )
