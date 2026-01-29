"""Device metadata reporting for Owl Cloud.

This module implements ADR-0032: Device Capabilities and Metadata Infrastructure.
It collects device capabilities from various sources and reports them to the cloud
via HTTP PUT, enabling dynamic feature detection and UI adaptation.

Key components:
- MetadataReporter: Orchestrates collection and HTTP upload
- Provider classes: Collect capabilities from different sources
  - SystemInfoProvider: Agent version, OS info
  - MoonrakerProvider: Moonraker version, installed plugins
  - KlipperProvider: Klipper version, features
  - CameraProvider: Camera availability and configuration

Usage:
    reporter = MetadataReporter(config, token_provider)
    await reporter.start()  # Non-blocking
    ...
    await reporter.stop()
"""

from .providers import (
    BaseProvider,
    CameraProvider,
    CapabilityProvider,
    KlipperProvider,
    MoonrakerProvider,
    SystemInfoProvider,
)
from .reporter import MetadataReporter, MetadataReporterConfig

__all__ = [
    "MetadataReporter",
    "MetadataReporterConfig",
    "CapabilityProvider",
    "BaseProvider",
    "SystemInfoProvider",
    "MoonrakerProvider",
    "KlipperProvider",
    "CameraProvider",
]
