"""Telemetry publishing pipeline for the Owl contract.

This package re-exports all public symbols so that existing imports
(``from moonraker_owl.telemetry import TelemetryPublisher``) continue to
work unchanged.  The implementation lives in :pymod:`publisher`.
"""

from __future__ import annotations

# -- submodule re-exports (order matters: no cycles) -------------------------
from .cadence import (
    ChannelCadenceController,
    ChannelDecision,
    ChannelRuntimeState,
    ChannelSchedule,
    TelemetryCadenceError,
)
from .event_types import Event, EventName, EventPriority, EventSeverity
from .events import EventCollector, RateLimitConfig
from .orchestrator import ChannelPayload, TelemetryOrchestrator
from .polling import DEFAULT_POLL_SPECS, PollSpec
from .selectors import SensorFilter
from .state_store import MoonrakerStateStore
from .telemetry_state import TelemetryHasher

# -- bridge re-exports -------------------------------------------------------
from .moonraker_bridge import (
    TelemetrySource,
    discover_moonraker_sensors,
)

# -- publisher re-exports ----------------------------------------------------
from .publisher import (
    MQTTClientLike,
    TelemetryConfigurationError,
    TelemetryPublisher,
    build_subscription_manifest,
    is_heater_object,
    heater_has_target,
    _normalise_fields,
    _normalise_field,
)

__all__ = [
    # Publisher
    "MQTTClientLike",
    "TelemetryConfigurationError",
    "TelemetryPublisher",
    # Utilities
    "build_subscription_manifest",
    "is_heater_object",
    "heater_has_target",
    # Submodule re-exports
    "ChannelCadenceController",
    "ChannelDecision",
    "ChannelRuntimeState",
    "ChannelSchedule",
    "TelemetryCadenceError",
    "Event",
    "EventCollector",
    "EventName",
    "EventPriority",
    "EventSeverity",
    "PollSpec",
    "RateLimitConfig",
    "SensorFilter",
    "ChannelPayload",
    "TelemetryOrchestrator",
    "MoonrakerStateStore",
    "TelemetryHasher",
]
