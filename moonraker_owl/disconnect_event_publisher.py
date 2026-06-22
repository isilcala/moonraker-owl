"""Publishes precise per-disconnect events to ``owl/printers/{deviceId}/events/disconnect``.

Companion to the periodic :class:`~moonraker_owl.health_publisher.HealthPublisher`
(ADR-0045). Where the health snapshot carries aggregate ``counters.disconnects.byCause``
and a rolling last-60s fallback list, this publisher emits one event per *unexpected*
session drop the moment the link recovers, so operators see precise timelines
("23:47:12 keepalive_timeout"). Planned token-refresh reconnections are not churn and
are never recorded by the coordinator, so they never reach this publisher.

The coordinator invokes :meth:`publish` from its reconnected path (link already back
up), so a direct QoS-1 publish suffices; the health channel's fallback list covers the
rare case where this single event is lost.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from . import constants
from .connection import DisconnectRecord
from .identifiers import uuid7
from .version import __version__

LOGGER = logging.getLogger(__name__)

DISCONNECT_EVENT_TYPE = "telemetry.event.disconnect"
DISCONNECT_EVENT_SCHEMA_VERSION = 2


class DisconnectEventPublisher:
    """Emits one ``events/disconnect`` message per unexpected session drop."""

    def __init__(self, *, mqtt_client: Any, device_id: str) -> None:
        self._mqtt_client = mqtt_client
        self._device_id = str(device_id)
        self._topic = constants.MQTTTopics.for_device(self._device_id).events_disconnect

    def publish(self, record: DisconnectRecord) -> None:
        """Build and publish a disconnect event (best-effort; never raises)."""
        try:
            payload = self.build_envelope(record)
            self._mqtt_client.publish(
                self._topic,
                json.dumps(payload).encode("utf-8"),
                qos=1,
                retain=False,
                buffer_on_failure=False,
            )
        except Exception as exc:  # pragma: no cover - exercised via tests
            LOGGER.warning("Disconnect event publish failed: %s", exc)

    def build_envelope(self, record: DisconnectRecord) -> dict:
        """Assemble the telemetry.event.disconnect envelope for *record*."""
        return {
            "$v": DISCONNECT_EVENT_SCHEMA_VERSION,
            "$type": DISCONNECT_EVENT_TYPE,
            "$id": str(uuid7()),
            "$ts": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "$origin": f"moonraker-owl@{__version__}",
            "deviceId": self._device_id,
            "payload": record.as_event_dict(),
        }
