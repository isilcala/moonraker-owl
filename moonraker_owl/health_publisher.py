"""Periodic edge-agent self-health publisher (MQTT health channel).

The :class:`HealthPublisher` gathers already-tracked health counters and gauges
from the running agent components and publishes a structured snapshot to the
``owl/printers/{deviceId}/health`` MQTT channel on a fixed cadence.

Design notes (see docs/proposals/edge-agent-health-metrics-over-mqtt.md):

* QoS 1, retain=True so a freshly-(re)connected cloud/UI sees the last value
  immediately.
* ``buffer_on_failure=False``: stale health has no value; the next tick is
  authoritative, so health never competes for offline-buffer budget.
* ``kind`` is always ``"full"`` — every tick is a complete snapshot.
* Counters are cumulative since process start. A consumer detects a restart via
  ``uptimeSeconds`` dropping (counters reset to 0).
* Every accessor is wrapped defensively: one missing/raising source must never
  kill the publish loop, and a publish failure is swallowed (rate-limited log).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

from . import constants
from .identifiers import uuid7
from .version import __version__

LOGGER = logging.getLogger(__name__)

# Suppress repeated publish-failure logs to one line per this many seconds.
_PUBLISH_WARN_INTERVAL_SECONDS = 60.0


class HealthPublisher:
    """Publishes periodic agent health snapshots to the MQTT health channel.

    The publisher is intentionally decoupled from the concrete component types:
    it only reads a small set of public accessors, each guarded so a partially
    initialised agent still produces a best-effort snapshot.
    """

    def __init__(
        self,
        *,
        mqtt_client: Any,
        device_id: str,
        moonraker: Any = None,
        publisher: Any = None,
        token_manager: Any = None,
        coordinator: Any = None,
        interval_seconds: float = 60.0,
        clock: Any = time.monotonic,
    ) -> None:
        self._mqtt_client = mqtt_client
        self._device_id = device_id
        self._moonraker = moonraker
        self._publisher = publisher
        self._token_manager = token_manager
        self._coordinator = coordinator
        self._interval_seconds = max(5.0, float(interval_seconds))
        self._clock = clock

        self._topic = constants.MQTTTopics.for_device(str(device_id)).health
        self._started_at = self._clock()
        self._seq = 0

        self._task: Optional["asyncio.Task[None]"] = None
        self._stop_event = asyncio.Event()
        self._last_publish_warn_at: Optional[float] = None

    async def start(self) -> None:
        """Start the background publish loop (idempotent)."""
        if self._task is not None and not self._task.done():
            LOGGER.warning("HealthPublisher already started")
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        LOGGER.info(
            "HealthPublisher started (interval=%.0fs, topic=%s)",
            self._interval_seconds,
            self._topic,
        )

    async def stop(self) -> None:
        """Stop the background publish loop and wait for it to finish."""
        self._stop_event.set()
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:  # pragma: no cover - defensive
            LOGGER.exception("HealthPublisher stop raised")

    async def _run_loop(self) -> None:
        # Publish an initial snapshot immediately on startup so the retained
        # health message is available to consumers right away, instead of only
        # after the first interval elapses (avoids an empty UI for ~1 minute).
        self.publish_once()
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._interval_seconds
                )
                # Stop event was set during the wait.
                break
            except asyncio.TimeoutError:
                pass
            self.publish_once()

    def publish_once(self) -> None:
        """Build and publish a single health snapshot (best-effort)."""
        try:
            payload = self.build_envelope()
            self._mqtt_client.publish(
                self._topic,
                json.dumps(payload).encode("utf-8"),
                qos=1,
                retain=True,
                buffer_on_failure=False,
            )
        except Exception as exc:  # pragma: no cover - exercised via tests
            self._warn_publish_failure(exc)

    def build_envelope(self) -> dict:
        """Assemble the full telemetry.health envelope from current sources."""
        envelope = {
            "$v": 1,
            "$type": "telemetry.health",
            "$id": str(uuid7()),
            "$ts": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "$origin": f"moonraker-owl@{__version__}",
            "$seq": self._seq,
            "kind": "full",
            "sessionId": None,
            "deviceId": str(self._device_id),
            "payload": self._build_payload(),
        }
        self._seq += 1
        return envelope

    def _build_payload(self) -> dict:
        return {
            "uptimeSeconds": max(0.0, self._clock() - self._started_at),
            "agentVersion": __version__,
            "mqtt": self._mqtt_metrics(),
            "publishQueue": self._publish_queue_metrics(),
            "moonraker": self._moonraker_metrics(),
            "token": self._token_metrics(),
        }

    def _mqtt_metrics(self) -> dict:
        buffer = _safe_invoke(self._mqtt_client, "offline_buffer_stats") or {}
        return {
            "connected": bool(_safe_invoke(self._mqtt_client, "is_connected")),
            "lastConnectRc": _safe_get(self._mqtt_client, "last_connect_rc"),
            "reconnectsTotal": int(
                _safe_get(self._coordinator, "reconnects_total", 0) or 0
            ),
            "resubscribedTotal": int(
                (_safe_invoke(self._mqtt_client, "subscription_stats") or {}).get(
                    "resubscribed_total", 0
                )
            ),
            "offlineBuffer": {
                "pendingCoalesced": int(buffer.get("pending_coalesced", 0)),
                "pendingEvents": int(buffer.get("pending_events", 0)),
                "bufferedTotal": int(buffer.get("buffered_total", 0)),
                "droppedTotal": int(buffer.get("dropped_total", 0)),
                "replayedTotal": int(buffer.get("replayed_total", 0)),
            },
        }

    def _publish_queue_metrics(self) -> dict:
        stats = _safe_invoke(self._publisher, "queue_stats") or {}
        return {
            "depth": int(stats.get("depth", 0)),
            "maxsize": int(stats.get("maxsize", 0)),
            "droppedTotal": int(stats.get("dropped_total", 0)),
        }

    def _moonraker_metrics(self) -> dict:
        return {
            "connected": bool(_safe_get(self._moonraker, "is_connected", False)),
            "consecutiveFailures": int(
                _safe_get(self._moonraker, "consecutive_failures", 0) or 0
            ),
            "outageSeconds": float(
                _safe_get(self._moonraker, "outage_seconds", 0.0) or 0.0
            ),
        }

    def _token_metrics(self) -> dict:
        seconds_until_expiry = _safe_invoke(
            self._token_manager, "seconds_until_expiry"
        )
        return {
            "renewalFailures": int(
                _safe_get(self._token_manager, "renewal_failures", 0) or 0
            ),
            "secondsUntilExpiry": (
                float(seconds_until_expiry)
                if seconds_until_expiry is not None
                else None
            ),
        }

    def _warn_publish_failure(self, exc: Exception) -> None:
        now = self._clock()
        if (
            self._last_publish_warn_at is not None
            and now - self._last_publish_warn_at < _PUBLISH_WARN_INTERVAL_SECONDS
        ):
            return
        self._last_publish_warn_at = now
        LOGGER.warning("Health snapshot publish failed: %s", exc)


def _safe_get(source: Any, name: str, default: Any = None) -> Any:
    """Read attribute *name* from *source*, swallowing any error."""
    if source is None:
        return default
    try:
        return getattr(source, name, default)
    except Exception:  # pragma: no cover - defensive
        return default


def _safe_invoke(source: Any, name: str) -> Any:
    """Resolve attribute *name* on *source* and call it when callable.

    Robust to the attribute being either a zero-arg method or a property: a
    callable is invoked, a non-callable value (e.g. a property's result) is
    returned as-is. All errors are swallowed, returning None.
    """
    if source is None:
        return None
    try:
        attr = getattr(source, name, None)
    except Exception:  # pragma: no cover - defensive
        return None
    if attr is None:
        return None
    if not callable(attr):
        return attr
    try:
        return attr()
    except Exception:  # pragma: no cover - defensive
        return None
