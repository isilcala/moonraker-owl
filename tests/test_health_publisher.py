"""Tests for the periodic MQTT health-snapshot publisher (audit Q6)."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest

from moonraker_owl import constants
from moonraker_owl.connection import DisconnectRecord, ReconnectReason
from moonraker_owl.health_publisher import HealthPublisher, _MIN_INTERVAL_SECONDS
from moonraker_owl.host_metrics import HostSample, NetworkSample

DEVICE_ID = "550e8400-e29b-41d4-a716-446655440000"


class FakeMQTT:
    def __init__(self, *, connected: bool = True, last_rc: int = 0) -> None:
        self._connected = connected
        self.last_connect_rc = last_rc
        self.published: list[dict] = []
        self.raise_on_publish = False

    def is_connected(self) -> bool:
        return self._connected

    def offline_buffer_stats(self) -> dict:
        return {
            "pending": 1,
            "pending_coalesced": 1,
            "pending_events": 0,
            "buffered_total": 220,
            "dropped_total": 3,
            "replayed_total": 217,
        }

    def subscription_stats(self) -> dict:
        return {"tracked": 4, "resubscribed_total": 34}

    def publish(self, topic, payload, qos=1, retain=False, *, buffer_on_failure=False):
        if self.raise_on_publish:
            raise RuntimeError("MQTT client not connected")
        self.published.append(
            {
                "topic": topic,
                "payload": payload,
                "qos": qos,
                "retain": retain,
                "buffer_on_failure": buffer_on_failure,
            }
        )


class FakePublisher:
    def queue_stats(self) -> dict:
        return {"depth": 2, "maxsize": 100, "dropped_total": 5}


class FakeMoonraker:
    is_connected = True
    consecutive_failures = 0
    poll_failures_total = 4
    outage_seconds = 0.0


class FakeTokenManager:
    renewal_failures = 0

    def seconds_until_expiry(self):
        return 3120.0


class FakeCoordinator:
    reconnects_total = 12

    @property
    def reconnects_by_cause(self) -> dict:
        return {
            "token_renewed": 9,
            "connection_lost": 3,
            "auth_failure": 0,
            "broker_disconnect": 0,
        }

    @property
    def disconnects_by_cause(self) -> dict:
        return {"connection_lost": 3, "auth_failure": 0, "broker_disconnect": 0}

    @property
    def churn_disconnects_total(self) -> int:
        return 3

    def recent_disconnects_snapshot(self, **_kwargs) -> list:
        return [
            DisconnectRecord(
                at=datetime(2026, 6, 21, 12, 0, 0, tzinfo=timezone.utc),
                cause=ReconnectReason.CONNECTION_LOST.value,
                reason_code=7,
                will_reconnect=True,
            )
        ]


def _fake_network() -> NetworkSample:
    return NetworkSample(wifi_rssi_dbm=-58.0, interface="wlan0", link_type="wifi")


def _fake_host() -> HostSample:
    return HostSample(
        throttled=False, throttled_flags="0x0", under_voltage=False, temp_c=47.2
    )


def _make_publisher(**overrides) -> HealthPublisher:
    kwargs = dict(
        mqtt_client=FakeMQTT(),
        device_id=DEVICE_ID,
        moonraker=FakeMoonraker(),
        publisher=FakePublisher(),
        token_manager=FakeTokenManager(),
        coordinator=FakeCoordinator(),
        interval_seconds=60.0,
        network_sampler=_fake_network,
        host_sampler=_fake_host,
    )
    kwargs.update(overrides)
    return HealthPublisher(**kwargs)


def test_health_topic_resolution() -> None:
    topics = constants.MQTTTopics.for_device(DEVICE_ID)
    assert topics.health == f"owl/printers/{DEVICE_ID}/health"


def test_build_envelope_has_all_catalog_fields() -> None:
    pub = _make_publisher()
    env = pub.build_envelope()

    assert env["$v"] == 2
    assert env["$type"] == "telemetry.health"
    assert env["$id"]
    assert env["$ts"]
    assert env["$origin"].startswith("moonraker-owl@")
    assert env["$seq"] == 0
    assert env["kind"] == "full"
    assert env["sessionId"] is None
    assert env["deviceId"] == DEVICE_ID

    payload = env["payload"]
    assert payload["uptimeSeconds"] >= 0
    assert payload["agentVersion"]

    mqtt = payload["mqtt"]
    assert mqtt["connected"] is True
    assert mqtt["lastConnectRc"] == 0
    assert mqtt["reconnectsTotal"] == 12
    assert mqtt["resubscribedTotal"] == 34
    buf = mqtt["offlineBuffer"]
    assert buf == {
        "pendingCoalesced": 1,
        "pendingEvents": 0,
        "bufferedTotal": 220,
        "droppedTotal": 3,
        "replayedTotal": 217,
    }

    assert payload["publishQueue"] == {"depth": 2, "maxsize": 100, "droppedTotal": 5}
    assert payload["moonraker"] == {
        "connected": True,
        "consecutiveFailures": 0,
        "pollFailures": 4,
        "outageSeconds": 0.0,
    }
    assert payload["token"] == {"renewalFailures": 0, "secondsUntilExpiry": 3120.0}


def test_build_envelope_v2_counters_segregate_token_refresh() -> None:
    """The user-facing churn signal must exclude planned token-refresh reconnects."""
    pub = _make_publisher()
    payload = pub.build_envelope()["payload"]

    counters = payload["counters"]
    # The full reconnect ledger keeps token refresh visible for diagnostics...
    assert counters["reconnects"]["total"] == 12
    assert counters["reconnects"]["byCause"]["token_renewed"] == 9
    assert counters["reconnects"]["byCause"]["connection_lost"] == 3
    # ...but the disconnect (churn) ledger structurally excludes it.
    assert "token_renewed" not in counters["disconnects"]["byCause"]
    assert counters["disconnects"]["total"] == 3
    assert counters["disconnects"]["byCause"]["connection_lost"] == 3
    # Offline-buffer overflow is renamed (loss, not packet loss) and surfaced here.
    assert counters["offlineBufferLost"] == 3


def test_build_envelope_v2_network_and_host() -> None:
    pub = _make_publisher()
    payload = pub.build_envelope()["payload"]

    assert payload["network"] == {"wifiRssiDbm": -58.0, "interface": "wlan0", "linkType": "wifi"}
    assert payload["host"] == {
        "throttled": False,
        "throttledFlags": "0x0",
        "underVoltage": False,
        "tempC": 47.2,
    }


def test_build_envelope_v2_recent_disconnects() -> None:
    pub = _make_publisher()
    recent = pub.build_envelope()["payload"]["recentDisconnects"]

    assert recent == [
        {
            "at": "2026-06-21T12:00:00+00:00",
            "cause": "connection_lost",
            "reasonCode": 7,
            "willReconnect": True,
        }
    ]


def test_build_envelope_wired_host_reports_null_rssi() -> None:
    pub = _make_publisher(network_sampler=lambda: NetworkSample(link_type="wired"))
    payload = pub.build_envelope()["payload"]
    assert payload["network"] == {"wifiRssiDbm": None, "interface": None, "linkType": "wired"}


def test_build_envelope_unknown_link_reports_null_rssi() -> None:
    # A host whose uplink we cannot classify must report linkType 'unknown' (never a false 'wired').
    pub = _make_publisher(network_sampler=lambda: NetworkSample(link_type="unknown"))
    payload = pub.build_envelope()["payload"]
    assert payload["network"] == {"wifiRssiDbm": None, "interface": None, "linkType": "unknown"}


def test_build_envelope_increments_seq() -> None:
    pub = _make_publisher()
    assert pub.build_envelope()["$seq"] == 0
    assert pub.build_envelope()["$seq"] == 1
    assert pub.build_envelope()["$seq"] == 2


def test_publish_once_uses_correct_topic_qos_retain_no_buffer() -> None:
    mqtt = FakeMQTT()
    pub = _make_publisher(mqtt_client=mqtt)
    pub.publish_once()

    assert len(mqtt.published) == 1
    msg = mqtt.published[0]
    assert msg["topic"] == f"owl/printers/{DEVICE_ID}/health"
    assert msg["qos"] == 1
    assert msg["retain"] is True
    assert msg["buffer_on_failure"] is False
    decoded = json.loads(msg["payload"].decode("utf-8"))
    assert decoded["$type"] == "telemetry.health"


def test_publish_failure_is_swallowed() -> None:
    mqtt = FakeMQTT()
    mqtt.raise_on_publish = True
    pub = _make_publisher(mqtt_client=mqtt)
    # Must not raise even though the underlying publish raises.
    pub.publish_once()
    assert mqtt.published == []


def test_build_envelope_tolerates_missing_sources() -> None:
    # All optional sources are None — a partially-initialised agent must still
    # produce a best-effort snapshot rather than raising.
    pub = HealthPublisher(
        mqtt_client=FakeMQTT(),
        device_id=DEVICE_ID,
        moonraker=None,
        publisher=None,
        token_manager=None,
        coordinator=None,
        interval_seconds=60.0,
    )
    payload = pub.build_envelope()["payload"]
    assert payload["moonraker"]["connected"] is False
    assert payload["publishQueue"]["droppedTotal"] == 0
    assert payload["token"]["secondsUntilExpiry"] is None
    assert payload["mqtt"]["reconnectsTotal"] == 0


def test_build_envelope_tolerates_raising_source() -> None:
    class Boom:
        @property
        def consecutive_failures(self):
            raise RuntimeError("boom")

        is_connected = False
        outage_seconds = 0.0

    pub = _make_publisher(moonraker=Boom())
    # A raising accessor degrades to the default, not a crash.
    payload = pub.build_envelope()["payload"]
    assert payload["moonraker"]["consecutiveFailures"] == 0


@pytest.mark.asyncio
async def test_start_is_idempotent() -> None:
    pub = _make_publisher()
    await pub.start()
    first = pub._task
    await pub.start()  # second start should not replace the task
    assert pub._task is first
    await pub.stop()


@pytest.mark.asyncio
async def test_stop_cancels_loop() -> None:
    pub = _make_publisher()
    await pub.start()
    assert pub._task is not None
    await pub.stop()
    assert pub._task is None


@pytest.mark.asyncio
async def test_loop_publishes_on_interval() -> None:
    mqtt = FakeMQTT()
    pub = _make_publisher(mqtt_client=mqtt)
    pub._interval_seconds = 0.02  # bypass the _MIN_INTERVAL_SECONDS construction clamp for speed
    await pub.start()
    await asyncio.sleep(0.1)
    await pub.stop()
    assert len(mqtt.published) >= 1


def test_interval_is_clamped_to_minimum() -> None:
    # A misconfigured/hostile sub-floor interval is clamped up to the shared _MIN_INTERVAL_SECONDS,
    # so the health loop can never be driven faster than the contract floor.
    pub = HealthPublisher(mqtt_client=FakeMQTT(), device_id=DEVICE_ID, interval_seconds=0.001)
    assert pub._interval_seconds == _MIN_INTERVAL_SECONDS
