"""Tests for the events/disconnect publisher (ADR-0045)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from moonraker_owl import constants
from moonraker_owl.connection import DisconnectRecord, ReconnectReason
from moonraker_owl.disconnect_event_publisher import DisconnectEventPublisher

DEVICE_ID = "550e8400-e29b-41d4-a716-446655440000"


class FakeMQTT:
    def __init__(self) -> None:
        self.published: list[dict] = []
        self.raise_on_publish = False

    def publish(self, topic, payload, qos=1, retain=False, *, buffer_on_failure=False):
        if self.raise_on_publish:
            raise RuntimeError("not connected")
        self.published.append(
            {
                "topic": topic,
                "payload": payload,
                "qos": qos,
                "retain": retain,
                "buffer_on_failure": buffer_on_failure,
            }
        )


def _record() -> DisconnectRecord:
    return DisconnectRecord(
        at=datetime(2026, 6, 21, 23, 47, 12, tzinfo=timezone.utc),
        cause=ReconnectReason.CONNECTION_LOST.value,
        reason_code=7,
        will_reconnect=True,
    )


def test_publish_uses_disconnect_topic_qos1_not_retained() -> None:
    mqtt = FakeMQTT()
    pub = DisconnectEventPublisher(mqtt_client=mqtt, device_id=DEVICE_ID)

    pub.publish(_record())

    assert len(mqtt.published) == 1
    msg = mqtt.published[0]
    assert msg["topic"] == f"owl/printers/{DEVICE_ID}/events/disconnect"
    assert msg["qos"] == 1
    assert msg["retain"] is False
    assert msg["buffer_on_failure"] is False


def test_envelope_shape() -> None:
    mqtt = FakeMQTT()
    pub = DisconnectEventPublisher(mqtt_client=mqtt, device_id=DEVICE_ID)

    env = pub.build_envelope(_record())

    assert env["$v"] == 2
    assert env["$type"] == "telemetry.event.disconnect"
    assert env["$id"]
    assert env["$origin"].startswith("moonraker-owl@")
    assert env["deviceId"] == DEVICE_ID
    assert env["payload"] == {
        "at": "2026-06-21T23:47:12+00:00",
        "cause": "connection_lost",
        "reasonCode": 7,
        "willReconnect": True,
    }


def test_published_payload_round_trips() -> None:
    mqtt = FakeMQTT()
    pub = DisconnectEventPublisher(mqtt_client=mqtt, device_id=DEVICE_ID)

    pub.publish(_record())

    decoded = json.loads(mqtt.published[0]["payload"].decode("utf-8"))
    assert decoded["$type"] == "telemetry.event.disconnect"
    assert decoded["payload"]["cause"] == "connection_lost"


def test_publish_failure_is_swallowed() -> None:
    mqtt = FakeMQTT()
    mqtt.raise_on_publish = True
    pub = DisconnectEventPublisher(mqtt_client=mqtt, device_id=DEVICE_ID)

    # Must not raise even though the underlying publish raises.
    pub.publish(_record())

    assert mqtt.published == []


def test_topic_matches_registry() -> None:
    topics = constants.MQTTTopics.for_device(DEVICE_ID)
    assert topics.events_disconnect == f"owl/printers/{DEVICE_ID}/events/disconnect"
