"""Tests for the centralized MQTT topic registry (P1-12)."""

from moonraker_owl.constants import DeviceTopics, MQTTTopics


def test_resolve_substitutes_device_id():
    assert (
        MQTTTopics.resolve(MQTTTopics.STATUS, "dev-1")
        == "owl/printers/dev-1/status"
    )


def test_for_device_returns_consistent_topics():
    topics = MQTTTopics.for_device("dev-1")
    assert isinstance(topics, DeviceTopics)
    assert topics.base == "owl/printers/dev-1"
    assert topics.status == "owl/printers/dev-1/status"
    assert topics.sensors == "owl/printers/dev-1/sensors"
    assert topics.events == "owl/printers/dev-1/events"
    assert topics.objects == "owl/printers/dev-1/objects"
    assert topics.config_notify == "owl/printers/dev-1/config/notify"
    assert topics.commands_prefix == "owl/printers/dev-1/commands"
    assert topics.acks_prefix == "owl/printers/dev-1/acks"


def test_ack_topic_uses_acks_prefix():
    topics = MQTTTopics.for_device("dev-1")
    assert topics.ack("printer%3Apause") == "owl/printers/dev-1/acks/printer%3Apause"


def test_broadcast_topic_is_not_device_scoped():
    assert MQTTTopics.BROADCAST_CONFIG_UPDATE == "owl/broadcasts/config-update"


def test_channel_topics_match_legacy_format():
    """Centralized topics must match the previously hard-coded f-strings."""
    device_id = "abc-123"
    topics = MQTTTopics.for_device(device_id)
    assert topics.status == f"owl/printers/{device_id}/status"
    assert topics.sensors == f"owl/printers/{device_id}/sensors"
    assert topics.events == f"owl/printers/{device_id}/events"
    assert topics.objects == f"owl/printers/{device_id}/objects"
    assert topics.commands_prefix == f"owl/printers/{device_id}/commands"
    assert topics.config_notify == f"owl/printers/{device_id}/config/notify"
