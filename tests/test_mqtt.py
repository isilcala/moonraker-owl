"""Tests for the MQTT adapter."""

import asyncio
from types import SimpleNamespace

import pytest
import pytest_asyncio

from moonraker_owl.adapters import MQTTClient, MQTTConnectionError
from moonraker_owl.config import CloudConfig

import paho.mqtt.client as mqtt


class FakeMqttClient:
    """Minimal fake paho-mqtt client for testing."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        events: dict,
        *,
        rc_connect: int = 0,
        rc_disconnect: int = 0,
        publish_rc: int = mqtt.MQTT_ERR_SUCCESS,
        subscribe_rc: int = mqtt.MQTT_ERR_SUCCESS,
        **unused,
    ):
        self._loop = loop
        self._events = events
        self._rc_connect = rc_connect
        self._rc_disconnect = rc_disconnect
        self._publish_rc = publish_rc
        self._subscribe_rc = subscribe_rc

        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None

    # paho interface -------------------------------------------------
    def enable_logger(self, logger):
        self._events.setdefault("logger_enabled", True)

    def username_pw_set(self, username, password=None):
        self._events["auth"] = (username, password)

    def connect_async(self, host, port, keepalive):
        self._events["connect_args"] = (host, port, keepalive)
        if self.on_connect:
            self._loop.call_soon(
                self.on_connect,
                self,
                None,
                None,
                self._rc_connect,
                None,
            )

    def loop_start(self):
        self._events["loop_start"] = self._events.get("loop_start", 0) + 1

    def loop_stop(self):
        self._events["loop_stop"] = self._events.get("loop_stop", 0) + 1

    def disconnect(self):
        self._events["disconnect_called"] = True
        if self.on_disconnect:
            self._loop.call_soon(
                self.on_disconnect,
                self,
                None,
                self._rc_disconnect,
                None,
            )

    def publish(self, topic, payload, qos=0, retain=False, properties=None):
        self._events.setdefault("published", []).append(
            (topic, payload, qos, retain, properties)
        )
        return SimpleNamespace(rc=self._publish_rc)

    def subscribe(self, topic, qos=0):
        self._events.setdefault("subscribed", []).append((topic, qos))
        return self._subscribe_rc, 1

    def reconnect(self):
        self._events["reconnect_called"] = self._events.get("reconnect_called", 0) + 1
        if self.on_connect:
            self._loop.call_soon(
                self.on_connect,
                self,
                None,
                None,
                self._rc_connect,
                None,
            )
        return mqtt.MQTT_ERR_SUCCESS

    def reconnect_async(self):  # pragma: no cover - optional path
        return self.reconnect()


@pytest_asyncio.fixture
async def mqtt_client(monkeypatch):
    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(loop, events, *args, **kwargs)

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(
        base_url="https://api.owl.dev",
        broker_host="broker.owl.dev",
        broker_port=1883,
        username="tenant:device",
        password="token",
    )

    client = MQTTClient(config, client_id="printer-42")
    await client.connect()

    yield client, events

    await client.disconnect()


@pytest.mark.asyncio
async def test_connect_configures_client(mqtt_client):
    client, events = mqtt_client

    assert events["connect_args"] == ("broker.owl.dev", 1883, 60)
    assert events["auth"] == ("tenant:device", "token")
    assert events["loop_start"] == 1


@pytest.mark.asyncio
async def test_publish_delegates_to_client(mqtt_client):
    client, events = mqtt_client

    client.publish("test/topic", b"payload", qos=1, retain=True)

    assert events["published"] == [("test/topic", b"payload", 1, True, None)]


@pytest.mark.asyncio
async def test_subscribe_records_topics(mqtt_client):
    client, events = mqtt_client

    client.subscribe("commands/#", qos=2)

    assert events["subscribed"] == [("commands/#", 2)]


@pytest.mark.asyncio
async def test_message_handler_dispatches_async(monkeypatch):
    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(loop, events, *args, **kwargs)

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(
        base_url="https://api.owl.dev",
        broker_host="broker.owl.dev",
        broker_port=1883,
    )

    client = MQTTClient(config, client_id="printer-99")

    message_event = asyncio.Event()

    async def handler(topic: str, payload: bytes) -> None:
        events["handled"] = (topic, payload)
        message_event.set()

    client.set_message_handler(handler)
    await client.connect()

    message = SimpleNamespace(topic="owl/topic", payload=b"data")
    client._on_message(client._client, None, message)  # type: ignore[arg-type]

    await asyncio.wait_for(message_event.wait(), timeout=1.0)
    await client.disconnect()

    assert events["handled"] == ("owl/topic", b"data")


@pytest.mark.asyncio
async def test_publish_failure_raises(monkeypatch):
    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(
            loop, events, publish_rc=mqtt.MQTT_ERR_NO_CONN, *args, **kwargs
        )

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(
        base_url="https://api.owl.dev",
        broker_host="broker.owl.dev",
        broker_port=1883,
    )

    client = MQTTClient(config, client_id="printer-7")
    await client.connect()

    with pytest.raises(MQTTConnectionError):
        client.publish("test", b"payload")

    await client.disconnect()


@pytest.mark.asyncio
async def test_reconnect_triggers_paho(mqtt_client):
    client, events = mqtt_client

    await client.reconnect()

    assert events.get("reconnect_called") == 1


@pytest.mark.asyncio
async def test_disconnect_handler_invoked(monkeypatch):
    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(loop, events, rc_disconnect=1, *args, **kwargs)

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(broker_host="broker.owl.dev", broker_port=1883)
    client = MQTTClient(config, client_id="printer-123")

    disconnect_event = asyncio.Event()

    def _handler(rc: int) -> None:
        events["disconnect_rc"] = rc
        disconnect_event.set()

    client.register_disconnect_handler(_handler)

    await client.connect()
    await client.disconnect()

    await asyncio.wait_for(disconnect_event.wait(), timeout=1.0)
    assert events.get("disconnect_rc") == 1


@pytest.mark.asyncio
async def test_connect_failure_raises(monkeypatch):
    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(loop, events, rc_connect=5, *args, **kwargs)

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(
        base_url="https://api.owl.dev",
        broker_host="broker.owl.dev",
        broker_port=1883,
    )

    client = MQTTClient(config, client_id="printer-8")

    with pytest.raises(MQTTConnectionError):
        await client.connect()
