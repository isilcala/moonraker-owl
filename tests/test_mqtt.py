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

    def connect_async(self, host, port, keepalive, **kwargs):
        self._events["connect_args"] = (host, port, keepalive)
        self._events["connect_kwargs"] = dict(kwargs)
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

    def will_set(self, topic, payload=None, qos=0, retain=False, properties=None):
        self._events["will_set"] = (topic, payload, qos, retain, properties)

    def tls_set(self, ca_certs=None, certfile=None, keyfile=None, cert_reqs=None, tls_version=None, ciphers=None):
        self._events["tls_set"] = {"cert_reqs": cert_reqs, "tls_version": tls_version}

    def disconnect(self):
        self._events["disconnect_called"] = True
        if self.on_disconnect:
            self._loop.call_soon(
                self.on_disconnect,
                self,
                None,
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

    def message_callback_add(self, topic, callback):
        self._events.setdefault("callbacks_added", []).append(topic)

    def unsubscribe(self, topic):
        self._events.setdefault("unsubscribed", []).append(topic)
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
    from unittest.mock import Mock
    
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

    # Create mock TokenManager for JWT authentication
    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt-token")

    client = MQTTClient(config, client_id="printer-42", token_manager=token_manager)
    await client.connect()

    yield client, events

    await client.disconnect()


@pytest.mark.asyncio
async def test_connect_configures_client(mqtt_client):
    client, events = mqtt_client

    assert events["connect_args"] == ("broker.owl.dev", 1883, 60)
    # JWT authentication: username=device_id, password=jwt_token
    assert events["auth"] == ("device-test", "mock-jwt-token")
    assert events.get("connect_kwargs", {}) == {}
    assert events["loop_start"] == 1


@pytest.mark.asyncio
async def test_last_will_applied_before_connect(monkeypatch):
    from unittest.mock import Mock
    
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

    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt")

    client = MQTTClient(config, client_id="printer-will", token_manager=token_manager)
    properties = object()
    client.set_last_will(
        "owl/printers/device/offline",
        b"offline",
        qos=1,
        retain=True,
        properties=properties,
    )

    await client.connect()
    await client.disconnect()

    assert events.get("will_set") == (
        "owl/printers/device/offline",
        b"offline",
        1,
        True,
        properties,
    )


@pytest.mark.asyncio
async def test_connect_with_session_expiry(monkeypatch):
    from unittest.mock import Mock
    
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

    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt")

    client = MQTTClient(config, client_id="printer-session", token_manager=token_manager)
    await client.connect(clean_start=False, session_expiry=120)

    try:
        kwargs = events.get("connect_kwargs", {})
        assert kwargs.get("clean_start") is False
        properties = kwargs.get("properties")
        assert properties is not None
        assert getattr(properties, "SessionExpiryInterval", None) == 120
    finally:
        await client.disconnect()


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
    from unittest.mock import Mock
    
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

    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt")

    client = MQTTClient(config, client_id="printer-handler", token_manager=token_manager)

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
    from unittest.mock import Mock
    
    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(
            loop, events, *args, publish_rc=mqtt.MQTT_ERR_NO_CONN, **kwargs
        )

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(
        base_url="https://api.owl.dev",
        broker_host="broker.owl.dev",
        broker_port=1883,
    )

    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt")

    client = MQTTClient(config, client_id="printer-pub", token_manager=token_manager)
    await client.connect()

    with pytest.raises(MQTTConnectionError):
        client.publish("test", b"payload")

    await client.disconnect()


@pytest.mark.asyncio
async def test_publish_buffers_on_failure_and_replays(monkeypatch):
    from unittest.mock import Mock

    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(
            loop, events, *args, publish_rc=mqtt.MQTT_ERR_NO_CONN, **kwargs
        )

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(broker_host="broker.owl.dev", broker_port=1883)
    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt")

    client = MQTTClient(config, client_id="printer-buf", token_manager=token_manager)
    await client.connect()

    # Failing publishes with buffering enabled must NOT raise.
    client.publish("owl/p/events", b"e1", qos=2, buffer_on_failure=True, coalesce=False)
    client.publish("owl/p/status", b"s1", qos=1, buffer_on_failure=True, coalesce=True)
    client.publish("owl/p/status", b"s2", qos=1, buffer_on_failure=True, coalesce=True)

    stats = client.offline_buffer_stats()
    assert stats["pending_events"] == 1
    assert stats["pending_coalesced"] == 1  # latest-wins coalescing
    assert stats["pending"] == 2

    # Recover the broker and replay (only buffered frames are flushed now).
    before = len(events.get("published", []))
    client._client._publish_rc = mqtt.MQTT_ERR_SUCCESS  # type: ignore[union-attr]
    client._replay_offline_buffer()

    replayed = [p[1] for p in events.get("published", [])[before:]]
    assert b"s2" in replayed  # latest status replayed
    assert b"e1" in replayed  # event replayed
    assert b"s1" not in replayed  # stale status coalesced away

    stats = client.offline_buffer_stats()
    assert stats["pending"] == 0
    assert stats["replayed_total"] == 2

    await client.disconnect()


@pytest.mark.asyncio
async def test_publish_buffers_when_disconnected(monkeypatch):
    from unittest.mock import Mock

    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(loop, events, *args, **kwargs)

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(broker_host="broker.owl.dev", broker_port=1883)
    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt")

    client = MQTTClient(config, client_id="printer-off", token_manager=token_manager)
    # No connect(): _client is None.

    # Without buffering, a disconnected publish raises.
    with pytest.raises(RuntimeError):
        client.publish("owl/p/status", b"x")

    # With buffering, it is held instead of raising.
    client.publish("owl/p/status", b"x", buffer_on_failure=True, coalesce=True)
    assert client.offline_buffer_stats()["pending"] == 1


@pytest.mark.asyncio
async def test_offline_event_buffer_drops_oldest(monkeypatch):
    from unittest.mock import Mock

    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(loop, events, *args, **kwargs)

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(broker_host="broker.owl.dev", broker_port=1883)
    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt")

    client = MQTTClient(
        config,
        client_id="printer-cap",
        token_manager=token_manager,
        offline_buffer_size=2,
    )

    for i in range(5):
        client.publish(
            "owl/p/events",
            f"e{i}".encode(),
            buffer_on_failure=True,
            coalesce=False,
        )

    stats = client.offline_buffer_stats()
    assert stats["pending_events"] == 2  # bounded
    assert stats["dropped_total"] == 3  # oldest three evicted

# has been moved to ConnectionCoordinator (see connection.py).
# MQTTClient now only provides connect/disconnect as a pure communication layer.


@pytest.mark.asyncio
async def test_disconnect_handler_invoked(monkeypatch):
    from unittest.mock import Mock
    
    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(loop, events, rc_disconnect=1, *args, **kwargs)

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(broker_host="broker.owl.dev", broker_port=1883)
    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt")
    client = MQTTClient(config, client_id="printer-123", token_manager=token_manager)

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


# -------------------------------------------------------------------------
# MQTT v5 Auth Failure Tests (rc=134, rc=135)
# -------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("auth_failure_rc", [5, 134, 135])
async def test_connect_retries_on_auth_failure_with_token_refresh(monkeypatch, auth_failure_rc):
    """Test that auth failure (rc=5, 134, 135) triggers token refresh and retry."""
    from unittest.mock import Mock, AsyncMock
    
    loop = asyncio.get_running_loop()
    events: dict = {}
    attempt_counter = {"count": 0}

    def factory(*args, **kwargs):
        """Factory that fails on first attempt, succeeds on second."""
        client = FakeMqttClient(loop, events, *args, **kwargs)
        original_connect_async = client.connect_async
        
        def patched_connect_async(host, port, keepalive, **kw):
            attempt_counter["count"] += 1
            if attempt_counter["count"] == 1:
                # First attempt fails with auth error
                events["connect_args"] = (host, port, keepalive)
                events["connect_kwargs"] = dict(kw)
                if client.on_connect:
                    loop.call_soon(
                        client.on_connect,
                        client,
                        None,
                        None,
                        auth_failure_rc,
                        None,
                    )
            else:
                # Second attempt succeeds
                original_connect_async(host, port, keepalive, **kw)
        
        client.connect_async = patched_connect_async
        return client

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(
        base_url="https://api.owl.dev",
        broker_host="broker.owl.dev",
        broker_port=1883,
    )

    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt")
    token_manager.refresh_token_now = AsyncMock()

    client = MQTTClient(config, client_id="printer-auth-test", token_manager=token_manager)

    # Should succeed after retry
    await client.connect()

    # Verify token was refreshed after first failure
    token_manager.refresh_token_now.assert_called_once()
    # Verify two connection attempts were made
    assert attempt_counter["count"] == 2
    await client.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize("auth_failure_rc", [134, 135])
async def test_mqttv5_auth_failure_raises_when_no_token_manager(monkeypatch, auth_failure_rc):
    """Test that MQTT v5 auth failures (rc=134, 135) raise error when no TokenManager."""
    loop = asyncio.get_running_loop()
    events: dict = {}

    def factory(*args, **kwargs):
        return FakeMqttClient(loop, events, rc_connect=auth_failure_rc, *args, **kwargs)

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(
        base_url="https://api.owl.dev",
        broker_host="broker.owl.dev",
        broker_port=1883,
    )

    # No token manager means JWT auth check fails first
    client = MQTTClient(config, client_id="printer-no-token")

    with pytest.raises(MQTTConnectionError):
        await client.connect()


@pytest.mark.asyncio
async def test_mqttv5_bad_password_rc134_triggers_auth_failure(monkeypatch):
    """Test specifically that rc=134 (Bad user name or password) is handled as auth failure."""
    from unittest.mock import Mock, AsyncMock
    
    loop = asyncio.get_running_loop()
    events: dict = {}

    # Always fail with rc=134
    def factory(*args, **kwargs):
        return FakeMqttClient(loop, events, rc_connect=134, *args, **kwargs)

    monkeypatch.setattr("moonraker_owl.adapters.mqtt.mqtt.Client", factory)

    config = CloudConfig(
        base_url="https://api.owl.dev",
        broker_host="broker.owl.dev",
        broker_port=1883,
    )

    token_manager = Mock()
    token_manager.get_mqtt_credentials.return_value = ("device-test", "mock-jwt")
    token_manager.refresh_token_now = AsyncMock()

    client = MQTTClient(config, client_id="printer-rc134", token_manager=token_manager)

    # Connection should fail after 2 attempts (initial + 1 retry)
    with pytest.raises(MQTTConnectionError, match="rejected connection"):
        await client.connect()

    # Token refresh should have been attempted once (after first failure)
    token_manager.refresh_token_now.assert_called_once()


@pytest.mark.asyncio
async def test_resubscribes_tracked_topics_on_reconnect(mqtt_client):
    """Tracked subscriptions are replayed when the broker reconnects."""
    client, events = mqtt_client

    client.subscribe("owl/cmd/#", qos=1)
    client.subscribe_with_handler("owl/state/#", lambda t, p: None, qos=1)

    # Clear the initial subscribe bookkeeping to isolate the replay.
    events["subscribed"] = []
    events["callbacks_added"] = []

    # Simulate a reconnect: paho invokes on_connect again (session_present
    # unknown -> treated as False, forcing a full resubscribe).
    fake = client._client
    fake.on_connect(fake, None, None, 0, None)
    await asyncio.sleep(0)

    topics = {t for t, _ in events["subscribed"]}
    assert topics == {"owl/cmd/#", "owl/state/#"}
    # Handler-backed subscription must have its callback re-added.
    assert "owl/state/#" in events["callbacks_added"]
    stats = client.subscription_stats()
    assert stats["tracked"] == 2
    assert stats["resubscribed_total"] >= 2


@pytest.mark.asyncio
async def test_unsubscribe_drops_tracked_subscription(mqtt_client):
    client, events = mqtt_client

    client.subscribe("owl/cmd/#", qos=1)
    assert client.subscription_stats()["tracked"] == 1

    client.unsubscribe("owl/cmd/#")
    assert client.subscription_stats()["tracked"] == 0
    assert "owl/cmd/#" in events["unsubscribed"]


@pytest.mark.asyncio
async def test_reconnect_tears_down_prior_client(mqtt_client):
    """A second connect without disconnect stops the previous paho client."""
    client, events = mqtt_client

    loop_stops_before = events.get("loop_stop", 0)
    await client.connect()
    # The prior client''s network loop must have been stopped to avoid fd leaks.
    assert events.get("loop_stop", 0) > loop_stops_before


def _make_bare_client():
    config = CloudConfig(broker_host="broker.owl.dev", broker_port=1883)
    return MQTTClient(config, client_id="printer-rc", token_manager=None)


def test_last_connect_rc_accessor_reflects_internal_state():
    client = _make_bare_client()
    assert client.last_connect_rc is None
    client._last_connect_rc = 5
    assert client.last_connect_rc == 5


def test_call_soon_on_loop_skips_closed_loop():
    client = _make_bare_client()
    loop = asyncio.new_event_loop()
    loop.close()
    client._loop = loop
    # Must not raise even though the loop is closed.
    client._call_soon_on_loop(lambda: None)


def test_call_soon_on_loop_skips_missing_loop():
    client = _make_bare_client()
    client._loop = None
    client._call_soon_on_loop(lambda: None)


def test_run_coro_on_loop_closes_coro_when_loop_closed():
    client = _make_bare_client()
    loop = asyncio.new_event_loop()
    loop.close()

    async def _coro():
        return None

    coro = _coro()
    client._run_coro_on_loop(coro, loop)
    # Submitting again on the now-closed coroutine raises (already closed).
    with pytest.raises(RuntimeError):
        coro.send(None)
