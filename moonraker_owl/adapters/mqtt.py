"""MQTT adapter encapsulating paho-mqtt client usage."""

from __future__ import annotations

import asyncio
import logging
import ssl
from collections import OrderedDict, deque
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, List, Optional

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from ..config import CloudConfig

if TYPE_CHECKING:
    from ..token_manager import TokenManager

LOGGER = logging.getLogger(__name__)

MessageHandler = Callable[[str, bytes], Awaitable[None] | None]

DEFAULT_OFFLINE_BUFFER_SIZE = 200


class _BufferedMessage:
    """A telemetry message held in memory while the broker is unreachable."""

    __slots__ = ("topic", "payload", "qos", "retain", "properties", "coalesce_key")

    def __init__(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        retain: bool,
        properties: Any,
        coalesce_key: Optional[str],
    ) -> None:
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain
        self.properties = properties
        self.coalesce_key = coalesce_key


class _Subscription:
    """A tracked topic subscription for replay after reconnect."""

    __slots__ = ("topic", "qos", "handler")

    def __init__(
        self, topic: str, qos: int, handler: Optional[MessageHandler]
    ) -> None:
        self.topic = topic
        self.qos = qos
        self.handler = handler


class MQTTConnectionError(RuntimeError):
    """Raised when the MQTT client fails to establish a connection."""
class MQTTClient:
    """Async-friendly wrapper over the threaded paho-mqtt client."""

    def __init__(
        self,
        config: CloudConfig,
        *,
        client_id: str,
        keepalive: int = 60,
        token_manager: Optional[TokenManager] = None,
        offline_buffer_size: int = DEFAULT_OFFLINE_BUFFER_SIZE,
    ) -> None:
        self.config = config
        self.client_id = client_id
        self.keepalive = keepalive
        self.token_manager = token_manager

        self._client: Optional[mqtt.Client] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._connected_event: Optional[asyncio.Event] = None
        self._disconnect_event: Optional[asyncio.Event] = None
        self._message_handler: Optional[MessageHandler] = None
        self._last_connect_rc: Optional[int] = None
        self._connected: bool = False
        self._disconnect_handlers: List[Callable[[int], None]] = []
        self._connect_handlers: List[Callable[[int], None]] = []
        self._last_will: Optional[Dict[str, Any]] = None

        # Tracked subscriptions for replay after reconnect. A new paho client is
        # created per connect attempt (and broker restarts clear the session),
        # so subscriptions and per-topic handlers must be re-applied whenever we
        # (re)connect, otherwise command/telemetry channels go silently deaf.
        self._subscriptions: "OrderedDict[str, _Subscription]" = OrderedDict()
        self._resubscribed_total = 0

        # In-memory offline buffer (Q1: no disk persistence). Latest-wins
        # coalesced telemetry (status/sensors/objects) keyed by topic, plus a
        # bounded best-effort event buffer that drops oldest on overflow.
        self._offline_buffer_size = max(int(offline_buffer_size), 0)
        self._offline_coalesced: "OrderedDict[str, _BufferedMessage]" = OrderedDict()
        self._offline_events: "deque[_BufferedMessage]" = deque(
            maxlen=self._offline_buffer_size or 1
        )
        self._offline_buffered_total = 0
        self._offline_dropped_total = 0
        self._offline_replayed_total = 0

    async def connect(
        self,
        timeout: float = 30.0,
        *,
        clean_start: bool = True,
        session_expiry: Optional[int] = None,
    ) -> None:
        """Connect to the MQTT broker and wait for acknowledgement.
        
        If JWT authentication fails (CONNACK code 5, 134, or 135) and TokenManager
        is available, automatically refreshes the token and retries connection
        (max 2 attempts).
        """
        max_attempts = 2 if self.token_manager else 1
        
        for attempt in range(1, max_attempts + 1):
            try:
                await self._attempt_connect(
                    timeout=timeout,
                    clean_start=clean_start,
                    session_expiry=session_expiry,
                    attempt=attempt,
                )
                return  # Connection successful
            except MQTTConnectionError as exc:
                # Check if this is an authentication failure
                # MQTT v3.1.1: rc=5 = Not authorized
                # MQTT v5: rc=134 = Bad user name or password, rc=135 = Not authorized
                auth_failure_codes = (5, 134, 135)
                if self._last_connect_rc in auth_failure_codes and self.token_manager and attempt < max_attempts:
                    LOGGER.warning(
                        "MQTT authentication failed (rc=%d), refreshing JWT token and retrying (attempt %d/%d)",
                        self._last_connect_rc,
                        attempt,
                        max_attempts,
                    )
                    try:
                        await self.token_manager.refresh_token_now()
                        LOGGER.info("JWT token refreshed successfully")
                        # Continue to next attempt
                        continue
                    except Exception as refresh_exc:
                        LOGGER.error("Failed to refresh JWT token: %s", refresh_exc, exc_info=True)
                        # Fall through to raise original connection error
                
                # Not an auth failure, or no more retries, or token refresh failed
                raise

    async def _attempt_connect(
        self,
        timeout: float,
        *,
        clean_start: bool,
        session_expiry: Optional[int],
        attempt: int,
    ) -> None:
        """Internal method to attempt a single MQTT connection."""

        self._loop = asyncio.get_running_loop()
        self._connected_event = asyncio.Event()
        self._disconnect_event = asyncio.Event()
        self._last_connect_rc = None

        # Tear down any prior client first so a reconnect that bypassed
        # disconnect() cannot leak its paho network thread / socket fd.
        if self._client is not None:
            try:
                self._client.loop_stop()
            except Exception:  # pragma: no cover - best-effort cleanup
                LOGGER.debug("Prior MQTT client loop_stop failed", exc_info=True)
            self._client = None

        # JWT authentication: clientId MUST match JWT sub claim (device_id)
        if not self.token_manager:
            raise MQTTConnectionError(
                "TokenManager is required for JWT authentication. "
                "Ensure device_private_key is configured (run: moonraker-owl link)"
            )

        device_id, jwt_token = self.token_manager.get_mqtt_credentials()
        actual_client_id = device_id
        username = device_id
        password = jwt_token

        client_kwargs: dict[str, Any] = {
            "client_id": actual_client_id,
            "protocol": mqtt.MQTTv5,
        }

        callback_api_version = getattr(mqtt, "CallbackAPIVersion", None)
        if callback_api_version is not None:
            client_kwargs["callback_api_version"] = callback_api_version.VERSION2

        client = mqtt.Client(**client_kwargs)
        client.enable_logger(LOGGER)

        # Configure TLS if enabled
        if self.config.broker_use_tls:
            LOGGER.info("Enabling TLS for MQTT connection")
            # Use system CA certificates for server verification
            client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)

        if self._last_will is not None:
            client.will_set(**self._last_will)

        client.username_pw_set(username, password)

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message

        self._client = client

        LOGGER.info(
            "Connecting to MQTT broker %s:%s (attempt %d)",
            self.config.broker_host,
            self.config.broker_port,
            attempt,
        )

        connect_kwargs: Dict[str, Any] = {}
        if not clean_start:
            connect_kwargs["clean_start"] = False

        properties = None
        if session_expiry is not None:
            properties = Properties(PacketTypes.CONNECT)
            properties.SessionExpiryInterval = session_expiry

        if properties is not None:
            connect_kwargs["properties"] = properties

        client.connect_async(
            self.config.broker_host,
            self.config.broker_port,
            self.keepalive,
            **connect_kwargs,
        )
        client.loop_start()

        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=timeout)
            if self._last_connect_rc is None or self._last_connect_rc != 0:
                raise MQTTConnectionError(
                    f"MQTT broker rejected connection (rc={self._last_connect_rc})"
                )
        except asyncio.TimeoutError as exc:
            client.loop_stop()
            raise MQTTConnectionError("Timed out connecting to MQTT broker") from exc
        except MQTTConnectionError:
            client.loop_stop()
            raise

    async def disconnect(self, timeout: float = 5.0) -> None:
        """Gracefully disconnect from the broker."""

        if not self._client:
            return

        assert self._disconnect_event is not None

        self._client.disconnect()

        try:
            await asyncio.wait_for(self._disconnect_event.wait(), timeout=timeout)
        finally:
            self._client.loop_stop()
            self._client = None
        self._connected = False

    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 1,
        retain: bool = False,
        *,
        properties=None,
        buffer_on_failure: bool = False,
        coalesce: bool = False,
    ) -> None:
        """Publish *payload* to *topic*.

        When ``buffer_on_failure`` is set, a disconnected client or a non-success
        publish result enqueues the message into the in-memory offline buffer
        instead of raising, so it can be replayed once the broker reconnects.
        ``coalesce`` keeps only the latest message per topic (status/sensors/
        objects); otherwise messages are appended to the best-effort event
        buffer that drops the oldest entry on overflow.
        """

        coalesce_key = topic if coalesce else None

        if not self._client:
            if buffer_on_failure:
                self._buffer_message(
                    _BufferedMessage(
                        topic, payload, qos, retain, properties, coalesce_key
                    )
                )
                return
            raise RuntimeError("MQTT client not connected")

        info = self._client.publish(
            topic, payload, qos=qos, retain=retain, properties=properties
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            if buffer_on_failure:
                self._buffer_message(
                    _BufferedMessage(
                        topic, payload, qos, retain, properties, coalesce_key
                    )
                )
                return
            raise MQTTConnectionError(f"Publish failed with rc={info.rc}")

    def _buffer_message(self, message: _BufferedMessage) -> None:
        """Store *message* in the bounded in-memory offline buffer."""

        if self._offline_buffer_size == 0:
            self._offline_dropped_total += 1
            return

        if message.coalesce_key is not None:
            existed = message.coalesce_key in self._offline_coalesced
            self._offline_coalesced[message.coalesce_key] = message
            self._offline_coalesced.move_to_end(message.coalesce_key)
            if not existed:
                self._offline_buffered_total += 1
            # Distinct coalesced topics are few, but bound defensively.
            while len(self._offline_coalesced) > self._offline_buffer_size:
                self._offline_coalesced.popitem(last=False)
                self._offline_dropped_total += 1
        else:
            if (
                self._offline_events.maxlen is not None
                and len(self._offline_events) >= self._offline_events.maxlen
            ):
                # deque.append will evict the oldest entry.
                self._offline_dropped_total += 1
            self._offline_events.append(message)
            self._offline_buffered_total += 1

    def _replay_offline_buffer(self) -> None:
        """Flush buffered messages to the broker after a reconnect (loop thread)."""

        if self._client is None:
            return

        # Coalesced (latest-wins) frames first, then events in FIFO order.
        messages = list(self._offline_coalesced.values()) + list(self._offline_events)
        self._offline_coalesced.clear()
        self._offline_events.clear()
        if not messages:
            return

        replayed = 0
        for index, message in enumerate(messages):
            try:
                info = self._client.publish(
                    message.topic,
                    message.payload,
                    qos=message.qos,
                    retain=message.retain,
                    properties=message.properties,
                )
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception(
                    "Failed to replay buffered MQTT message for %s", message.topic
                )
                self._rebuffer(messages[index:])
                break
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                # Broker not ready yet; keep the remainder for the next attempt.
                self._rebuffer(messages[index:])
                break
            replayed += 1

        if replayed:
            self._offline_replayed_total += replayed
            LOGGER.info(
                "Replayed %d buffered MQTT message(s) after reconnect", replayed
            )

    def _rebuffer(self, messages: List[_BufferedMessage]) -> None:
        for message in messages:
            self._buffer_message(message)

    def offline_buffer_stats(self) -> Dict[str, int]:
        """Return counters describing the in-memory offline buffer for health."""

        return {
            "pending": len(self._offline_coalesced) + len(self._offline_events),
            "pending_coalesced": len(self._offline_coalesced),
            "pending_events": len(self._offline_events),
            "buffered_total": self._offline_buffered_total,
            "dropped_total": self._offline_dropped_total,
            "replayed_total": self._offline_replayed_total,
        }

    def set_last_will(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int = 0,
        retain: bool = False,
        properties=None,
    ) -> None:
        self._last_will = {
            "topic": topic,
            "payload": payload,
            "qos": qos,
            "retain": retain,
        }
        if properties is not None:
            self._last_will["properties"] = properties

    def subscribe(self, topic: str, qos: int = 1) -> None:
        if not self._client:
            raise RuntimeError("MQTT client not connected")
        self._subscriptions[topic] = _Subscription(topic, qos, None)
        result, _ = self._client.subscribe(topic, qos=qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise MQTTConnectionError(f"Subscribe failed with rc={result}")

    def subscribe_with_handler(
        self,
        topic: str,
        handler: MessageHandler,
        qos: int = 1,
    ) -> None:
        """Subscribe to *topic* and route matching messages to *handler*.

        Uses paho-mqtt's ``message_callback_add`` so the handler fires
        independently of the global ``set_message_handler`` callback.
        """
        if not self._client:
            raise RuntimeError("MQTT client not connected")

        self._subscriptions[topic] = _Subscription(topic, qos, handler)
        self._client.message_callback_add(topic, self._make_bridge(topic, handler))
        result, _ = self._client.subscribe(topic, qos=qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise MQTTConnectionError(f"Subscribe failed with rc={result}")

    def _make_bridge(
        self, topic: str, handler: MessageHandler
    ) -> Callable[[Any, Any, Any], None]:
        """Build a paho message-callback that bridges into the asyncio loop."""

        def _bridge(
            client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
        ) -> None:
            loop = self._loop
            if not loop:
                return
            try:
                result = handler(message.topic, message.payload)
                if asyncio.iscoroutine(result):
                    self._run_coro_on_loop(result, loop)
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("Topic handler raised for %s", topic)

        return _bridge

    def set_message_handler(self, handler: Optional[MessageHandler]) -> None:
        self._message_handler = handler

    def unsubscribe(self, topic: str) -> None:
        if not self._client:
            raise RuntimeError("MQTT client not connected")

        self._subscriptions.pop(topic, None)
        result, _ = self._client.unsubscribe(topic)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise MQTTConnectionError(f"Unsubscribe failed with rc={result}")

    def _replay_subscriptions(self) -> None:
        """Re-apply all tracked subscriptions after a (re)connect.

        Always re-adds per-topic message callbacks (a fresh paho client loses
        them) and re-issues the SUBSCRIBE packets so neither a new client nor a
        broker-side session reset can leave a channel silently deaf. Scheduled
        on the asyncio loop thread, where ``subscribe`` is safe to call.
        """
        client = self._client
        if client is None or not self._subscriptions:
            return
        for sub in list(self._subscriptions.values()):
            try:
                if sub.handler is not None:
                    client.message_callback_add(
                        sub.topic, self._make_bridge(sub.topic, sub.handler)
                    )
                result, _ = client.subscribe(sub.topic, qos=sub.qos)
                if result != mqtt.MQTT_ERR_SUCCESS:
                    LOGGER.error(
                        "Resubscribe failed for %s (rc=%s)", sub.topic, result
                    )
                else:
                    self._resubscribed_total += 1
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("Resubscribe raised for %s", sub.topic)

    def subscription_stats(self) -> Dict[str, int]:
        """Expose subscription/replay counters for health reporting."""
        return {
            "tracked": len(self._subscriptions),
            "resubscribed_total": self._resubscribed_total,
        }

    @staticmethod
    def _extract_session_present(flags: Any) -> bool:
        """Read ``session_present`` from paho v1 (dict) or v2 (object) flags."""
        if flags is None:
            return False
        value = getattr(flags, "session_present", None)
        if value is None and isinstance(flags, dict):
            value = flags.get("session_present", False)
        return bool(value)

    def register_disconnect_handler(self, handler: Callable[[int], None]) -> None:
        self._disconnect_handlers.append(handler)

    def register_connect_handler(self, handler: Callable[[int], None]) -> None:
        self._connect_handlers.append(handler)

    def is_connected(self) -> bool:
        return self._connected

    @property
    def last_connect_rc(self) -> Optional[int]:
        """Most recent CONNACK reason code.

        Public accessor so callers (e.g. the connection supervisor) do not
        reach into the private ``_last_connect_rc`` attribute. Reading a single
        ``Optional[int]`` is atomic under CPython's GIL, so no extra locking is
        required for this cross-thread read.
        """
        return self._last_connect_rc

    def _call_soon_on_loop(self, callback: Callable[..., Any], *args: Any) -> None:
        """Schedule a callback on the asyncio loop from a paho thread.

        Snapshots ``self._loop`` and skips the call when the loop is missing or
        already closed. The ``RuntimeError`` catch handles the race where the
        loop is closed between the check and the call during shutdown.
        """
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(callback, *args)
        except RuntimeError:  # pragma: no cover - shutdown race
            pass

    def _run_coro_on_loop(
        self, coro: Any, loop: Optional[asyncio.AbstractEventLoop]
    ) -> None:
        """Submit a coroutine to the loop from a paho thread, guarding shutdown.

        Closes the coroutine (avoiding a ``RuntimeWarning``) when the loop is
        gone or closed instead of letting ``run_coroutine_threadsafe`` raise.
        """
        if loop is None or loop.is_closed():
            coro.close()
            return
        try:
            asyncio.run_coroutine_threadsafe(coro, loop)
        except RuntimeError:  # pragma: no cover - shutdown race
            coro.close()

    # ------------------------------------------------------------------
    # Internal callbacks bridging the threaded paho callbacks into asyncio
    # ------------------------------------------------------------------
    def _on_connect(
        self,
        client: mqtt.Client,
        userdata,
        flags,
        rc: int,
        properties=None,
    ) -> None:
        reason = _normalise_reason_code(rc)
        self._last_connect_rc = reason
        if reason == 0:
            session_present = self._extract_session_present(flags)
            LOGGER.info(
                "Connected to MQTT broker (session_present=%s)", session_present
            )
            self._connected = True
            if self._connected_event:
                self._connected_event.set()
            self._call_soon_on_loop(self._replay_subscriptions)
            self._call_soon_on_loop(self._replay_offline_buffer)
            for handler in self._connect_handlers:
                self._call_soon_on_loop(handler, reason)
        else:
            LOGGER.error("MQTT connection failed with rc=%s", reason)
            self._connected = False
            if self._connected_event:
                self._connected_event.set()

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata,
        disconnect_flags: Any,
        rc: Any,
        properties: Any = None,
    ) -> None:
        _ = disconnect_flags  # API compatibility placeholder
        reason = _normalise_reason_code(rc)
        LOGGER.info("Disconnected from MQTT broker (rc=%s)", reason)
        if self._disconnect_event:
            self._disconnect_event.set()
        self._connected = False
        for handler in self._disconnect_handlers:
            self._call_soon_on_loop(handler, reason)

    def _on_message(
        self, client: mqtt.Client, userdata, message: mqtt.MQTTMessage
    ) -> None:
        handler = self._message_handler
        loop = self._loop
        if not handler or not loop:
            return

        try:
            result = handler(message.topic, message.payload)
            if asyncio.iscoroutine(result):
                self._run_coro_on_loop(result, loop)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("MQTT message handler raised an exception")


def _normalise_reason_code(code: Any) -> int:
    if code is None:
        return 0

    if isinstance(code, int):
        return code

    try:
        return int(code)
    except (TypeError, ValueError):
        pass

    value = getattr(code, "value", None)
    if value is None:
        return 0

    if isinstance(value, int):
        return value

    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
