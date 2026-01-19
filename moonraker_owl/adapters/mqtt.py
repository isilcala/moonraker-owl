"""MQTT adapter encapsulating paho-mqtt client usage."""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, List, Optional

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from ..config import CloudConfig

if TYPE_CHECKING:
    from ..token_manager import TokenManager

LOGGER = logging.getLogger(__name__)

MessageHandler = Callable[[str, bytes], Awaitable[None] | None]


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

    async def connect(
        self,
        timeout: float = 30.0,
        *,
        clean_start: bool = True,
        session_expiry: Optional[int] = None,
    ) -> None:
        """Connect to the MQTT broker and wait for acknowledgement.
        
        If JWT authentication fails (CONNACK code 5) and TokenManager is available,
        automatically refreshes the token and retries connection (max 2 attempts).
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
                # Check if this is an authentication failure (CONNACK code 5)
                if self._last_connect_rc == 5 and self.token_manager and attempt < max_attempts:
                    LOGGER.warning(
                        "MQTT authentication failed (CONNACK 5), refreshing JWT token and retrying (attempt %d/%d)",
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
    ) -> None:
        if not self._client:
            raise RuntimeError("MQTT client not connected")

        info = self._client.publish(
            topic, payload, qos=qos, retain=retain, properties=properties
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise MQTTConnectionError(f"Publish failed with rc={info.rc}")

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
        result, _ = self._client.subscribe(topic, qos=qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise MQTTConnectionError(f"Subscribe failed with rc={result}")

    def set_message_handler(self, handler: Optional[MessageHandler]) -> None:
        self._message_handler = handler

    def unsubscribe(self, topic: str) -> None:
        if not self._client:
            raise RuntimeError("MQTT client not connected")

        result, _ = self._client.unsubscribe(topic)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise MQTTConnectionError(f"Unsubscribe failed with rc={result}")

    def register_disconnect_handler(self, handler: Callable[[int], None]) -> None:
        self._disconnect_handlers.append(handler)

    def register_connect_handler(self, handler: Callable[[int], None]) -> None:
        self._connect_handlers.append(handler)

    def is_connected(self) -> bool:
        return self._connected

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
            LOGGER.info("Connected to MQTT broker")
            self._connected = True
            if self._connected_event:
                self._connected_event.set()
            if self._loop:
                for handler in self._connect_handlers:
                    self._loop.call_soon_threadsafe(handler, reason)
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
        if self._loop:
            for handler in self._disconnect_handlers:
                self._loop.call_soon_threadsafe(handler, reason)

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
                asyncio.run_coroutine_threadsafe(result, loop)
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
