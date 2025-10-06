"""MQTT adapter encapsulating paho-mqtt client usage."""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional

import paho.mqtt.client as mqtt

from ..config import CloudConfig

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
    ) -> None:
        self.config = config
        self.client_id = client_id
        self.keepalive = keepalive

        self._client: Optional[mqtt.Client] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._connected_event: Optional[asyncio.Event] = None
        self._disconnect_event: Optional[asyncio.Event] = None
        self._message_handler: Optional[MessageHandler] = None
        self._last_connect_rc: Optional[int] = None

    async def connect(self, timeout: float = 30.0) -> None:
        """Connect to the MQTT broker and wait for acknowledgement."""

        self._loop = asyncio.get_running_loop()
        self._connected_event = asyncio.Event()
        self._disconnect_event = asyncio.Event()
        self._last_connect_rc = None

        client = mqtt.Client(client_id=self.client_id)
        client.enable_logger(LOGGER)

        if self.config.username:
            client.username_pw_set(self.config.username, self.config.password)

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message

        self._client = client

        LOGGER.info(
            "Connecting to MQTT broker %s:%s",
            self.config.broker_host,
            self.config.broker_port,
        )

        client.connect_async(
            self.config.broker_host, self.config.broker_port, self.keepalive
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

    def publish(
        self, topic: str, payload: bytes, qos: int = 1, retain: bool = False
    ) -> None:
        if not self._client:
            raise RuntimeError("MQTT client not connected")

        info = self._client.publish(topic, payload, qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise MQTTConnectionError(f"Publish failed with rc={info.rc}")

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

    # ------------------------------------------------------------------
    # Internal callbacks bridging the threaded paho callbacks into asyncio
    # ------------------------------------------------------------------
    def _on_connect(self, client: mqtt.Client, userdata, flags, rc: int) -> None:
        self._last_connect_rc = rc
        if rc == 0:
            LOGGER.info("Connected to MQTT broker")
            if self._connected_event:
                self._connected_event.set()
        else:
            LOGGER.error("MQTT connection failed with rc=%s", rc)
            if self._connected_event:
                self._connected_event.set()

    def _on_disconnect(self, client: mqtt.Client, userdata, rc: int) -> None:
        LOGGER.info("Disconnected from MQTT broker (rc=%s)", rc)
        if self._disconnect_event:
            self._disconnect_event.set()

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
