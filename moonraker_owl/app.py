"""Main application entry-point for moonraker-owl."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dataclasses import dataclass
from typing import Optional

from . import constants
from .adapters import MoonrakerClient, MQTTClient, MQTTConnectionError
from .commands import CommandConfigurationError, CommandProcessor
from .config import OwlConfig, load_config
from .health import HealthReporter, HealthServer
from .logging import configure_logging
from .telemetry import (
    TelemetryConfigurationError,
    TelemetryPublisher,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class AppContext:
    config: OwlConfig


class MoonrakerOwlApp:
    """Coordinates application startup and shutdown."""

    def __init__(self, config: Optional[OwlConfig] = None) -> None:
        self._config = config or load_config()
        self._context = AppContext(config=self._config)
        self._moonraker_client: Optional[MoonrakerClient] = None
        self._mqtt_client: Optional[MQTTClient] = None
        self._telemetry_publisher: Optional[TelemetryPublisher] = None
        self._command_processor: Optional[CommandProcessor] = None
        self._supervisor_task: Optional[asyncio.Task[None]] = None
        self._connection_lost_event: Optional[asyncio.Event] = None
        self._supervisor_stop: Optional[asyncio.Event] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stopping = False
        self._last_disconnect_rc: Optional[int] = None
        self._health = HealthReporter()
        self._health_server: Optional[HealthServer] = None

    async def run(self) -> None:
        """Run the main supervisor loop.

        Currently this is a placeholder that will be extended in subsequent phases.
        """

        self._loop = asyncio.get_running_loop()
        self._shutdown_event = asyncio.Event()

        LOGGER.info("moonraker-owl starting with config: %s", self._config.path)
        started = await self._start_services()
        if not started:
            LOGGER.warning("Service startup incomplete; running in degraded mode")

        try:
            await self._idle_loop()
        except asyncio.CancelledError:
            LOGGER.info("moonraker-owl received shutdown signal")
            raise
        finally:
            await self._stop_services()

    async def _idle_loop(self) -> None:
        LOGGER.info("moonraker-owl supervisor active; awaiting shutdown signal")
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()
        await self._shutdown_event.wait()

    @classmethod
    def start(cls, config: Optional[OwlConfig] = None) -> None:
        instance = cls(config=config)
        configure_logging(
            instance._config.logging.level,
            log_path=instance._config.logging.path,
            log_network=instance._config.logging.log_network,
        )
        try:
            asyncio.run(instance.run())
        except KeyboardInterrupt:
            LOGGER.info("moonraker-owl received shutdown signal")

    async def _start_services(self) -> bool:
        device_id = _resolve_device_id(self._config)
        client_id = _build_client_id(device_id)

        self._connection_lost_event = asyncio.Event()
        self._supervisor_stop = asyncio.Event()
        self._stopping = False
        self._last_disconnect_rc = None

        await self._health.update("mqtt", False, "initialising")
        await self._health.update("telemetry", False, "awaiting mqtt connectivity")
        await self._health.update("commands", False, "awaiting mqtt connectivity")

        self._moonraker_client = MoonrakerClient(self._config.moonraker)
        self._mqtt_client = MQTTClient(self._config.cloud, client_id=client_id)
        self._mqtt_client.register_disconnect_handler(self._on_mqtt_disconnect)
        self._mqtt_client.register_connect_handler(self._on_mqtt_connect)

        try:
            await self._mqtt_client.connect()
            await self._health.update("mqtt", True, None)
        except MQTTConnectionError as exc:
            await self._health.update("mqtt", False, str(exc))
            LOGGER.error("MQTT connection failed: %s", exc)
            self._mqtt_client = None
            if self._moonraker_client is not None:
                await self._moonraker_client.stop()
            self._moonraker_client = None
            return False

        try:
            self._telemetry_publisher = TelemetryPublisher(
                self._config, self._moonraker_client, self._mqtt_client
            )
            await self._telemetry_publisher.start()
            await self._health.update("telemetry", True, None)
        except TelemetryConfigurationError as exc:
            await self._health.update("telemetry", False, str(exc))
            LOGGER.error("Telemetry disabled: %s", exc)
            if self._moonraker_client is not None:
                await self._moonraker_client.stop()
                self._moonraker_client = None
            if self._mqtt_client is not None:
                await self._mqtt_client.disconnect()
                self._mqtt_client = None
            self._telemetry_publisher = None
            return False
        except Exception:
            await self._health.update("telemetry", False, "initialisation failure")
            LOGGER.exception("Failed to start telemetry publisher")
            if self._moonraker_client is not None:
                await self._moonraker_client.stop()
                self._moonraker_client = None
            if self._mqtt_client is not None:
                await self._mqtt_client.disconnect()
                self._mqtt_client = None
            self._telemetry_publisher = None
            raise

        LOGGER.info(
            "Telemetry publisher started on topic %s",
            self._telemetry_publisher.topic,
        )

        try:
            self._command_processor = CommandProcessor(
                self._config,
                self._moonraker_client,
                self._mqtt_client,
                telemetry=self._telemetry_publisher,
            )
            await self._command_processor.start()
            await self._health.update("commands", True, None)
        except CommandConfigurationError as exc:
            LOGGER.warning("Command processor disabled: %s", exc)
            await self._health.update("commands", False, str(exc))
            self._command_processor = None
        except Exception as exc:
            LOGGER.exception("Failed to start command processor")
            await self._health.update("commands", False, str(exc))
            self._command_processor = None

        await self._start_health_server()

        if self._connection_lost_event is None:
            self._connection_lost_event = asyncio.Event()
        if self._supervisor_stop is None:
            self._supervisor_stop = asyncio.Event()

        self._supervisor_task = asyncio.create_task(self._supervise_connection())

        return True

    async def _start_health_server(self) -> None:
        resilience = self._config.resilience
        if not resilience.health_enabled or resilience.health_port <= 0:
            return

        server = HealthServer(
            self._health,
            resilience.health_host,
            resilience.health_port,
        )
        try:
            await server.start()
        except OSError as exc:
            LOGGER.error("Failed to start health endpoint: %s", exc)
            await self._health.update("health-endpoint", False, str(exc))
        else:
            self._health_server = server
            await self._health.update("health-endpoint", True, None)

    async def _stop_health_server(self) -> None:
        if self._health_server is None:
            return
        await self._health_server.stop()
        self._health_server = None
        await self._health.update("health-endpoint", False, "shutdown")

    async def _deactivate_components(self, reason: str) -> None:
        if self._command_processor is not None:
            try:
                await self._command_processor.stop()
            except Exception:  # pragma: no cover - defensive cleanup
                LOGGER.debug("Error stopping command processor", exc_info=True)
            await self._health.update("commands", False, reason)

        if self._telemetry_publisher is not None:
            try:
                await self._telemetry_publisher.stop()
            except Exception:  # pragma: no cover - defensive cleanup
                LOGGER.debug("Error stopping telemetry publisher", exc_info=True)
            await self._health.update("telemetry", False, reason)

    async def _restart_components(self) -> None:
        if self._telemetry_publisher is not None:
            try:
                await self._telemetry_publisher.start()
            except RuntimeError:
                pass
            else:
                await self._health.update("telemetry", True, None)
                LOGGER.info(
                    "Telemetry publisher started on topic %s",
                    self._telemetry_publisher.topic,
                )

        if self._command_processor is not None:
            try:
                await self._command_processor.start()
                await self._health.update("commands", True, None)
            except RuntimeError:
                # Already active; ignore
                pass
            except Exception as exc:
                LOGGER.warning("Command processor restart failed: %s", exc)
                await self._health.update("commands", False, str(exc))
                self._command_processor = None

    async def _supervise_connection(self) -> None:
        if self._connection_lost_event is None or self._supervisor_stop is None:
            return
        if self._mqtt_client is None:
            return

        backoff_initial = max(0.5, self._config.resilience.reconnect_initial_seconds)
        backoff_max = max(
            backoff_initial, self._config.resilience.reconnect_max_seconds
        )
        delay = backoff_initial

        while not self._supervisor_stop.is_set():
            await self._connection_lost_event.wait()
            self._connection_lost_event.clear()

            if self._supervisor_stop.is_set() or self._mqtt_client is None:
                break

            await self._health.update(
                "mqtt",
                False,
                f"disconnected (rc={self._last_disconnect_rc})",
            )
            await self._deactivate_components("waiting for mqtt reconnect")

            while not self._supervisor_stop.is_set():
                try:
                    await self._mqtt_client.reconnect()
                    await self._health.update("mqtt", True, None)
                    delay = backoff_initial
                    break
                except MQTTConnectionError as exc:
                    LOGGER.warning("MQTT reconnect failed: %s", exc)
                    await self._health.update("mqtt", False, str(exc))
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, backoff_max)
            else:
                break

            if self._supervisor_stop.is_set():
                break

            try:
                await self._restart_components()
            except Exception as exc:  # pragma: no cover - defensive restart handling
                LOGGER.exception(
                    "Failed to restart components after reconnect: %s", exc
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, backoff_max)
                continue

        LOGGER.debug("MQTT supervisor loop terminated")

    def _on_mqtt_disconnect(self, rc: int) -> None:
        self._last_disconnect_rc = rc
        if self._stopping:
            return
        if self._connection_lost_event is not None:
            self._connection_lost_event.set()
        asyncio.create_task(
            self._health.update("mqtt", False, f"disconnected (rc={rc})")
        )

    def _on_mqtt_connect(self, rc: int) -> None:
        if self._stopping:
            return
        asyncio.create_task(self._health.update("mqtt", True, None))

    async def _stop_services(self) -> None:
        self._stopping = True

        if self._supervisor_stop is not None:
            self._supervisor_stop.set()
        if self._connection_lost_event is not None:
            self._connection_lost_event.set()

        if self._supervisor_task is not None:
            self._supervisor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._supervisor_task
            self._supervisor_task = None

        await self._deactivate_components("shutdown")
        await self._stop_health_server()

        if self._moonraker_client is not None:
            await self._moonraker_client.stop()
            self._moonraker_client = None

        if self._mqtt_client is not None:
            await self._mqtt_client.disconnect()
            self._mqtt_client = None
            await self._health.update("mqtt", False, "shutdown")

        if self._shutdown_event is not None:
            self._shutdown_event.set()


def _resolve_device_id(config: OwlConfig) -> Optional[str]:
    parser = config.raw
    device_id = parser.get("cloud", "device_id", fallback="")
    if device_id:
        return device_id

    username = config.cloud.username
    if username and ":" in username:
        return username.split(":", 1)[1]

    return None


def _build_client_id(device_id: Optional[str]) -> str:
    suffix = device_id or str(os.getpid())
    return f"{constants.APP_NAME}-{suffix}"
