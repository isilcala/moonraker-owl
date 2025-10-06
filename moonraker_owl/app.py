"""Main application entry-point for moonraker-owl."""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Optional

from . import constants
from .adapters import MoonrakerClient, MQTTClient, MQTTConnectionError
from .commands import CommandConfigurationError, CommandProcessor
from .config import OwlConfig, load_config
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

    async def run(self) -> None:
        """Run the main supervisor loop.

        Currently this is a placeholder that will be extended in subsequent phases.
        """

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
        LOGGER.info("moonraker-owl idle loop active; awaiting further implementation")
        while True:
            await asyncio.sleep(1)

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

        self._moonraker_client = MoonrakerClient(self._config.moonraker)
        self._mqtt_client = MQTTClient(self._config.cloud, client_id=client_id)

        try:
            await self._mqtt_client.connect()
        except MQTTConnectionError as exc:
            LOGGER.error("MQTT connection failed: %s", exc)
            self._mqtt_client = None
            self._moonraker_client = None
            # Moonraker client never started; nothing to stop
            return False

        try:
            self._telemetry_publisher = TelemetryPublisher(
                self._config, self._moonraker_client, self._mqtt_client
            )
            await self._telemetry_publisher.start()
        except TelemetryConfigurationError as exc:
            LOGGER.error("Telemetry disabled: %s", exc)
            if self._moonraker_client is not None:
                await self._moonraker_client.stop()
                self._moonraker_client = None
            await self._mqtt_client.disconnect()
            self._telemetry_publisher = None
            self._mqtt_client = None
            return False
        except Exception:
            LOGGER.exception("Failed to start telemetry publisher")
            if self._moonraker_client is not None:
                await self._moonraker_client.stop()
                self._moonraker_client = None
            await self._mqtt_client.disconnect()
            self._telemetry_publisher = None
            self._mqtt_client = None
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
            )
            await self._command_processor.start()
        except CommandConfigurationError as exc:
            LOGGER.warning("Command processor disabled: %s", exc)
            self._command_processor = None
        except Exception:
            LOGGER.exception("Failed to start command processor")
            self._command_processor = None

        return True

    async def _stop_services(self) -> None:
        if self._command_processor is not None:
            await self._command_processor.stop()
            self._command_processor = None

        if self._telemetry_publisher is not None:
            await self._telemetry_publisher.stop()
            self._telemetry_publisher = None

        if self._moonraker_client is not None:
            await self._moonraker_client.stop()
            self._moonraker_client = None

        if self._mqtt_client is not None:
            await self._mqtt_client.disconnect()
            self._mqtt_client = None


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
