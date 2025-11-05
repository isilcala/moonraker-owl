"""Main application entry-point for moonraker-owl."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import random
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


class AgentState(str, Enum):
    COLD_START = "cold_start"
    AWAITING_MQTT = "awaiting_mqtt"
    AWAITING_MOONRAKER = "awaiting_moonraker"
    ACTIVE = "active"
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    STOPPING = "stopping"


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
        self._state = AgentState.COLD_START
        self._state_detail: Optional[str] = None
        self._device_id: Optional[str] = None
        self._agent_topic: Optional[str] = None
        self._base_topic: Optional[str] = None
        self._mqtt_ready = False
        self._telemetry_ready = False
        self._commands_ready = False
        self._moonraker_failures = 0
        self._moonraker_monitor_task: Optional[asyncio.Task[None]] = None
        self._moonraker_breaker_tripped = False

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

    async def _transition_state(
        self, state: AgentState, *, detail: Optional[str] = None
    ) -> None:
        if state == self._state and detail == self._state_detail:
            return

        previous = self._state
        self._state = state
        self._state_detail = detail

        message_detail = detail or state.value
        LOGGER.info("Agent state transition %s -> %s (%s)", previous.value, state.value, message_detail)
        await self._health.set_agent_state(
            state.value,
            healthy=state == AgentState.ACTIVE,
            detail=message_detail,
        )

    def _schedule_state_transition(
        self, state: AgentState, *, detail: Optional[str] = None
    ) -> None:
        loop = self._loop
        if loop is None:
            return

        async def _runner() -> None:
            await self._transition_state(state, detail=detail)

        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if current_loop is loop:
            asyncio.create_task(_runner())
        else:
            asyncio.run_coroutine_threadsafe(_runner(), loop)

    def _build_presence_payload(
        self, state: str, *, detail: Optional[str] = None
    ) -> bytes:
        document = {
            "state": state,
            "updatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
        if self._device_id:
            document["deviceId"] = self._device_id
        if detail:
            document["detail"] = detail
        return json.dumps(document).encode("utf-8")

    async def _publish_presence(
        self,
        state: str,
        *,
        retain: bool = True,
        detail: Optional[str] = None,
    ) -> None:
        if self._mqtt_client is None or self._agent_topic is None:
            return

        payload = self._build_presence_payload(state, detail=detail)
        try:
            self._mqtt_client.publish(
                self._agent_topic,
                payload,
                qos=1,
                retain=retain,
            )
        except MQTTConnectionError as exc:
            LOGGER.debug("Failed to publish agent presence: %s", exc)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.debug("Unexpected error publishing agent presence", exc_info=True)

    def _queue_presence_publish(
        self,
        state: str,
        *,
        retain: bool = True,
        detail: Optional[str] = None,
    ) -> None:
        loop = self._loop
        if loop is None:
            return

        async def _runner() -> None:
            await self._publish_presence(state, retain=retain, detail=detail)

        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if current_loop is loop:
            asyncio.create_task(_runner())
        else:
            asyncio.run_coroutine_threadsafe(_runner(), loop)

    def _schedule_health_update(
        self, name: str, healthy: bool, detail: Optional[str]
    ) -> None:
        loop = self._loop
        if loop is None:
            return

        async def _runner() -> None:
            await self._health.update(name, healthy, detail)

        loop.call_soon_threadsafe(lambda: asyncio.create_task(_runner()))

    def _command_health_detail(self, baseline: Optional[str] = None) -> Optional[str]:
        processor = self._command_processor
        if processor is None:
            return baseline

        pending = getattr(processor, "pending_count", 0)
        detail_parts: list[str] = []
        if baseline:
            detail_parts.append(baseline)
        if pending > 0:
            detail_parts.append(f"pending_commands={pending}")
        if not detail_parts:
            return None
        return " ".join(detail_parts)

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
        await self._transition_state(AgentState.COLD_START, detail="initialising")

        device_id = _resolve_device_id(self._config)
        client_id = _build_client_id(device_id)
        self._device_id = device_id
        self._base_topic = f"owl/printers/{device_id}" if device_id else None
        self._agent_topic = (
            f"{self._base_topic}/agent/state" if self._base_topic is not None else None
        )

        self._connection_lost_event = asyncio.Event()
        self._supervisor_stop = asyncio.Event()
        self._stopping = False
        self._last_disconnect_rc = None
        self._mqtt_ready = False
        self._telemetry_ready = False
        self._commands_ready = False

        await self._health.update("mqtt", False, "initialising")
        await self._health.update("moonraker", False, "awaiting mqtt connectivity")
        await self._health.update("telemetry", False, "awaiting mqtt connectivity")
        await self._health.update("commands", False, "awaiting mqtt connectivity")

        self._moonraker_client = MoonrakerClient(self._config.moonraker)
        self._mqtt_client = MQTTClient(self._config.cloud, client_id=client_id)
        self._mqtt_client.register_disconnect_handler(self._on_mqtt_disconnect)
        self._mqtt_client.register_connect_handler(self._on_mqtt_connect)

        if self._agent_topic is not None:
            offline_payload = self._build_presence_payload(
                "offline", detail="unexpected disconnect"
            )
            self._mqtt_client.set_last_will(
                self._agent_topic,
                offline_payload,
                qos=1,
                retain=True,
            )

        await self._transition_state(
            AgentState.AWAITING_MQTT,
            detail="connecting to mqtt broker",
        )

        mqtt_connected = await self._connect_mqtt()
        if not mqtt_connected:
            await self._transition_state(
                AgentState.DEGRADED, detail="mqtt unavailable"
            )
            if self._moonraker_client is not None:
                await self._moonraker_client.stop()
                self._moonraker_client = None
            return False

        await self._transition_state(
            AgentState.AWAITING_MOONRAKER,
            detail="mqtt connected; starting runtime",
        )

        runtime_ready = await self._start_runtime_components()

        await self._start_health_server()
        self._start_moonraker_monitor()

        if self._connection_lost_event is None:
            self._connection_lost_event = asyncio.Event()
        if self._supervisor_stop is None:
            self._supervisor_stop = asyncio.Event()

        self._supervisor_task = asyncio.create_task(self._supervise_connection())

        if runtime_ready:
            await self._transition_state(AgentState.ACTIVE, detail="runtime ready")
            self._queue_presence_publish("online", retain=True, detail="agent active")
        else:
            await self._transition_state(
                AgentState.DEGRADED, detail="runtime initialisation incomplete"
            )

        return runtime_ready

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

    def _start_moonraker_monitor(self) -> None:
        if self._moonraker_monitor_task is not None and not self._moonraker_monitor_task.done():
            return
        if self._moonraker_client is None:
            return
        self._moonraker_monitor_task = asyncio.create_task(self._monitor_moonraker())

    async def _stop_moonraker_monitor(self) -> None:
        if self._moonraker_monitor_task is None:
            return
        self._moonraker_monitor_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._moonraker_monitor_task
        self._moonraker_monitor_task = None

    async def _register_moonraker_failure(self, reason: str) -> None:
        detail = reason or "moonraker failure"
        self._moonraker_failures += 1
        await self._health.update("moonraker", False, detail)

        threshold = max(1, self._config.resilience.moonraker_breaker_threshold)
        if self._moonraker_failures < threshold or self._moonraker_breaker_tripped:
            return

        LOGGER.warning(
            "Moonraker breaker tripped after %d failures: %s",
            self._moonraker_failures,
            detail,
        )
        self._moonraker_breaker_tripped = True
        await self._transition_state(
            AgentState.DEGRADED,
            detail="moonraker unavailable",
        )
        await self._deactivate_components("moonraker unavailable")

    async def _register_moonraker_recovery(self) -> None:
        self._moonraker_failures = 0
        await self._health.update("moonraker", True, None)

        if not self._moonraker_breaker_tripped and self._telemetry_ready:
            return

        try:
            runtime_ready = await self._restart_components()
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to restart runtime after Moonraker recovery")
            await self._transition_state(
                AgentState.DEGRADED,
                detail="moonraker recovered but restart failed",
            )
            return

        if runtime_ready:
            self._moonraker_breaker_tripped = False
            await self._transition_state(
                AgentState.ACTIVE,
                detail="moonraker recovered",
            )
            self._queue_presence_publish(
                "online", retain=True, detail="agent active"
            )
        else:
            await self._transition_state(
                AgentState.DEGRADED,
                detail="moonraker recovered; runtime still initialising",
            )

    async def _monitor_moonraker(self) -> None:
        resilience = self._config.resilience
        interval = max(5, resilience.heartbeat_interval_seconds)

        while not self._stopping:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

            if self._stopping:
                break

            if not self._mqtt_ready or self._moonraker_client is None:
                self._moonraker_failures = 0
                continue

            try:
                await self._moonraker_client.fetch_printer_state({})
            except Exception as exc:  # pragma: no cover - defensive logging
                await self._register_moonraker_failure(str(exc))
                continue

            await self._register_moonraker_recovery()

    async def _connect_mqtt(self) -> bool:
        if self._mqtt_client is None:
            return False

        resilience = self._config.resilience
        session_expiry = resilience.session_expiry_seconds
        if session_expiry <= 0:
            session_expiry = None

        try:
            await self._mqtt_client.connect(
                clean_start=False,
                session_expiry=session_expiry,
            )
        except MQTTConnectionError as exc:
            await self._health.update("mqtt", False, str(exc))
            LOGGER.error("MQTT connection failed: %s", exc)
            if self._mqtt_client is not None:
                await self._mqtt_client.disconnect()
            self._mqtt_client = None
            return False

        self._mqtt_ready = True
        await self._health.update("mqtt", True, None)
        return True

    async def _start_runtime_components(self) -> bool:
        if self._moonraker_client is None or self._mqtt_client is None:
            return False

        telemetry_ready = False
        moonraker_ready = False
        self._telemetry_ready = False
        self._commands_ready = False

        try:
            await self._moonraker_client.fetch_printer_state({})
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning("Moonraker readiness check failed: %s", exc)
            await self._health.update("moonraker", False, str(exc))
        else:
            moonraker_ready = True
            await self._health.update("moonraker", True, None)

        telemetry = self._telemetry_publisher
        if telemetry is None:
            telemetry = TelemetryPublisher(
                self._config, self._moonraker_client, self._mqtt_client
            )
            self._telemetry_publisher = telemetry

        try:
            await telemetry.start()
        except TelemetryConfigurationError as exc:
            await self._health.update("telemetry", False, str(exc))
            LOGGER.error("Telemetry disabled: %s", exc)
            self._telemetry_publisher = None
            return False
        except Exception:
            await self._health.update("telemetry", False, "initialisation failure")
            LOGGER.exception("Failed to start telemetry publisher")
            self._telemetry_publisher = None
            raise
        else:
            telemetry_ready = True
            self._telemetry_ready = True
            LOGGER.info(
                "Telemetry publisher started on topic %s",
                telemetry.topic,
            )
            await self._health.update("telemetry", True, None)

        processor = self._command_processor
        if processor is None and self._telemetry_publisher is not None:
            processor = CommandProcessor(
                self._config,
                self._moonraker_client,
                self._mqtt_client,
                telemetry=self._telemetry_publisher,
            )
            self._command_processor = processor

        if processor is not None:
            try:
                await processor.start()
            except CommandConfigurationError as exc:
                LOGGER.warning("Command processor disabled: %s", exc)
                await self._health.update(
                    "commands",
                    False,
                    self._command_health_detail(str(exc)),
                )
                self._command_processor = None
                processor = None
            except Exception as exc:
                LOGGER.exception("Failed to start command processor")
                await self._health.update(
                    "commands",
                    False,
                    self._command_health_detail(str(exc)),
                )
                self._command_processor = None
                processor = None
            else:
                self._commands_ready = True
                await self._health.update(
                    "commands",
                    True,
                    self._command_health_detail(),
                )
        else:
            await self._health.update(
                "commands",
                False,
                self._command_health_detail("telemetry unavailable"),
            )

        if telemetry_ready and moonraker_ready:
            self._moonraker_breaker_tripped = False

        return telemetry_ready and moonraker_ready

    async def _stop_health_server(self) -> None:
        if self._health_server is None:
            return
        await self._health_server.stop()
        self._health_server = None
        await self._health.update("health-endpoint", False, "shutdown")

    async def _deactivate_components(
        self, reason: str, *, preserve_instances: bool = True
    ) -> None:
        if self._command_processor is not None:
            try:
                await self._command_processor.stop()
            except Exception:  # pragma: no cover - defensive cleanup
                LOGGER.debug("Error stopping command processor", exc_info=True)
            try:
                await self._command_processor.abandon_inflight(reason)
            except AttributeError:
                LOGGER.debug("Command processor missing abandon_inflight handler")
            except Exception:  # pragma: no cover - defensive cleanup
                LOGGER.debug("Failed to abandon inflight commands", exc_info=True)
            await self._health.update(
                "commands",
                False,
                self._command_health_detail(reason),
            )
            if not preserve_instances:
                self._command_processor = None
        self._commands_ready = False

        if self._telemetry_publisher is not None:
            try:
                await self._telemetry_publisher.stop()
            except Exception:  # pragma: no cover - defensive cleanup
                LOGGER.debug("Error stopping telemetry publisher", exc_info=True)
            await self._health.update("telemetry", False, reason)
            if not preserve_instances:
                self._telemetry_publisher = None
        self._telemetry_ready = False

        await self._health.update("moonraker", False, reason)

    async def _restart_components(self) -> bool:
        return await self._start_runtime_components()

    async def _supervise_connection(self) -> None:
        if self._connection_lost_event is None or self._supervisor_stop is None:
            return
        if self._mqtt_client is None:
            return

        resilience = self._config.resilience
        backoff_initial = max(0.5, resilience.reconnect_initial_seconds)
        backoff_max = max(backoff_initial, resilience.reconnect_max_seconds)
        jitter_ratio = max(0.0, min(1.0, resilience.reconnect_jitter_ratio))
        delay = backoff_initial

        while not self._supervisor_stop.is_set():
            await self._connection_lost_event.wait()
            self._connection_lost_event.clear()

            if self._supervisor_stop.is_set() or self._mqtt_client is None:
                break

            await self._transition_state(
                AgentState.RECOVERING,
                detail=f"mqtt disconnected (rc={self._last_disconnect_rc})",
            )

            await self._health.update(
                "mqtt",
                False,
                f"disconnected (rc={self._last_disconnect_rc})",
            )
            await self._deactivate_components("waiting for mqtt reconnect")
            self._mqtt_ready = False

            while not self._supervisor_stop.is_set():
                try:
                    await self._mqtt_client.reconnect()
                    self._mqtt_ready = True
                    await self._health.update("mqtt", True, None)
                    delay = backoff_initial
                    break
                except MQTTConnectionError as exc:
                    LOGGER.warning("MQTT reconnect failed: %s", exc)
                    await self._health.update("mqtt", False, str(exc))
                    sleep_for = delay
                    if jitter_ratio > 0.0:
                        jitter = delay * jitter_ratio
                        lower = max(0.1, delay - jitter)
                        upper = delay + jitter
                        sleep_for = random.uniform(lower, upper)
                    await asyncio.sleep(sleep_for)
                    delay = min(delay * 2, backoff_max)
            else:
                break

            if self._supervisor_stop.is_set():
                break

            try:
                runtime_ready = await self._restart_components()
            except Exception as exc:  # pragma: no cover - defensive restart handling
                LOGGER.exception(
                    "Failed to restart components after reconnect: %s", exc
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, backoff_max)
                continue
            else:
                if runtime_ready:
                    await self._transition_state(
                        AgentState.ACTIVE,
                        detail="runtime recovered",
                    )
                    self._queue_presence_publish(
                        "online", retain=True, detail="agent active"
                    )
                else:
                    await self._transition_state(
                        AgentState.DEGRADED,
                        detail="runtime restart incomplete",
                    )

        LOGGER.debug("MQTT supervisor loop terminated")

    def _on_mqtt_disconnect(self, rc: int) -> None:
        self._last_disconnect_rc = rc
        self._mqtt_ready = False
        if self._stopping:
            self._schedule_health_update(
                "mqtt", False, f"disconnected (rc={rc})"
            )
            return
        if self._connection_lost_event is not None:
            self._connection_lost_event.set()
        self._schedule_health_update(
            "mqtt", False, f"disconnected (rc={rc})"
        )
        self._schedule_state_transition(
            AgentState.RECOVERING,
            detail=f"mqtt disconnected (rc={rc})",
        )

    def _on_mqtt_connect(self, rc: int) -> None:
        if self._stopping:
            return
        self._mqtt_ready = True
        self._schedule_health_update("mqtt", True, None)

    async def _stop_services(self) -> None:
        await self._transition_state(AgentState.STOPPING, detail="shutdown requested")
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

        await self._stop_moonraker_monitor()
        await self._deactivate_components("shutdown", preserve_instances=False)
        await self._stop_health_server()

        if self._moonraker_client is not None:
            await self._moonraker_client.stop()
            self._moonraker_client = None

        if self._mqtt_client is not None:
            await self._publish_presence(
                "offline", retain=True, detail="shutdown"
            )
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
