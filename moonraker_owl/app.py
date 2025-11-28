"""Main application entry-point for moonraker-owl."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Optional

from . import constants
from .adapters import MQTTClient, MQTTConnectionError
from .backends import MoonrakerBackend
from .commands import CommandConfigurationError, CommandProcessor
from .config import OwlConfig, load_config
from .connection import ConnectionCoordinator, ReconnectReason
from .core import PrinterBackend, PrinterHealthAssessment
from .health import HealthReporter, HealthServer
from .logging import configure_logging
from .telemetry import (
    TelemetryConfigurationError,
    TelemetryPublisher,
)
from .token_manager import TokenManager

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
    """Coordinates application startup and shutdown.

    This class orchestrates the Owl agent lifecycle, managing:
    - MQTT connectivity and authentication
    - Printer backend health monitoring
    - Telemetry publishing and command processing
    - State machine transitions

    The printer backend can be injected for testing or to support
    different printer control systems (Moonraker, OctoPrint, etc.).
    """

    def __init__(
        self,
        config: Optional[OwlConfig] = None,
        *,
        printer_backend: Optional[PrinterBackend] = None,
    ) -> None:
        """Initialize the Owl agent.

        Args:
            config: Application configuration. If None, loads from default path.
            printer_backend: Printer backend implementation. If None, creates
                           MoonrakerBackend from config.
        """
        self._config = config or load_config()
        self._context = AppContext(config=self._config)
        # Create default backend if not provided
        self._printer_backend: Optional[PrinterBackend] = (
            printer_backend or MoonrakerBackend(self._config.moonraker)
        )
        self._mqtt_client: Optional[MQTTClient] = None
        self._token_manager: Optional[TokenManager] = None
        self._connection_coordinator: Optional[ConnectionCoordinator] = None
        self._telemetry_publisher: Optional[TelemetryPublisher] = None
        self._command_processor: Optional[CommandProcessor] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stopping = False
        self._last_disconnect_rc: Optional[int] = None
        self._health = HealthReporter()
        self._health_server: Optional[HealthServer] = None
        self._state = AgentState.COLD_START
        self._state_detail: Optional[str] = None
        self._device_id: Optional[str] = None
        self._base_topic: Optional[str] = None
        self._mqtt_ready = False
        self._telemetry_ready = False
        self._commands_ready = False
        self._moonraker_failures = 0
        self._moonraker_monitor_task: Optional[asyncio.Task[None]] = None
        self._moonraker_breaker_tripped = False
        self._telemetry_status_listener: Callable[[dict[str, Any]], Awaitable[None]] = (
            self._handle_telemetry_status_update
        )
        self._status_listener_registered = False
        self._moonraker_recovery_lock: Optional[asyncio.Lock] = None

    @property
    def _moonraker_client(self) -> Any:
        """Backward-compatible access to underlying Moonraker client.

        This property provides access to the MoonrakerClient for components
        that haven't been migrated to use the PrinterBackend abstraction.

        Deprecated: Use self._printer_backend methods instead.
        """
        backend = self._printer_backend
        if backend is None:
            return None
        # MoonrakerBackend exposes .client property
        return getattr(backend, "client", None)

    async def run(self) -> None:
        """Run the main supervisor loop.

        Currently this is a placeholder that will be extended in subsequent phases.
        """

        self._loop = asyncio.get_running_loop()
        self._shutdown_event = asyncio.Event()
        self._moonraker_recovery_lock = asyncio.Lock()

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
        LOGGER.info(
            "Agent state transition %s -> %s (%s)",
            previous.value,
            state.value,
            message_detail,
        )
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

        self._stopping = False
        self._last_disconnect_rc = None
        self._mqtt_ready = False
        self._telemetry_ready = False
        self._commands_ready = False

        await self._health.update("mqtt", False, "initialising")
        await self._health.update("moonraker", False, "awaiting mqtt connectivity")
        await self._health.update("telemetry", False, "awaiting mqtt connectivity")
        await self._health.update("commands", False, "awaiting mqtt connectivity")

        # Initialize TokenManager (JWT authentication - required)
        if not self._config.cloud.device_private_key:
            LOGGER.error(
                "Device private key not found. JWT authentication is required. "
                "Please link your device: moonraker-owl link"
            )
            return False

        if not device_id:
            LOGGER.error("Device ID not found in configuration")
            return False

        try:
            LOGGER.info("Initializing TokenManager for JWT authentication")
            self._token_manager = TokenManager(
                device_id=device_id,
                private_key_b64=self._config.cloud.device_private_key,
                base_url=self._config.cloud.base_url,
            )
            await self._token_manager.start()
            LOGGER.info("TokenManager initialized successfully")
        except Exception as exc:
            LOGGER.error("Failed to initialize TokenManager: %s", exc, exc_info=True)
            # Clean up resources on failure
            if self._token_manager:
                await self._token_manager.stop()
                self._token_manager = None
            return False

        self._mqtt_client = MQTTClient(
            self._config.cloud, client_id=client_id, token_manager=self._token_manager
        )
        self._mqtt_client.register_disconnect_handler(self._on_mqtt_disconnect)
        self._mqtt_client.register_connect_handler(self._on_mqtt_connect)

        # Create ConnectionCoordinator with session expiry from config
        resilience = self._config.resilience
        session_expiry = resilience.session_expiry_seconds
        if session_expiry <= 0:
            session_expiry = None

        self._connection_coordinator = ConnectionCoordinator(
            mqtt_client=self._mqtt_client,
            token_manager=self._token_manager,
            resilience_config=resilience,
            session_expiry=session_expiry,
        )

        # Register callbacks with coordinator
        self._connection_coordinator.register_disconnected_callback(
            self._on_connection_lost
        )
        self._connection_coordinator.register_reconnected_callback(
            self._on_connection_restored
        )

        await self._transition_state(
            AgentState.AWAITING_MQTT,
            detail="connecting to mqtt broker",
        )

        mqtt_connected = await self._connect_mqtt()
        if not mqtt_connected:
            await self._transition_state(AgentState.DEGRADED, detail="mqtt unavailable")
            if self._printer_backend is not None:
                await self._printer_backend.stop()
                self._printer_backend = None
            return False

        await self._transition_state(
            AgentState.AWAITING_MOONRAKER,
            detail="mqtt connected; starting runtime",
        )

        runtime_ready = await self._start_runtime_components()

        await self._start_health_server()
        self._start_moonraker_monitor()

        # Start connection supervisor via ConnectionCoordinator
        self._connection_coordinator.start_supervisor()

        if runtime_ready:
            await self._transition_state(AgentState.ACTIVE, detail="runtime ready")
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
        if (
            self._moonraker_monitor_task is not None
            and not self._moonraker_monitor_task.done()
        ):
            return
        if self._printer_backend is None:
            return
        self._moonraker_monitor_task = asyncio.create_task(self._monitor_moonraker())

    async def _stop_moonraker_monitor(self) -> None:
        if self._moonraker_monitor_task is None:
            return
        self._moonraker_monitor_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._moonraker_monitor_task
        self._moonraker_monitor_task = None

    async def _register_moonraker_failure(
        self,
        reason: str,
        *,
        force_trip: bool = False,
        source: str = "monitor",
    ) -> None:
        detail = reason or "moonraker failure"
        self._moonraker_failures += 1
        await self._health.update("moonraker", False, detail)

        threshold = max(1, self._config.resilience.moonraker_breaker_threshold)
        if force_trip and self._moonraker_failures < threshold:
            self._moonraker_failures = threshold
        if self._moonraker_failures < threshold or self._moonraker_breaker_tripped:
            return

        LOGGER.warning(
            "Moonraker breaker tripped after %d failures%s [%s]: %s",
            self._moonraker_failures,
            " (forced)" if force_trip else "",
            source,
            detail,
        )
        self._moonraker_breaker_tripped = True
        await self._transition_state(
            AgentState.DEGRADED,
            detail="moonraker unavailable",
        )
        await self._publish_moonraker_degraded(detail)
        await self._deactivate_components(
            "moonraker unavailable",
            keep_telemetry=True,
        )

    async def _register_moonraker_recovery(self) -> None:
        self._moonraker_failures = 0
        await self._health.update("moonraker", True, None)

        lock = self._moonraker_recovery_lock
        if lock is None:
            lock = asyncio.Lock()
            self._moonraker_recovery_lock = lock

        async with lock:
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
            else:
                await self._transition_state(
                    AgentState.DEGRADED,
                    detail="moonraker recovered; runtime still initialising",
                )

    async def _publish_moonraker_degraded(self, reason: str) -> None:
        if self._telemetry_publisher is None:
            return

        detail = reason or "Moonraker unavailable"
        try:
            await self._telemetry_publisher.publish_system_status(
                printer_state="error",
                message=detail,
            )
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.debug("Failed to publish degraded telemetry snapshot", exc_info=True)

    async def _handle_telemetry_status_update(self, payload: dict[str, Any]) -> None:
        assessment = self._analyse_moonraker_snapshot(payload)
        if not assessment.healthy:
            await self._register_moonraker_failure(
                assessment.detail or "moonraker telemetry failure",
                force_trip=assessment.force_trip,
                source="status-listener",
            )
            return

        if self._moonraker_failures == 0 and not self._moonraker_breaker_tripped:
            return

        await self._register_moonraker_recovery()

    async def _monitor_moonraker(self) -> None:
        """Monitor printer health via PrinterBackend.assess_health()."""
        resilience = self._config.resilience
        interval = max(5, resilience.heartbeat_interval_seconds)

        while not self._stopping:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

            if self._stopping:
                break

            if not self._mqtt_ready or self._printer_backend is None:
                self._moonraker_failures = 0
                continue

            assessment = await self._printer_backend.assess_health()
            if not assessment.healthy:
                await self._register_moonraker_failure(
                    assessment.detail or "printer reported error",
                    force_trip=assessment.force_trip,
                )
                continue

            await self._register_moonraker_recovery()

    def _analyse_moonraker_snapshot(
        self, snapshot: dict[str, Any]
    ) -> PrinterHealthAssessment:
        """Analyse a snapshot using the backend's health analysis.

        This method delegates to MoonrakerBackend._analyse_snapshot() if available,
        otherwise returns a generic healthy assessment.
        """
        backend = self._printer_backend
        if backend is None:
            return PrinterHealthAssessment(healthy=True)

        # Try to use backend's analysis method if available
        analyse_method = getattr(backend, "_analyse_snapshot", None)
        if analyse_method is not None:
            return analyse_method(snapshot)

        # Fallback: assume healthy if no analysis available
        return PrinterHealthAssessment(healthy=True)

    async def _connect_mqtt(self) -> bool:
        """Establish initial MQTT connection via ConnectionCoordinator."""
        if self._connection_coordinator is None:
            return False

        try:
            await self._connection_coordinator.connect()
        except MQTTConnectionError as exc:
            await self._health.update("mqtt", False, str(exc))
            LOGGER.error("MQTT connection failed: %s", exc)
            return False

        self._mqtt_ready = True
        await self._health.update("mqtt", True, None)

        # Start JWT token renewal loop after MQTT connection succeeds
        if self._token_manager:
            LOGGER.info("Starting TokenManager renewal loop")
            self._token_manager.start_renewal_loop(on_renewed=self._on_token_renewed)

        return True

    async def _start_runtime_components(self) -> bool:
        if self._printer_backend is None or self._mqtt_client is None:
            return False

        telemetry_ready = False
        moonraker_ready = False
        self._telemetry_ready = False
        self._commands_ready = False

        try:
            await self._printer_backend.fetch_state({})
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.warning("Printer backend readiness check failed: %s", exc)
            await self._health.update("moonraker", False, str(exc))
        else:
            moonraker_ready = True
            await self._health.update("moonraker", True, None)

        telemetry = self._telemetry_publisher
        if telemetry is None:
            # Use backend factory if available, otherwise fallback to direct creation
            telemetry = self._printer_backend.create_telemetry_publisher(
                self._config, self._mqtt_client
            )
            self._telemetry_publisher = telemetry
            self._status_listener_registered = False

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
            if not self._status_listener_registered:
                try:
                    telemetry.register_status_listener(self._telemetry_status_listener)
                except AttributeError:  # pragma: no cover - defensive guard
                    LOGGER.debug(
                        "Telemetry publisher does not support status listeners"
                    )
                except Exception:  # pragma: no cover - defensive logging
                    LOGGER.debug(
                        "Failed to register telemetry status listener",
                        exc_info=True,
                    )
                else:
                    self._status_listener_registered = True

        processor = self._command_processor
        if processor is None and self._telemetry_publisher is not None:
            # Use backend factory for command processor
            processor = self._printer_backend.create_command_processor(
                self._config,
                self._mqtt_client,
                self._telemetry_publisher,
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
        self,
        reason: str,
        *,
        preserve_instances: bool = True,
        keep_telemetry: bool = False,
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
            if keep_telemetry:
                await self._health.update("telemetry", False, reason)
            else:
                if self._status_listener_registered:
                    try:
                        self._telemetry_publisher.unregister_status_listener(
                            self._telemetry_status_listener
                        )
                    except AttributeError:
                        LOGGER.debug(
                            "Telemetry publisher missing unregister_status_listener"
                        )
                    except Exception:  # pragma: no cover - defensive cleanup
                        LOGGER.debug(
                            "Failed to unregister telemetry status listener",
                            exc_info=True,
                        )
                    finally:
                        self._status_listener_registered = False
                try:
                    await self._telemetry_publisher.stop()
                except Exception:  # pragma: no cover - defensive cleanup
                    LOGGER.debug("Error stopping telemetry publisher", exc_info=True)
                await self._health.update("telemetry", False, reason)
                if not preserve_instances:
                    self._telemetry_publisher = None
                    self._status_listener_registered = False
                self._telemetry_ready = False

        await self._health.update("moonraker", False, reason)

    async def _restart_components(self) -> bool:
        return await self._start_runtime_components()

    # -------------------------------------------------------------------------
    # Connection Coordinator Callbacks
    # -------------------------------------------------------------------------

    async def _on_connection_lost(self, reason: ReconnectReason) -> None:
        """Called by ConnectionCoordinator when connection is lost.

        This callback handles state transitions and component deactivation
        before reconnection is attempted.
        """
        LOGGER.info(
            "Connection lost (reason=%s), deactivating components", reason.value
        )

        await self._transition_state(
            AgentState.RECOVERING,
            detail=f"mqtt disconnected ({reason.value})",
        )

        await self._health.update(
            "mqtt",
            False,
            f"disconnected ({reason.value})",
        )

        await self._deactivate_components("waiting for mqtt reconnect")
        self._mqtt_ready = False

    async def _on_connection_restored(self) -> None:
        """Called by ConnectionCoordinator when connection is restored.

        This callback handles component reactivation and state transitions
        after successful reconnection.
        """
        LOGGER.info("Connection restored, restarting components")

        self._mqtt_ready = True
        await self._health.update("mqtt", True, None)

        try:
            runtime_ready = await self._restart_components()
        except Exception as exc:
            LOGGER.exception("Failed to restart components after reconnect: %s", exc)
            await self._transition_state(
                AgentState.DEGRADED,
                detail="runtime restart failed",
            )
            return

        if runtime_ready:
            await self._transition_state(
                AgentState.ACTIVE,
                detail="runtime recovered",
            )
        else:
            await self._transition_state(
                AgentState.DEGRADED,
                detail="runtime restart incomplete",
            )

    # -------------------------------------------------------------------------
    # MQTT Client Callbacks (from paho-mqtt thread)
    # -------------------------------------------------------------------------

    def _on_mqtt_disconnect(self, rc: int) -> None:
        """Handle MQTT disconnect event from paho-mqtt.

        This is called from the paho-mqtt thread when connection is lost.
        It schedules a reconnection request via the ConnectionCoordinator.
        """
        self._last_disconnect_rc = rc
        self._mqtt_ready = False

        if self._stopping:
            self._schedule_health_update("mqtt", False, f"disconnected (rc={rc})")
            return

        self._schedule_health_update("mqtt", False, f"disconnected (rc={rc})")

        # Request reconnection via coordinator
        if self._connection_coordinator is not None:
            # Determine reconnect reason based on disconnect code
            if rc == 5:  # CONNACK code 5 = Not authorized
                reason = ReconnectReason.AUTH_FAILURE
            else:
                reason = ReconnectReason.CONNECTION_LOST

            self._connection_coordinator.request_reconnect(reason)

    def _on_mqtt_connect(self, rc: int) -> None:
        if self._stopping:
            return
        self._mqtt_ready = True
        self._schedule_health_update("mqtt", True, None)

    async def _on_token_renewed(self) -> None:
        """Callback invoked when TokenManager renews JWT token.

        Requests reconnection via ConnectionCoordinator with new credentials.
        """
        if self._connection_coordinator is not None and self._mqtt_ready:
            LOGGER.info("JWT token renewed, requesting MQTT reconnection")
            self._connection_coordinator.request_reconnect(
                ReconnectReason.TOKEN_RENEWED
            )

    async def _stop_services(self) -> None:
        await self._transition_state(AgentState.STOPPING, detail="shutdown requested")
        self._stopping = True

        # Stop connection coordinator first
        if self._connection_coordinator is not None:
            await self._connection_coordinator.stop_supervisor()

        await self._stop_moonraker_monitor()
        await self._deactivate_components("shutdown", preserve_instances=False)
        await self._stop_health_server()

        if self._printer_backend is not None:
            await self._printer_backend.stop()
            self._printer_backend = None

        if self._mqtt_client is not None:
            await self._mqtt_client.disconnect()
            self._mqtt_client = None
            await self._health.update("mqtt", False, "shutdown")

        if self._token_manager is not None:
            LOGGER.info("Stopping TokenManager")
            await self._token_manager.stop()
            self._token_manager = None

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
