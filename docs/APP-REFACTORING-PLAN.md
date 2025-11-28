# App.py Refactoring Plan

## Current State Analysis

`MoonrakerOwlApp` is a 932-line "god class" that mixes four distinct responsibilities:

### Responsibility Breakdown

| Category | Methods | Lines (approx) | Description |
|----------|---------|----------------|-------------|
| **Core Agent Logic** | `run()`, `_idle_loop()`, `_transition_state()`, `start()` | ~120 | Application lifecycle, state machine |
| **Service Orchestration** | `_start_services()`, `_stop_services()`, `_start_runtime_components()`, `_deactivate_components()`, `_restart_components()` | ~300 | Wiring up components, dependency management |
| **MQTT Communication** | `_connect_mqtt()`, `_on_mqtt_disconnect()`, `_on_mqtt_connect()`, `_on_token_renewed()`, `_on_connection_lost()`, `_on_connection_restored()` | ~150 | MQTT lifecycle, connection callbacks |
| **Moonraker Specific** | `_monitor_moonraker()`, `_analyse_moonraker_snapshot()`, `_register_moonraker_failure()`, `_register_moonraker_recovery()`, `_publish_moonraker_degraded()`, `_handle_telemetry_status_update()` | ~250 | Health monitoring, failure detection, Moonraker API interpretation |

### Problems

1. **Tight Coupling**: Moonraker-specific logic (snapshot analysis, state mapping) is embedded directly in the app class
2. **No Abstraction**: Cannot swap Moonraker for OctoPrint/Duet without massive changes
3. **Testing Difficulty**: Cannot test core agent logic independently of Moonraker
4. **Single Responsibility Violation**: One class does too much

## Target Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     OwlAgent (core)                          │
│  - Application lifecycle (run, idle_loop)                    │
│  - State machine (AgentState transitions)                    │
│  - Health reporting coordination                             │
│  - Service orchestration (start/stop components)             │
└──────────────────────────┬──────────────────────────────────┘
                           │ depends on
           ┌───────────────┴───────────────┐
           │      PrinterBackend           │
           │   (Protocol - new)            │
           │  - start(callback)            │
           │  - stop()                     │
           │  - assess_health() -> Health  │
           │  - create_telemetry_pub()     │
           │  - create_command_proc()      │
           └───────────────┬───────────────┘
                           │ implemented by
    ┌──────────────────────┼──────────────────────┐
    │                      │                      │
┌───┴────────┐       ┌─────┴─────┐          ┌────┴────┐
│ Moonraker  │       │ OctoPrint │          │  Duet   │
│  Backend   │       │  Backend  │          │ Backend │
│ (current)  │       │ (future)  │          │(future) │
└────────────┘       └───────────┘          └─────────┘
```

## Implementation Plan

### Phase 1: Define PrinterBackend Protocol

Create `moonraker_owl/core/printer_backend.py`:

```python
class PrinterHealthAssessment:
    healthy: bool
    detail: Optional[str] = None
    force_trip: bool = False

class PrinterBackend(Protocol):
    async def start(self, on_status: Callable[[dict], Awaitable[None]]) -> None:
        """Start printer connection and status streaming."""
        ...

    async def stop(self) -> None:
        """Stop printer connection."""
        ...

    async def assess_health(self) -> PrinterHealthAssessment:
        """Poll printer and return health assessment."""
        ...

    async def fetch_state(self) -> dict:
        """Get current printer state snapshot."""
        ...

    def create_telemetry_publisher(
        self, config: OwlConfig, mqtt: MQTTClient
    ) -> TelemetryPublisher:
        """Factory for printer-specific telemetry publisher."""
        ...

    def create_command_processor(
        self, config: OwlConfig, mqtt: MQTTClient, telemetry: TelemetryPublisher
    ) -> CommandProcessor:
        """Factory for printer-specific command processor."""
        ...
```

### Phase 2: Extract MoonrakerBackend

Move Moonraker-specific logic to `moonraker_owl/backends/moonraker.py`:

- `_monitor_moonraker()` → `MoonrakerBackend.start_monitor()`
- `_analyse_moonraker_snapshot()` → `MoonrakerBackend.assess_health()`
- `_register_moonraker_failure/recovery()` → Internal to MoonrakerBackend
- Snapshot parsing helpers → Stay in backend

### Phase 3: Refactor OwlAgent

Slim down `app.py` to use the `PrinterBackend` abstraction:

- Remove Moonraker-specific methods
- Inject `PrinterBackend` via constructor
- Keep state machine, orchestration, MQTT handling

### Phase 4: Update Tests

- Add unit tests for `MoonrakerBackend` in isolation
- Update integration tests to use dependency injection
- Ensure coverage remains at 100% for refactored code

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `core/printer_backend.py` | Create | Protocol + data classes |
| `backends/__init__.py` | Create | Package init |
| `backends/moonraker.py` | Create | MoonrakerBackend implementation |
| `app.py` | Modify | Remove Moonraker-specific code, use backend |
| `tests/test_moonraker_backend.py` | Create | Backend unit tests |
| `tests/test_app.py` | Modify | Update to use mocked backend |

## Migration Strategy

1. Create new modules alongside existing code
2. Refactor incrementally, keeping tests green
3. Final cleanup: remove dead code, update imports

## Risk Assessment

- **Low Risk**: Protocol extraction is additive
- **Medium Risk**: Moving methods may break existing tests
- **Mitigation**: Run tests after each small change
