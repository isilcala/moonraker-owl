# moonraker-owl

A Python companion service that links Moonraker-powered printers to the Owl Cloud platform. The repository currently contains scaffolding for the service, installer scripts, and configuration loader. Implementation of telemetry streaming, command handling, and linking is planned in subsequent phases.

## Layout

```
Owl.Moonraker/
├─ moonraker_owl/           # Python package (application code)
│   ├─ adapters.py          # MQTT and external service adapters
│   ├─ app.py               # Main OwlApp class and lifecycle
│   ├─ commands.py          # Command processor (pause/resume/cancel/sensors)
│   ├─ config.py            # Configuration loader and validation
│   ├─ connection.py        # MQTT connection coordinator with backoff
│   ├─ core.py              # Shared types and PrinterAdapter base
│   ├─ telemetry/           # Telemetry publishing pipeline
│   │   ├─ __init__.py      # TelemetryPublisher - main entry point
│   │   ├─ cadence.py       # Rate limiting and deduplication controller
│   │   ├─ events.py        # Event collector and priority queues
│   │   ├─ event_types.py   # Event data types and enums
│   │   ├─ orchestrator.py  # Payload building and channel coordination
│   │   ├─ polling.py       # Polling scheduler for non-subscribed objects
│   │   ├─ selectors/       # Channel-specific payload selectors
│   │   ├─ state_store.py   # Moonraker state aggregation
│   │   └─ telemetry_state.py  # Hashing utilities
│   └─ token_manager.py     # JWT token management and renewal
├─ scripts/                 # Installation and maintenance helpers
├─ systemd/                 # Service unit templates
├─ tests/                   # Test suite
└─ pyproject.toml           # Packaging metadata
```

## Development

Create a virtual environment and install dependencies:

```powershell
cd d:\Projects\Owl\Owl.Moonraker
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

Run the unit tests:

```powershell
pytest
```

## Module Overview

### Telemetry Pipeline

The `telemetry/` package handles all MQTT telemetry publishing:

- **TelemetryPublisher** (`__init__.py`): Main orchestrator that consumes Moonraker updates, applies cadence control, and publishes to MQTT channels (status/sensors/events)
- **ChannelCadenceController** (`cadence.py`): Enforces minimum publish intervals, deduplicates payloads via hashing, and manages watchdog timers
- **PollingScheduler** (`polling.py`): Declarative polling for Moonraker objects not available via websocket subscriptions
- **EventCollector** (`events.py`): Priority-based event queuing with rate limiting
- **TelemetryOrchestrator** (`orchestrator.py`): Builds channel payloads from aggregated Moonraker state

### Command Handling

`commands.py` processes cloud commands via MQTT:

- Subscribes to `owl/printers/{deviceId}/commands/#`
- Executes Moonraker actions (pause/resume/cancel)
- Tracks state-based command completion for accurate ACKs
- Handles `control:set-telemetry-rate` for telemetry cadence control

### Connection Management

`connection.py` provides resilient MQTT connectivity:

- ConnectionCoordinator state machine
- Automatic reconnection with exponential backoff
- Token renewal coordination without loops

## Status

- ✅ Configuration loader with defaults
- ✅ CLI scaffolding (start/link/show-config commands)
- ✅ Logging bootstrapper
- ✅ Moonraker and MQTT adapters
- ✅ Device linking workflow with persisted credentials
- ✅ Telemetry publisher and command processor (pause/resume/cancel) with unit tests
- ✅ Resilience supervisor (MQTT reconnect/backoff, health endpoint, CI workflow)
- ⏳ Deployment polish (installer upgrades, packaging, soak testing)

Track progress in `docs/plans/moonraker-owl-plugin-plan.md`.
