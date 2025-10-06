# moonraker-owl

A Python companion service that links Moonraker-powered printers to the Owl Cloud platform. The repository currently contains scaffolding for the service, installer scripts, and configuration loader. Implementation of telemetry streaming, command handling, and linking is planned in subsequent phases.

## Layout

```
Owl.Moonraker/
├─ moonraker_owl/           # Python package (application code)
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
