# moonraker-owl

[简体中文](README.zh-CN.md) | English

Owl Cloud companion agent for Moonraker-powered 3D printers. This service connects your Klipper printer to the Owl Cloud platform for remote monitoring, AI defect detection, and print management.

## Features

- 📡 **Real-time Telemetry**: Streams printer status, temperatures, and print progress to Owl Cloud
- 🎮 **Remote Control**: Pause, resume, and cancel prints from anywhere
- 📸 **Camera Capture**: Integrates with webcam for AI-powered defect detection
- 🔄 **Auto-Update**: Seamless updates via Moonraker Update Manager
- 🔒 **Secure**: JWT-based authentication with end-to-end encryption

## Requirements

- **Klipper** with **Moonraker** installed and running
- **Python 3.10** or later
- **Linux** (tested on Raspberry Pi OS, Armbian, MainsailOS)
- Internet connection

## Quick Start

### 1. Clone the Repository

```bash
cd ~
git clone https://gitee.com/project-owl/agent.git moonraker-owl
cd moonraker-owl
```

### 2. Run the Installer

```bash
./scripts/install.sh
```

The installer will:
- Create a Python virtual environment in `.venv/`
- Install moonraker-owl and dependencies
- Create the configuration file at `~/printer_data/config/moonraker-owl.toml`
- Register and enable the systemd service

### 3. Link to Owl Cloud

Before starting the service, you must link your printer:

1. Open **Owl Web** and navigate to **Printers → Add Printer**
2. Copy the 6-character link code
3. Run the link command:

```bash
~/moonraker-owl/.venv/bin/moonraker-owl --config ~/printer_data/config/moonraker-owl.toml link
```

4. Enter the link code when prompted
5. Start the service:

```bash
sudo systemctl start moonraker-owl
```

### 4. Verify Installation

```bash
# Check service status
sudo systemctl status moonraker-owl

# View live logs
journalctl -u moonraker-owl -f

# View configuration
~/moonraker-owl/.venv/bin/moonraker-owl --config ~/printer_data/config/moonraker-owl.toml show-config
```

## Moonraker Update Manager

Add the following to your `moonraker.conf` for automatic updates:

```ini
[update_manager moonraker-owl]
type: git_repo
path: ~/moonraker-owl
origin: https://gitee.com/isilcala/moonraker-owl.git
primary_branch: main
virtualenv: ~/moonraker-owl/.venv
requirements: requirements.txt
install_script: scripts/install.sh
is_system_service: True
managed_services: moonraker-owl
```

After adding, restart Moonraker:

```bash
sudo systemctl restart moonraker
```

## Configuration

The configuration file is located at `~/printer_data/config/moonraker-owl.toml`.

### Configuration Architecture

The agent uses a three-tier configuration model:

| Tier | Source | Scope | Example Settings |
|------|--------|-------|------------------|
| **A** (Infra) | `moonraker-owl.toml` | Local on-device | `base_url`, `broker_host`, `moonraker.url`, `logging.level` |
| **B** (Credentials) | `~/.moonraker-owl/credentials.json` | Per-device secrets | `device_id`, `device_secret` |
| **C** (Cloud) | Cloud API + LKG cache | Fleet-managed | Telemetry cadence, camera settings, compression |

- **Tier A** — edited by the user in the TOML file. Infrastructure settings that vary per physical installation.
- **Tier B** — written automatically during the `link` command. Stored separately from the config file so TOML remains safe to share. Legacy `~/.owl/device.json` is auto-migrated.
- **Tier C** — fetched from the cloud API on startup and periodically refreshed. A last-known-good (LKG) cache at `~/.moonraker-owl/cloud-config.json` is used when the cloud is unreachable. Changes pushed via MQTT notifications are applied immediately without restart.

### Key Settings

| Section | Setting | Description |
|---------|---------|-------------|
| `[cloud]` | `base_url` | Owl Cloud API endpoint |
| `[cloud]` | `broker_host` | MQTT broker address |
| `[moonraker]` | `url` | Local Moonraker API URL |
| `[camera]` | `enabled` | Enable camera capture for AI detection |
| `[camera]` | `snapshot_url` | Webcam snapshot URL |
| `[logging]` | `level` | Log level (DEBUG, INFO, WARNING, ERROR) |

See `owl.toml.example` for all available options with descriptions.

### Default Environment

The agent is pre-configured to connect to the **Staging environment**:

| Setting | Default Value |
|---------|---------------|
| API Endpoint | `https://owl.elencala.com` |
| MQTT Broker | `mqtt.owl.elencala.com:8883` (TLS) |
| Web Interface | `https://owl.elencala.com` |

### Camera Setup

Camera capture is **zero-configuration** by default. The agent automatically discovers
webcams from your Moonraker configuration - no manual URL setup required!

To enable AI defect detection, simply set:

```ini
[camera]
enabled = true
```

The agent will:
1. Query Moonraker's webcam API (`/server/webcams/list`)
2. Auto-select the first available webcam (or use `camera_name` to pick a specific one)
3. Resolve relative URLs (e.g., `/webcam/snapshot`) to absolute URLs
4. Cache the discovered URL for performance (cache refreshes on Moonraker reconnect)

**Configuration priority:**

| Priority | Config | Behavior |
|----------|--------|----------|
| 1 | `snapshot_url = http://...` | Use explicit URL directly (for external cameras) |
| 2 | `camera_name = bed_cam` | Auto-discover, but select camera by name |
| 3 | `camera_name = auto` (default) | Auto-discover, use first available camera |

**Examples:**

```ini
# Simplest: auto-discover first available camera
[camera]
enabled = true

# Select a specific webcam by name (as configured in moonraker.conf)
[camera]
enabled = true
camera_name = bed_cam

# Manual URL for external/IP cameras not in Moonraker config
[camera]
enabled = true
snapshot_url = http://192.168.1.100:8080/snapshot
```

**Troubleshooting:**

- If auto-discovery fails, check that your webcam is configured in `moonraker.conf`
- View discovered cameras: `curl http://localhost:7125/server/webcams/list`
- The agent logs the discovered URL at startup (check `journalctl -u moonraker-owl`)

## Commands

| Command | Description |
|---------|-------------|
| `moonraker-owl start` | Start the agent (normally run via systemd) |
| `moonraker-owl link` | Link printer to Owl Cloud |
| `moonraker-owl show-config` | Display current configuration |

## Service Management

```bash
# Start service
sudo systemctl start moonraker-owl

# Stop service
sudo systemctl stop moonraker-owl

# Restart service
sudo systemctl restart moonraker-owl

# View status
sudo systemctl status moonraker-owl

# View logs
journalctl -u moonraker-owl -f

# Enable on boot (done by installer)
sudo systemctl enable moonraker-owl

# Disable on boot
sudo systemctl disable moonraker-owl
```

## Uninstallation

To remove moonraker-owl:

```bash
cd ~/moonraker-owl
./scripts/uninstall.sh
```

Options:
- `--keep-config`: Keep configuration and credentials for reinstallation
- `--all`: Remove everything without prompting

To completely remove the source code after uninstalling:

```bash
rm -rf ~/moonraker-owl
```

## Troubleshooting

### Service won't start

1. Check if linked:
   ```bash
   cat ~/.moonraker-owl/credentials.json
   ```
   If the file doesn't exist, run the link command first.

2. Check logs:
   ```bash
   journalctl -u moonraker-owl -n 50 --no-pager
   ```

### Cannot connect to Moonraker

1. Verify Moonraker is running:
   ```bash
   curl http://127.0.0.1:7125/server/info
   ```

2. Check the `url` setting in configuration.

### Camera not working

1. Test the snapshot URL directly:
   ```bash
   curl -o /tmp/test.jpg http://localhost/webcam/?action=snapshot
   ```

2. Verify `camera.enabled = true` in configuration.

## File Locations

| File | Path |
|------|------|
| Source code | `~/moonraker-owl/` |
| Virtual environment | `~/moonraker-owl/.venv/` |
| Configuration | `~/printer_data/config/moonraker-owl.toml` |
| Logs | `~/printer_data/logs/moonraker-owl.log` |
| Credentials | `~/.moonraker-owl/credentials.json` |
| Cloud config cache | `~/.moonraker-owl/cloud-config.json` |
| Service file | `/etc/systemd/system/moonraker-owl.service` |

## Development

For development and testing:

```bash
cd ~/moonraker-owl

# Create development environment
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest

# Run with debug logging
moonraker-owl --config owl.toml start
```

## Support

- **Documentation**: https://docs.owl.dev
- **Issues**: https://github.com/project-owl/agent/issues
- **Community**: https://discord.gg/owl-3dprint

## License

MIT License - see LICENSE file for details.
