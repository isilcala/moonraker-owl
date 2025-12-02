"""Centralized command name constants for printer MQTT command/ack topics.

These names appear as the final segment of command topics:
    owl/printers/{deviceId}/commands/{commandName}

Command naming conventions:
- Print control: Simple names (pause, resume, cancel)
- Device control: {entity}:{action} (heater:set-target, fan:set-speed)
- Media operations: {action}:{target} (upload:thumbnail, capture:frame)
- Telemetry control: sensors:{action} (sensors:set-rate)

All names must match the regex: ^[a-z0-9._:-]{2,64}$

Keep in sync with C# PrinterCommandNames.cs.
"""

from __future__ import annotations


class PrinterCommandNames:
    """Printer command name constants matching C# PrinterCommandNames.cs."""

    # -------------------------------------------------------------------------
    # Print Control Commands
    # -------------------------------------------------------------------------

    PAUSE = "pause"
    """Pause the current print job."""

    RESUME = "resume"
    """Resume a paused print job."""

    CANCEL = "cancel"
    """Cancel the current print job."""

    EMERGENCY_STOP = "emergency-stop"
    """Emergency stop - immediately halt all printer operations."""

    FIRMWARE_RESTART = "firmware-restart"
    """Restart the Klipper firmware."""

    # -------------------------------------------------------------------------
    # Telemetry Control Commands
    # -------------------------------------------------------------------------

    SENSORS_SET_RATE = "sensors:set-rate"
    """Set telemetry sensors reporting rate (watch window)."""

    # -------------------------------------------------------------------------
    # Heater Control Commands
    # -------------------------------------------------------------------------

    HEATER_SET_TARGET = "heater:set-target"
    """Set target temperature for a heater."""

    HEATER_TURN_OFF = "heater:turn-off"
    """Turn off a heater (set target to 0)."""

    # -------------------------------------------------------------------------
    # Fan Control Commands
    # -------------------------------------------------------------------------

    FAN_SET_SPEED = "fan:set-speed"
    """Set fan speed."""

    # -------------------------------------------------------------------------
    # Media Upload Commands
    # -------------------------------------------------------------------------

    UPLOAD_THUMBNAIL = "upload:thumbnail"
    """Upload print job thumbnail to cloud storage."""

    CAPTURE_FRAME = "capture:frame"
    """Capture and upload camera frame for AI detection."""

    # -------------------------------------------------------------------------
    # Command Sets
    # -------------------------------------------------------------------------

    PRINT_CONTROL_COMMANDS = frozenset({PAUSE, RESUME, CANCEL})
    """Commands that control print job state."""

    STATE_CONFIRMATION_COMMANDS = frozenset({PAUSE, RESUME, CANCEL})
    """Commands that require printer state confirmation before ACK."""
