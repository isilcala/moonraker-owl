"""Centralized command name constants for printer MQTT command/ack topics.

These names appear as the final segment of command topics:
    owl/printers/{deviceId}/commands/{commandName}

Command naming conventions (ADR-0013):
- User commands: {domain}:{action} (print:pause, heater:set-target)
- System sync: sync:{type} (sync:job-thumbnail) - fire-and-forget
- System control: control:{type} (control:set-telemetry-rate) - requires ACK
- System tasks: task:{type} (task:upload-thumbnail) - requires result

All names must match the regex: ^[a-z0-9._:-]{2,64}$

Keep in sync with C# PrinterCommandNames.cs.
"""

from __future__ import annotations


class PrinterCommandNames:
    """Printer command name constants matching C# PrinterCommandNames.cs."""

    # -------------------------------------------------------------------------
    # Print Control Commands (user-triggered)
    # -------------------------------------------------------------------------

    PAUSE = "print:pause"
    """Pause the current print job."""

    RESUME = "print:resume"
    """Resume a paused print job."""

    CANCEL = "print:cancel"
    """Cancel the current print job."""

    EMERGENCY_STOP = "print:emergency-stop"
    """Emergency stop - immediately halt all printer operations."""

    FIRMWARE_RESTART = "print:firmware-restart"
    """Restart the Klipper firmware."""

    REPRINT = "print:reprint"
    """Reprint the last print job."""

    # -------------------------------------------------------------------------
    # System Control Commands (cloud-triggered, requires ACK)
    # -------------------------------------------------------------------------

    SET_TELEMETRY_RATE = "control:set-telemetry-rate"
    """Set telemetry sensors reporting rate (watch window)."""

    # -------------------------------------------------------------------------
    # Heater Control Commands (user-triggered)
    # -------------------------------------------------------------------------

    HEATER_SET_TARGET = "heater:set-target"
    """Set target temperature for a heater."""

    HEATER_TURN_OFF = "heater:turn-off"
    """Turn off a heater (set target to 0)."""

    # -------------------------------------------------------------------------
    # Fan Control Commands (user-triggered)
    # -------------------------------------------------------------------------

    FAN_SET_SPEED = "fan:set-speed"
    """Set fan speed."""

    # -------------------------------------------------------------------------
    # System Sync Commands (cloud-triggered, fire-and-forget)
    # -------------------------------------------------------------------------

    SYNC_JOB_THUMBNAIL = "sync:job-thumbnail"
    """Sync the thumbnail URL for the current print job."""

    # -------------------------------------------------------------------------
    # System Task Commands (cloud-triggered, requires result)
    # -------------------------------------------------------------------------

    UPLOAD_THUMBNAIL = "task:upload-thumbnail"
    """Upload print job thumbnail to presigned URL."""

    CAPTURE_IMAGE = "task:capture-image"
    """Capture and upload camera frame to presigned URL."""

    # -------------------------------------------------------------------------
    # Object Control Commands (user-triggered, ADR-0016)
    # -------------------------------------------------------------------------

    OBJECT_EXCLUDE = "object:exclude"
    """Exclude an object from the current print."""

    # -------------------------------------------------------------------------
    # Command Sets
    # -------------------------------------------------------------------------

    PRINT_CONTROL_COMMANDS = frozenset({PAUSE, RESUME, CANCEL})
    """Commands that control print job state."""

    STATE_CONFIRMATION_COMMANDS = frozenset({PAUSE, RESUME, CANCEL})
    """Commands that require printer state confirmation before ACK."""
