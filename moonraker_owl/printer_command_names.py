"""Centralized command name constants for printer MQTT command/ack topics.

These names appear as the final segment of command topics:
    owl/printers/{deviceId}/commands/{commandName}

Command naming conventions (ADR-0013):
- User commands: {domain}:{action} (print:pause, heater:set-target)
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

    START = "print:start"
    """Start printing a specified GCode file."""

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
    # System Task Commands (cloud-triggered, requires result)
    # -------------------------------------------------------------------------

    UPLOAD_THUMBNAIL = "task:upload-thumbnail"
    """Upload print job thumbnail to presigned URL."""

    CAPTURE_IMAGE = "task:capture-image"
    """Capture and upload camera frame to presigned URL."""

    UPLOAD_TIMELAPSE = "task:upload-timelapse"
    """Upload timelapse video and preview to presigned URLs."""

    DOWNLOAD_GCODE = "task:download-gcode"
    """Download GCode file from cloud storage and upload to Moonraker."""

    # -------------------------------------------------------------------------
    # Query Commands (Cold Path, data retrieval via ACK result)
    # -------------------------------------------------------------------------

    QUERY_FILE_LIST = "query:file-list"
    """Query GCode file list from the printer's local storage."""

    # -------------------------------------------------------------------------
    # Job Lifecycle Commands (cloud-triggered, informational)
    # -------------------------------------------------------------------------

    JOB_REGISTERED = "job:registered"
    """Server confirms PrintJob creation with cloud-side ID mapping."""

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

    # ── Dispatch Path Annotations (ADR-0039) ────────────────────────────────
    #
    # Keep in sync with C# PrinterCommandNames.HotPathCommands.
    #
    # Hot Path: fire-and-forget MQTT dispatch, no Outbox, < 200 ms ACK target.
    # Cold Path: Outbox + ACK wait, result processors, guaranteed delivery.
    #
    # The agent treats both paths identically (receive → ACK accepted → execute
    # → ACK completed/failed). The distinction is server-side only, documented
    # here for cross-codebase alignment.

    HOT_PATH_COMMANDS: frozenset[str] = frozenset({
        PAUSE, RESUME, CANCEL, EMERGENCY_STOP, FIRMWARE_RESTART, START,
        HEATER_SET_TARGET, HEATER_TURN_OFF, FAN_SET_SPEED,
        OBJECT_EXCLUDE, SET_TELEMETRY_RATE,
    })
    """Commands dispatched via Hot Path (ADR-0039): fire-and-forget MQTT."""

    COLD_PATH_COMMANDS: frozenset[str] = frozenset({
        UPLOAD_THUMBNAIL, CAPTURE_IMAGE, UPLOAD_TIMELAPSE,
        DOWNLOAD_GCODE,
    })

    COLD_PATH_QUERY_COMMANDS: frozenset[str] = frozenset({
        QUERY_FILE_LIST,
    })
    """Query commands dispatched via Cold Path for data retrieval (ADR-0039)."""
    """Commands dispatched via Cold Path (ADR-0039): Outbox + result processors."""

    @classmethod
    def is_hot_path(cls, command_name: str) -> bool:
        """Return True if the command uses Hot Path dispatch."""
        return command_name in cls.HOT_PATH_COMMANDS

    @classmethod
    def is_cold_path(cls, command_name: str) -> bool:
        """Return True if the command uses Cold Path dispatch."""
        return command_name in cls.COLD_PATH_COMMANDS
