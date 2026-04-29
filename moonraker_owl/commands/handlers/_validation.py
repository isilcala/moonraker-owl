"""Shared validation helpers for command handlers.

Centralises the regex check that prevents G-code injection through
cloud-supplied identifiers (LED names, pin names, fan names, macro names,
heater names, exclude-object names).

Klipper config object names accept ASCII letters, digits, underscores,
hyphens, and dots. See:
- https://www.klipper3d.org/Config_Reference.html (object naming convention)
- klippy/configfile.py: ``_pconfig`` accepts identifiers under those classes.
- Slicer-emitted ``EXCLUDE_OBJECT_DEFINE`` names commonly include dots.

Anything outside ``[A-Za-z0-9_\\-\\.]`` (notably whitespace, ``;``, ``\\n``,
``=`` and the rest of the G-code metacharacter set) is rejected so that a
cloud-supplied identifier cannot terminate the current command and inject
a follow-up one (e.g. ``foo\\nM112``).
"""

from __future__ import annotations

import re
from typing import Optional

from ..types import CommandProcessingError

# Pattern is intentionally narrower than what Moonraker tolerates so we have
# a defense-in-depth boundary even if Moonraker relaxes its parser later.
_KLIPPER_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_\-\.]+$")

# Klipper has no documented hard cap, but real-world object names are short.
# 128 is generous and well under any reasonable parser limit.
DEFAULT_MAX_IDENTIFIER_LENGTH = 128


def validate_klipper_identifier(
    name: str,
    *,
    field: str,
    command_id: Optional[str] = None,
    max_length: int = DEFAULT_MAX_IDENTIFIER_LENGTH,
) -> str:
    """Return ``name`` unchanged if it is a safe Klipper identifier.

    Raises :class:`CommandProcessingError` (code ``invalid_<field>_name``)
    if the value is empty, too long, or contains characters outside
    ``[A-Za-z0-9_\\-\\.]``. The error message is intentionally generic — it
    must not echo the rejected value (which may include control characters
    or be deliberately crafted to corrupt the log line).

    Parameters
    ----------
    name:
        The candidate identifier (already trimmed / case-normalised by the
        caller). Must be the *final* value that will be interpolated into a
        G-code script — not the raw cloud payload — so prefix-stripping logic
        in handlers stays intact.
    field:
        Logical field name (``"led"``, ``"pin"``, ``"fan"``, ``"macro"``,
        ``"heater"``). Used only for the error code/message; not interpolated
        into G-code.
    command_id:
        Optional command id to attach to the raised error.
    max_length:
        Hard upper bound on length. Default 128.
    """
    if not isinstance(name, str) or not name:
        raise CommandProcessingError(
            f"{field} name must be a non-empty string",
            code=f"invalid_{field}_name",
            command_id=command_id,
        )

    if len(name) > max_length:
        raise CommandProcessingError(
            f"{field} name exceeds maximum length of {max_length} characters",
            code=f"invalid_{field}_name",
            command_id=command_id,
        )

    if not _KLIPPER_IDENTIFIER_PATTERN.match(name):
        # Deliberately do not echo `name` back: it may contain newlines or
        # other control characters from a malicious payload.
        raise CommandProcessingError(
            f"Invalid {field} name. Only letters, digits, '_', '-', and '.' "
            "are allowed.",
            code=f"invalid_{field}_name",
            command_id=command_id,
        )

    return name


__all__ = [
    "DEFAULT_MAX_IDENTIFIER_LENGTH",
    "validate_klipper_identifier",
]
