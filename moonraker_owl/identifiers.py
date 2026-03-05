"""UUID v7 (RFC 9562) generation for message identifiers.

Python 3.14+ provides uuid.uuid7() natively. This module provides a
compatible implementation for Python >= 3.10.
"""

from __future__ import annotations

import os
import time
import uuid


def uuid7() -> uuid.UUID:
    """Generate a UUID v7 (RFC 9562) — time-ordered with random suffix.

    Layout (128 bits):
        48-bit unix_ts_ms | 4-bit ver (0x7) | 12-bit rand_a
        2-bit variant (0b10) | 62-bit rand_b
    """
    timestamp_ms = int(time.time() * 1000)
    rand_a = int.from_bytes(os.urandom(2), "big") & 0x0FFF
    rand_b = int.from_bytes(os.urandom(8), "big") & 0x3FFFFFFFFFFFFFFF

    value = (
        (timestamp_ms & 0xFFFFFFFFFFFF) << 80
        | 0x7 << 76
        | rand_a << 64
        | 0x2 << 62
        | rand_b
    )
    return uuid.UUID(int=value)
