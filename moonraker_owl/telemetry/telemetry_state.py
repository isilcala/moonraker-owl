"""Telemetry hashing utilities.

This module provides deterministic hashing for telemetry payloads,
used for deduplication in the cadence controller.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

# Precision for float normalization in hash computation
HASH_FLOAT_PRECISION = 4


def _normalize_value(value: Any) -> Any:
    """Normalize a value for deterministic hashing."""
    if isinstance(value, dict):
        return {key: _normalize_value(value[key]) for key in sorted(value.keys())}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if isinstance(value, float):
        return round(value, HASH_FLOAT_PRECISION)
    return value


class TelemetryHasher:
    """Produce deterministic hashes for telemetry payloads.
    
    This class provides a consistent hashing interface that can be
    injected into the ChannelCadenceController for deduplication.
    
    The hashing algorithm:
    1. Recursively sorts dict keys
    2. Rounds floats to HASH_FLOAT_PRECISION decimal places
    3. JSON-encodes with minimal separators
    4. Produces MD5 hex digest
    """

    def hash_payload(self, payload: Any) -> str:
        """Compute a deterministic hash for a payload."""
        normalized = _normalize_value(payload)
        blob = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        return hashlib.md5(blob).hexdigest()
