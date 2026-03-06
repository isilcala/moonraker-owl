"""Shared JSON serialization helpers."""

from __future__ import annotations

from typing import Any


def json_default(value: Any) -> Any:
    """Fallback serializer for ``json.dumps(default=...)``.

    Converts unknown types to their string representation so that
    Moonraker objects (e.g. ``Sentinel``) don't crash telemetry encoding.
    """
    try:
        return str(value)
    except Exception:  # pragma: no cover - defensive fallback
        return repr(value)
