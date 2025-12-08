"""W3C Trace Context support for distributed tracing.

This module provides lightweight W3C Trace Context (traceparent) generation
without requiring the full OpenTelemetry SDK. This keeps the agent lightweight
while enabling end-to-end tracing when messages flow to the cloud.

The traceparent header format (per W3C spec):
    00-{trace-id}-{span-id}-{trace-flags}

Where:
    - version: 2 hex digits, always "00" for current spec
    - trace-id: 32 hex digits (128-bit random)
    - span-id: 16 hex digits (64-bit random)
    - trace-flags: 2 hex digits, "01" = sampled

References:
    - https://www.w3.org/TR/trace-context/
    - https://www.w3.org/TR/trace-context/#traceparent-header
"""

import secrets
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TraceContext:
    """Represents a W3C Trace Context."""

    trace_id: str  # 32 hex chars
    span_id: str  # 16 hex chars
    sampled: bool = True

    @property
    def traceparent(self) -> str:
        """Format as W3C traceparent header value.

        Format: {version}-{trace-id}-{span-id}-{trace-flags}
        Example: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
        """
        flags = "01" if self.sampled else "00"
        return f"00-{self.trace_id}-{self.span_id}-{flags}"

    @classmethod
    def create(cls, sampled: bool = True) -> "TraceContext":
        """Create a new trace context with random IDs.

        Args:
            sampled: Whether this trace should be sampled (recorded).

        Returns:
            A new TraceContext with cryptographically random IDs.
        """
        # Generate 128-bit trace ID (32 hex chars)
        trace_id = secrets.token_hex(16)
        # Generate 64-bit span ID (16 hex chars)
        span_id = secrets.token_hex(8)
        return cls(trace_id=trace_id, span_id=span_id, sampled=sampled)

    @classmethod
    def parse(cls, traceparent: str) -> Optional["TraceContext"]:
        """Parse a W3C traceparent header value.

        Args:
            traceparent: The traceparent header value to parse.

        Returns:
            TraceContext if valid, None if invalid format.
        """
        if not traceparent:
            return None

        parts = traceparent.split("-")
        if len(parts) != 4:
            return None

        version, trace_id, span_id, flags = parts

        # Validate format
        if version != "00":
            return None
        if len(trace_id) != 32 or not _is_hex(trace_id):
            return None
        if len(span_id) != 16 or not _is_hex(span_id):
            return None
        if len(flags) != 2 or not _is_hex(flags):
            return None

        # All-zero trace-id or span-id is invalid
        if trace_id == "0" * 32 or span_id == "0" * 16:
            return None

        sampled = (int(flags, 16) & 0x01) == 0x01
        return cls(trace_id=trace_id, span_id=span_id, sampled=sampled)


def _is_hex(s: str) -> bool:
    """Check if string contains only hex characters."""
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


def create_traceparent(sampled: bool = True) -> str:
    """Create a new W3C traceparent header value.

    This is the main entry point for creating trace context.
    Use this when starting a new trace (e.g., publishing telemetry).

    Args:
        sampled: Whether this trace should be sampled.

    Returns:
        A valid W3C traceparent header value.

    Example:
        >>> traceparent = create_traceparent()
        >>> print(traceparent)
        00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
    """
    return TraceContext.create(sampled=sampled).traceparent
