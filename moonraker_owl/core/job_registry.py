"""In-memory registry for PrintJob mappings.

This module provides a simple in-memory cache that maps Moonraker job information
to cloud-side PrintJobId. The mapping is populated when the server sends a
`job:registered` command after creating a PrintJob record.

Design decisions:
- Pure in-memory storage (no persistence) because:
  1. Timelapse render window is typically < 5 minutes
  2. If Agent restarts, we'd also miss the timelapse event itself
  3. Backend fallback (MoonrakerJobId lookup) handles edge cases
- Bounded size with automatic cleanup to prevent memory leaks
- Thread-safe for use from asyncio event loop
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Optional

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PrintJobMapping:
    """Represents a mapping between Moonraker job and cloud PrintJob."""

    moonraker_job_id: str
    print_job_id: str
    filename: str
    registered_at: datetime


class PrintJobRegistry:
    """In-memory registry for PrintJob ID mappings.

    When the server creates a PrintJob, it sends a `job:registered` command
    with the mapping information. This registry stores that mapping so that
    subsequent events (like timelapse detection) can include the correct
    PrintJobId.

    The registry is intentionally NOT persisted because:
    1. Timelapse render window is typically < 5 minutes
    2. If Agent restarts, we'd also miss the timelapse event itself
    3. Backend fallback (MoonrakerJobId lookup) handles edge cases

    Usage:
        registry = PrintJobRegistry()

        # When job:registered command is received
        registry.register(
            moonraker_job_id="0002DD",
            print_job_id="550e8400-e29b-41d4-a716-446655440000",
            filename="benchy.gcode"
        )

        # When timelapse event is detected
        mapping = registry.find_by_filename("benchy.gcode")
        if mapping:
            print_job_id = mapping.print_job_id
    """

    MAX_ENTRIES = 50  # Prevent unbounded growth
    MAX_AGE = timedelta(hours=24)  # Auto-cleanup old entries

    def __init__(
        self,
        *,
        max_entries: int = MAX_ENTRIES,
        max_age: timedelta = MAX_AGE,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self._max_entries = max_entries
        self._max_age = max_age
        self._clock = clock or (lambda: datetime.now(timezone.utc))

        # Two indexes for efficient lookup
        self._by_moonraker_id: Dict[str, PrintJobMapping] = {}
        self._by_filename: OrderedDict[str, PrintJobMapping] = OrderedDict()

    def register(
        self,
        moonraker_job_id: str,
        print_job_id: str,
        filename: str,
    ) -> None:
        """Register a new PrintJob mapping.

        Args:
            moonraker_job_id: Moonraker's job ID (e.g., "0002DD")
            print_job_id: Cloud-side PrintJob UUID
            filename: The gcode filename being printed
        """
        mapping = PrintJobMapping(
            moonraker_job_id=moonraker_job_id,
            print_job_id=print_job_id,
            filename=filename,
            registered_at=self._clock(),
        )

        # Store in both indexes
        self._by_moonraker_id[moonraker_job_id] = mapping
        # Move to end if already exists (for LRU-like behavior)
        if filename in self._by_filename:
            del self._by_filename[filename]
        self._by_filename[filename] = mapping

        LOGGER.debug(
            "Registered PrintJob mapping: %s -> %s (file: %s)",
            moonraker_job_id,
            print_job_id[:8] if len(print_job_id) > 8 else print_job_id,
            filename,
        )

        self._cleanup_if_needed()

    def find_by_filename(self, filename: str) -> Optional[PrintJobMapping]:
        """Find a mapping by gcode filename.

        Args:
            filename: The gcode filename to search for

        Returns:
            The mapping if found, None otherwise
        """
        mapping = self._by_filename.get(filename)
        if mapping and self._is_expired(mapping):
            self._remove_mapping(mapping)
            return None
        return mapping

    def find_by_moonraker_job_id(self, job_id: str) -> Optional[PrintJobMapping]:
        """Find a mapping by Moonraker job ID.

        Args:
            job_id: Moonraker's job ID (e.g., "0002DD")

        Returns:
            The mapping if found, None otherwise
        """
        mapping = self._by_moonraker_id.get(job_id)
        if mapping and self._is_expired(mapping):
            self._remove_mapping(mapping)
            return None
        return mapping

    def size(self) -> int:
        """Return the number of entries in the registry."""
        return len(self._by_filename)

    def clear(self) -> None:
        """Clear all entries from the registry."""
        self._by_moonraker_id.clear()
        self._by_filename.clear()
        LOGGER.debug("PrintJob registry cleared")

    def _is_expired(self, mapping: PrintJobMapping) -> bool:
        """Check if a mapping has expired."""
        age = self._clock() - mapping.registered_at
        return age > self._max_age

    def _remove_mapping(self, mapping: PrintJobMapping) -> None:
        """Remove a mapping from both indexes."""
        self._by_moonraker_id.pop(mapping.moonraker_job_id, None)
        self._by_filename.pop(mapping.filename, None)

    def _cleanup_if_needed(self) -> None:
        """Remove old or excess entries."""
        # Remove expired entries
        now = self._clock()
        expired_filenames = [
            fn
            for fn, m in self._by_filename.items()
            if (now - m.registered_at) > self._max_age
        ]
        for filename in expired_filenames:
            mapping = self._by_filename.pop(filename, None)
            if mapping:
                self._by_moonraker_id.pop(mapping.moonraker_job_id, None)

        # Remove oldest entries if over limit
        while len(self._by_filename) > self._max_entries:
            oldest_filename, oldest_mapping = self._by_filename.popitem(last=False)
            self._by_moonraker_id.pop(oldest_mapping.moonraker_job_id, None)
            LOGGER.debug(
                "Evicted old PrintJob mapping: %s (limit: %d)",
                oldest_filename,
                self._max_entries,
            )
