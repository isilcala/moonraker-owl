"""Command idempotency guard for preventing duplicate execution.

This module implements the idempotency requirements from ADR-0013 Appendix D.
It tracks processed command IDs to prevent duplicate execution when the cloud
retries commands due to network issues or missed ACKs.

Key features:
- TTL-based expiration (default 24 hours)
- Memory-efficient dict with periodic cleanup
- Thread-safe for async usage
- Persists terminal status for duplicate detection and ACK replay

See: docs/adr/0013-agent-communication-architecture-v2.md#appendix-d-agent-side-idempotency
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ProcessedCommand:
    """Record of a processed command for idempotency tracking."""

    command_id: str
    status: str
    stage: str
    processed_at: datetime
    error_code: Optional[str] = None
    error_message: Optional[str] = None


class CommandIdempotencyGuard:
    """
    Tracks processed command IDs to prevent duplicate execution.
    Uses in-memory dict with TTL-based expiration.

    This is the authoritative source for "has this command been processed?"
    The cloud-side Outbox won't retry completed commands after the TTL period,
    so the 24-hour window is sufficient.

    Usage:
        guard = CommandIdempotencyGuard(ttl_hours=24)

        # Before executing a command:
        if not guard.should_process(command_id):
            # Duplicate - send ACK without re-executing
            cached = guard.get_cached_result(command_id)
            await send_ack(command_id, cached.status, skipped=True)
            return

        # Execute command...
        result = await execute(command)

        # After successful execution:
        guard.mark_processed(command_id, status="completed", stage="execution")

        await send_ack(command_id, result.status)
    """

    def __init__(
        self,
        ttl_hours: int = 24,
        max_entries: int = 10000,
        cleanup_interval: int = 100,
        *,
        state_path: Optional[Path] = None,
    ) -> None:
        """
        Initialize the idempotency guard.

        Args:
            ttl_hours: Time-to-live for processed entries (default 24 hours).
            max_entries: Maximum entries before forced cleanup (memory safety).
            cleanup_interval: Run cleanup every N operations.
            state_path: Optional path to persist processed entries across
                process restarts (audit A-08). When set, the guard loads
                non-expired entries on init and writes the dict on every
                ``mark_processed`` call. The file is non-secret operational
                state (command IDs + terminal status + timestamps) but is
                still chmod'd ``0o600`` for hygiene because it lives in
                the same directory as device credentials. Set to ``None``
                to keep the legacy in-memory-only behaviour (used by tests).
        """
        self._processed: Dict[str, ProcessedCommand] = {}
        self._ttl = timedelta(hours=ttl_hours)
        self._max_entries = max_entries
        self._cleanup_interval = cleanup_interval
        self._operation_count = 0
        self._state_path = state_path
        if state_path is not None:
            self._load_from_disk()

    def should_process(self, command_id: str) -> bool:
        """
        Check if a command should be processed.

        Returns True if command should be processed (not a duplicate).
        Returns False if this is a duplicate (already processed within TTL).
        """
        self._maybe_cleanup()

        if command_id in self._processed:
            entry = self._processed[command_id]
            # Verify not expired
            if not self._is_expired(entry):
                LOGGER.info(
                    "Duplicate command detected: %s (processed at %s, status=%s)",
                    command_id,
                    entry.processed_at.isoformat(),
                    entry.status,
                )
                return False
            # Entry expired, remove it and allow reprocessing
            del self._processed[command_id]

        return True

    def mark_processed(
        self,
        command_id: str,
        *,
        status: str,
        stage: str,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Mark a command as successfully processed.

        Call this AFTER the command has been executed and ACK sent.
        The status is stored for replay if the cloud retries.
        """
        # Check if we need cleanup before adding
        self._maybe_cleanup()

        self._processed[command_id] = ProcessedCommand(
            command_id=command_id,
            status=status,
            stage=stage,
            processed_at=datetime.now(timezone.utc),
            error_code=error_code,
            error_message=error_message,
        )
        LOGGER.debug(
            "Command %s marked as processed: status=%s, stage=%s",
            command_id,
            status,
            stage,
        )
        # Audit A-08: persist immediately so a restart-during-Outbox-retry
        # window doesn't re-execute the command. The write is atomic
        # (temp + replace); a partial write at worst loses the most
        # recent entry, which falls back to re-execution — same as
        # the pre-A-08 behaviour, so this is safe.
        if self._state_path is not None:
            self._save_to_disk()

    def get_cached_result(self, command_id: str) -> Optional[ProcessedCommand]:
        """
        Get the cached result for a processed command.

        Returns None if not found or expired.
        """
        entry = self._processed.get(command_id)
        if entry is None:
            return None
        if self._is_expired(entry):
            del self._processed[command_id]
            return None
        return entry

    def clear(self) -> None:
        """Clear all tracked commands. Used for testing or reset."""
        self._processed.clear()
        self._operation_count = 0

    @property
    def entry_count(self) -> int:
        """Get current number of tracked entries."""
        return len(self._processed)

    def _is_expired(self, entry: ProcessedCommand) -> bool:
        """Check if an entry has expired based on TTL."""
        cutoff = datetime.now(timezone.utc) - self._ttl
        return entry.processed_at < cutoff

    def _maybe_cleanup(self) -> None:
        """
        Periodically remove expired entries to prevent memory growth.
        Also enforces max_entries limit.
        """
        self._operation_count += 1

        # Run cleanup periodically or when we hit max entries
        if (
            self._operation_count % self._cleanup_interval != 0
            and len(self._processed) < self._max_entries
        ):
            return

        self._cleanup_expired()

    def _cleanup_expired(self) -> None:
        """Remove all expired entries."""
        cutoff = datetime.now(timezone.utc) - self._ttl
        expired_keys = [
            k for k, v in self._processed.items() if v.processed_at < cutoff
        ]

        for key in expired_keys:
            del self._processed[key]

        if expired_keys:
            LOGGER.debug("Cleaned up %d expired command entries", len(expired_keys))

        # If still over limit after TTL cleanup, remove oldest entries
        if len(self._processed) >= self._max_entries:
            # Sort by processed_at and remove oldest half
            sorted_entries = sorted(
                self._processed.items(), key=lambda x: x[1].processed_at
            )
            remove_count = len(sorted_entries) // 2
            for key, _ in sorted_entries[:remove_count]:
                del self._processed[key]
            LOGGER.warning(
                "Forced cleanup of %d oldest command entries (max_entries=%d reached)",
                remove_count,
                self._max_entries,
            )

    # ------------------------------------------------------------------
    # Persistence (audit A-08)
    # ------------------------------------------------------------------

    def _load_from_disk(self) -> None:
        """Load persisted entries, dropping any that have expired."""
        path = self._state_path
        if path is None or not path.exists():
            return
        try:
            raw = path.read_text(encoding="utf-8")
            payload = json.loads(raw) if raw.strip() else {}
        except (OSError, ValueError) as exc:
            LOGGER.warning(
                "Failed to read idempotency state from %s (%s); starting empty.",
                path,
                exc,
            )
            return

        entries = payload.get("entries") if isinstance(payload, dict) else None
        if not isinstance(entries, list):
            return

        cutoff = datetime.now(timezone.utc) - self._ttl
        loaded = 0
        dropped = 0
        for item in entries:
            if not isinstance(item, dict):
                continue
            try:
                processed_at = datetime.fromisoformat(item["processed_at"])
                if processed_at.tzinfo is None:
                    processed_at = processed_at.replace(tzinfo=timezone.utc)
                command_id = str(item["command_id"])
                status = str(item["status"])
                stage = str(item["stage"])
            except (KeyError, ValueError, TypeError):
                dropped += 1
                continue
            if processed_at < cutoff:
                dropped += 1
                continue
            self._processed[command_id] = ProcessedCommand(
                command_id=command_id,
                status=status,
                stage=stage,
                processed_at=processed_at,
                error_code=item.get("error_code"),
                error_message=item.get("error_message"),
            )
            loaded += 1
        if loaded or dropped:
            LOGGER.info(
                "Idempotency state restored from %s: %d entries loaded, %d dropped (expired/invalid).",
                path,
                loaded,
                dropped,
            )

    def _save_to_disk(self) -> None:
        """Atomically persist the processed map; non-fatal on errors."""
        path = self._state_path
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "version": 1,
                "entries": [
                    {
                        "command_id": entry.command_id,
                        "status": entry.status,
                        "stage": entry.stage,
                        "processed_at": entry.processed_at.isoformat(),
                        "error_code": entry.error_code,
                        "error_message": entry.error_message,
                    }
                    for entry in self._processed.values()
                ],
            }
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            fd, tmp_name = tempfile.mkstemp(
                prefix=".idempotency-", suffix=".tmp", dir=str(path.parent)
            )
            tmp_path = Path(tmp_name)
            try:
                with os.fdopen(fd, "wb") as handle:
                    # chmod 0600 before writing payload
                    try:
                        os.fchmod(handle.fileno(), 0o600)
                    except (AttributeError, OSError):
                        # Windows / unsupported FS — best-effort.
                        pass
                    handle.write(data)
                    handle.flush()
                    try:
                        os.fsync(handle.fileno())
                    except OSError:
                        pass
                tmp_path.replace(path)
                try:
                    os.chmod(path, 0o600)
                except OSError:
                    pass
            except Exception:
                try:
                    tmp_path.unlink(missing_ok=True)  # type: ignore[call-arg]
                except TypeError:
                    if tmp_path.exists():
                        tmp_path.unlink()
                raise
        except OSError as exc:
            # Persistence is best-effort; an I/O failure must not break
            # command processing. Log once at warning level.
            LOGGER.warning(
                "Failed to persist idempotency state to %s (%s); continuing in-memory.",
                path,
                exc,
            )

        # If still over limit after TTL cleanup, remove oldest entries
        if len(self._processed) >= self._max_entries:
            # Sort by processed_at and remove oldest half
            sorted_entries = sorted(
                self._processed.items(), key=lambda x: x[1].processed_at
            )
            remove_count = len(sorted_entries) // 2
            for key, _ in sorted_entries[:remove_count]:
                del self._processed[key]
            LOGGER.warning(
                "Forced cleanup of %d oldest command entries (max_entries=%d reached)",
                remove_count,
                self._max_entries,
            )
