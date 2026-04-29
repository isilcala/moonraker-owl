"""Sprint 3 security regression tests.

Covers:
- A-08: persistent idempotency guard (survives process restart).
- A-09: agent-side floor on the sensors watch-mode interval.
"""

from __future__ import annotations

import json
import os
import stat
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from moonraker_owl.core.idempotency import (
    CommandIdempotencyGuard,
    ProcessedCommand,
)


# ---------------------------------------------------------------------------
# A-08: persistent idempotency
# ---------------------------------------------------------------------------


class TestIdempotencyPersistence:
    def test_persists_across_restart(self, tmp_path: Path) -> None:
        state_path = tmp_path / "idempotency.json"

        guard = CommandIdempotencyGuard(ttl_hours=24, state_path=state_path)
        guard.mark_processed(
            "cmd-001", status="completed", stage="execution"
        )
        guard.mark_processed(
            "cmd-002",
            status="failed",
            stage="execution",
            error_code="boom",
            error_message="went wrong",
        )

        assert state_path.exists(), "state file must be created on first write"

        # Simulate restart
        del guard
        restored = CommandIdempotencyGuard(ttl_hours=24, state_path=state_path)

        assert restored.entry_count == 2
        assert restored.should_process("cmd-001") is False
        cached = restored.get_cached_result("cmd-002")
        assert cached is not None
        assert cached.status == "failed"
        assert cached.error_code == "boom"
        assert cached.error_message == "went wrong"

    def test_expired_entries_dropped_on_load(self, tmp_path: Path) -> None:
        state_path = tmp_path / "idempotency.json"

        # Hand-craft a state file with one expired and one fresh entry.
        old = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        fresh = datetime.now(timezone.utc).isoformat()
        state_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "entries": [
                        {
                            "command_id": "cmd-old",
                            "status": "completed",
                            "stage": "execution",
                            "processed_at": old,
                        },
                        {
                            "command_id": "cmd-fresh",
                            "status": "completed",
                            "stage": "execution",
                            "processed_at": fresh,
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )

        guard = CommandIdempotencyGuard(ttl_hours=24, state_path=state_path)
        assert guard.entry_count == 1
        assert guard.should_process("cmd-old") is True
        assert guard.should_process("cmd-fresh") is False

    def test_corrupt_file_does_not_crash(self, tmp_path: Path) -> None:
        state_path = tmp_path / "idempotency.json"
        state_path.write_text("not-json{{{", encoding="utf-8")

        # Must not raise; logs a warning and starts empty.
        guard = CommandIdempotencyGuard(ttl_hours=24, state_path=state_path)
        assert guard.entry_count == 0
        # Still functional.
        guard.mark_processed("cmd-x", status="completed", stage="execution")
        assert guard.should_process("cmd-x") is False

    def test_missing_state_file_starts_empty(self, tmp_path: Path) -> None:
        state_path = tmp_path / "does-not-exist.json"
        guard = CommandIdempotencyGuard(ttl_hours=24, state_path=state_path)
        assert guard.entry_count == 0
        # Writing creates the file.
        guard.mark_processed("cmd-1", status="completed", stage="execution")
        assert state_path.exists()

    def test_no_state_path_keeps_in_memory_behavior(self, tmp_path: Path) -> None:
        """state_path=None must not write any file (legacy path used by tests)."""
        guard = CommandIdempotencyGuard(ttl_hours=24, state_path=None)
        guard.mark_processed("cmd-1", status="completed", stage="execution")
        # Nothing should have been created in tmp_path.
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="POSIX permission semantics not enforced on Windows",
    )
    def test_state_file_is_chmod_0600(self, tmp_path: Path) -> None:
        state_path = tmp_path / "idempotency.json"
        guard = CommandIdempotencyGuard(ttl_hours=24, state_path=state_path)
        guard.mark_processed("cmd-1", status="completed", stage="execution")

        mode = stat.S_IMODE(os.stat(state_path).st_mode)
        assert mode == 0o600, f"expected 0600, got {oct(mode)}"

    def test_unwritable_directory_does_not_crash(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """If persistence fails (e.g. read-only FS), command processing must still succeed."""
        # Point at a path inside a regular file (impossible directory).
        bogus_parent = tmp_path / "not-a-dir"
        bogus_parent.write_text("x", encoding="utf-8")
        state_path = bogus_parent / "idempotency.json"

        guard = CommandIdempotencyGuard(ttl_hours=24, state_path=state_path)
        # Must not raise.
        guard.mark_processed("cmd-1", status="completed", stage="execution")
        # In-memory state still tracked.
        assert guard.should_process("cmd-1") is False


# ---------------------------------------------------------------------------
# A-09: sensors watch-mode floor
# ---------------------------------------------------------------------------


class TestSensorsRateFloor:
    """Verify watch-mode interval is clamped to the configured floor."""

    def _make_publisher(self, floor: float = 0.5):
        # Imported lazily to keep test discovery fast and let the autouse
        # idempotency fixture from conftest.py run before any
        # processor.* import happens.
        from moonraker_owl.telemetry.publisher import TelemetryPublisher
        from helpers import build_config
        from test_telemetry import FakeMoonrakerClient, FakeMQTTClient

        config = build_config(sensors_interval_seconds=1.0)
        config.telemetry_cadence.sensors_min_interval_seconds = floor

        moonraker = FakeMoonrakerClient({"result": {"status": {}}})
        mqtt = FakeMQTTClient()
        return TelemetryPublisher(config, moonraker, mqtt, poll_specs=())

    def test_watch_below_floor_is_clamped(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        publisher = self._make_publisher(floor=0.5)

        publisher.apply_sensors_rate(
            mode="watch", interval_seconds=0.001, duration_seconds=10
        )

        # Internal interval should equal the floor, not the cloud value.
        assert publisher._sensors_interval == pytest.approx(0.5)

    def test_watch_above_floor_is_unchanged(self) -> None:
        publisher = self._make_publisher(floor=0.2)

        # Watch mode is also capped *upward* at publisher._watch_interval (1.0s),
        # so we pick a value above the floor but below that ceiling.
        publisher.apply_sensors_rate(
            mode="watch", interval_seconds=0.8, duration_seconds=10
        )
        assert publisher._sensors_interval == pytest.approx(0.8)

    def test_idle_mode_is_not_clamped(self) -> None:
        """Idle cadence is operator-configured (TOML) and may legitimately
        run faster than the watch floor; do not clamp it."""
        publisher = self._make_publisher(floor=0.5)

        publisher.apply_sensors_rate(
            mode="idle", interval_seconds=0.1, duration_seconds=None
        )
        # Idle mode falls back to publisher._idle_interval when interval
        # is supplied but mode != watch; interval=0.1 is preserved as-is.
        assert publisher._sensors_interval == pytest.approx(0.1)

    def test_default_floor_is_500ms(self) -> None:
        from moonraker_owl.config import TelemetryCadenceConfig

        cad = TelemetryCadenceConfig()
        assert cad.sensors_min_interval_seconds == 0.5
