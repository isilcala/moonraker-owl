"""Unit tests for MoonrakerBackend."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from moonraker_owl.backends.moonraker import (
    MoonrakerBackend,
    _extract_bool,
    _extract_str,
    _normalise_state,
)
from moonraker_owl.core import PrinterHealthAssessment


class TestMoonrakerBackendHealthAnalysis:
    """Tests for health assessment logic."""

    @pytest.fixture
    def backend(self) -> MoonrakerBackend:
        """Create a MoonrakerBackend with mocked client."""
        mock_config = MagicMock()
        mock_config.url = "http://localhost:7125"
        mock_client = AsyncMock()
        return MoonrakerBackend(mock_config, client=mock_client)

    def test_healthy_standby(self, backend: MoonrakerBackend) -> None:
        """Printer in standby state should be healthy."""
        snapshot = {
            "result": {
                "status": {
                    "webhooks": {"state": "ready"},
                    "printer": {"state": "ready", "is_shutdown": False},
                    "print_stats": {"state": "standby", "message": ""},
                }
            }
        }
        assessment = backend._analyse_snapshot(snapshot)
        assert assessment.healthy is True
        assert assessment.detail is None

    def test_healthy_printing(self, backend: MoonrakerBackend) -> None:
        """Printer actively printing should be healthy."""
        snapshot = {
            "result": {
                "status": {
                    "webhooks": {"state": "ready"},
                    "printer": {"state": "ready", "is_shutdown": False},
                    "print_stats": {"state": "printing", "message": ""},
                }
            }
        }
        assessment = backend._analyse_snapshot(snapshot)
        assert assessment.healthy is True

    def test_healthy_paused(self, backend: MoonrakerBackend) -> None:
        """Paused print should be healthy."""
        snapshot = {
            "result": {
                "status": {
                    "print_stats": {"state": "paused", "message": ""},
                }
            }
        }
        assessment = backend._analyse_snapshot(snapshot)
        assert assessment.healthy is True

    def test_unhealthy_printer_shutdown(self, backend: MoonrakerBackend) -> None:
        """Printer shutdown should be unhealthy with force_trip."""
        snapshot = {
            "result": {
                "status": {
                    "printer": {"state": "ready", "is_shutdown": True},
                    "print_stats": {"state": "error", "message": "MCU shutdown"},
                }
            }
        }
        assessment = backend._analyse_snapshot(snapshot)
        assert assessment.healthy is False
        assert assessment.force_trip is True
        assert "MCU shutdown" in (assessment.detail or "")

    def test_unhealthy_print_stats_error(self, backend: MoonrakerBackend) -> None:
        """Print stats error should be unhealthy with force_trip."""
        snapshot = {
            "result": {
                "status": {
                    "print_stats": {"state": "error", "message": "Heater timeout"},
                }
            }
        }
        assessment = backend._analyse_snapshot(snapshot)
        assert assessment.healthy is False
        assert assessment.force_trip is True
        assert "Heater timeout" in (assessment.detail or "")

    def test_unhealthy_printer_state_error(self, backend: MoonrakerBackend) -> None:
        """Printer state error should be unhealthy."""
        snapshot = {
            "result": {
                "status": {
                    "printer": {"state": "error", "is_shutdown": False},
                    "print_stats": {"state": "standby", "message": ""},
                }
            }
        }
        assessment = backend._analyse_snapshot(snapshot)
        assert assessment.healthy is False
        assert assessment.force_trip is True

    def test_webhooks_error_with_healthy_print_state(
        self, backend: MoonrakerBackend
    ) -> None:
        """Webhooks error but healthy print_stats should be healthy."""
        snapshot = {
            "result": {
                "status": {
                    "webhooks": {"state": "error"},
                    "print_stats": {"state": "printing", "message": ""},
                }
            }
        }
        assessment = backend._analyse_snapshot(snapshot)
        # When print_stats is in a healthy state, webhooks error is ignored
        assert assessment.healthy is True

    def test_webhooks_error_with_unhealthy_print_state(
        self, backend: MoonrakerBackend
    ) -> None:
        """Webhooks error with non-healthy print_stats should be unhealthy."""
        snapshot = {
            "result": {
                "status": {
                    "webhooks": {"state": "shutdown"},
                    "print_stats": {"state": "unknown", "message": ""},
                }
            }
        }
        assessment = backend._analyse_snapshot(snapshot)
        assert assessment.healthy is False
        assert assessment.force_trip is True

    def test_invalid_snapshot_not_dict(self, backend: MoonrakerBackend) -> None:
        """Non-dict snapshot should be unhealthy."""
        assessment = backend._analyse_snapshot("not a dict")  # type: ignore
        assert assessment.healthy is False
        assert assessment.force_trip is True
        assert "not a mapping" in (assessment.detail or "")

    def test_empty_snapshot(self, backend: MoonrakerBackend) -> None:
        """Empty snapshot should be healthy (no error indicators)."""
        assessment = backend._analyse_snapshot({})
        assert assessment.healthy is True


class TestMoonrakerBackendLifecycle:
    """Tests for backend start/stop lifecycle."""

    @pytest.fixture
    def mock_client(self) -> AsyncMock:
        """Create a mock MoonrakerClient."""
        client = AsyncMock()
        client.start = AsyncMock()
        client.stop = AsyncMock()
        client.fetch_printer_state = AsyncMock(return_value={})
        return client

    @pytest.fixture
    def backend(self, mock_client: AsyncMock) -> MoonrakerBackend:
        """Create a MoonrakerBackend with mocked client."""
        mock_config = MagicMock()
        mock_config.url = "http://localhost:7125"
        return MoonrakerBackend(mock_config, client=mock_client)

    @pytest.mark.asyncio
    async def test_start_calls_client_start(
        self, backend: MoonrakerBackend, mock_client: AsyncMock
    ) -> None:
        """Start should call the underlying client's start method."""
        callback = AsyncMock()
        await backend.start(callback)

        mock_client.start.assert_called_once()
        assert backend._started is True

    @pytest.mark.asyncio
    async def test_start_twice_logs_warning(
        self, backend: MoonrakerBackend, mock_client: AsyncMock
    ) -> None:
        """Starting twice should log a warning and not call start again."""
        callback = AsyncMock()
        await backend.start(callback)
        await backend.start(callback)

        # Should only be called once
        assert mock_client.start.call_count == 1

    @pytest.mark.asyncio
    async def test_stop_calls_client_stop(
        self, backend: MoonrakerBackend, mock_client: AsyncMock
    ) -> None:
        """Stop should call the underlying client's stop method."""
        callback = AsyncMock()
        await backend.start(callback)
        await backend.stop()

        mock_client.stop.assert_called_once()
        assert backend._started is False

    @pytest.mark.asyncio
    async def test_stop_without_start(
        self, backend: MoonrakerBackend, mock_client: AsyncMock
    ) -> None:
        """Stop without start should be a no-op."""
        await backend.stop()
        mock_client.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_assess_health_connection_error(
        self, backend: MoonrakerBackend, mock_client: AsyncMock
    ) -> None:
        """Connection error during health check should return unhealthy."""
        mock_client.fetch_printer_state.side_effect = Exception("Connection refused")

        assessment = await backend.assess_health()

        assert assessment.healthy is False
        assert "connection error" in (assessment.detail or "")
        assert assessment.force_trip is False

    @pytest.mark.asyncio
    async def test_fetch_state_delegates_to_client(
        self, backend: MoonrakerBackend, mock_client: AsyncMock
    ) -> None:
        """fetch_state should delegate to the client."""
        expected = {"result": {"status": {"foo": "bar"}}}
        mock_client.fetch_printer_state.return_value = expected

        result = await backend.fetch_state({"foo": None})

        mock_client.fetch_printer_state.assert_called_once_with({"foo": None})
        assert result == expected


class TestHelperFunctions:
    """Tests for snapshot parsing helper functions."""

    def test_normalise_state_valid(self) -> None:
        """Valid state should be normalised to lowercase."""
        assert _normalise_state({"state": "Ready"}, "state") == "ready"
        assert _normalise_state({"state": "  PRINTING  "}, "state") == "printing"

    def test_normalise_state_empty(self) -> None:
        """Empty state should return None."""
        assert _normalise_state({"state": ""}, "state") is None
        assert _normalise_state({"state": "   "}, "state") is None

    def test_normalise_state_not_dict(self) -> None:
        """Non-dict node should return None."""
        assert _normalise_state("not a dict", "state") is None
        assert _normalise_state(None, "state") is None

    def test_normalise_state_missing_field(self) -> None:
        """Missing field should return None."""
        assert _normalise_state({"other": "value"}, "state") is None

    def test_extract_str_valid(self) -> None:
        """Valid string should be extracted."""
        assert _extract_str({"message": "Hello"}, "message") == "Hello"
        assert _extract_str({"message": "  trimmed  "}, "message") == "trimmed"

    def test_extract_str_empty(self) -> None:
        """Empty string should return None."""
        assert _extract_str({"message": ""}, "message") is None
        assert _extract_str({"message": "   "}, "message") is None

    def test_extract_str_not_dict(self) -> None:
        """Non-dict node should return None."""
        assert _extract_str("not a dict", "message") is None

    def test_extract_bool_true(self) -> None:
        """True boolean should be extracted."""
        assert _extract_bool({"is_shutdown": True}, "is_shutdown") is True

    def test_extract_bool_false(self) -> None:
        """False boolean should be extracted."""
        assert _extract_bool({"is_shutdown": False}, "is_shutdown") is False

    def test_extract_bool_not_dict(self) -> None:
        """Non-dict node should return False."""
        assert _extract_bool("not a dict", "is_shutdown") is False

    def test_extract_bool_non_bool_value(self) -> None:
        """Non-boolean value should return False."""
        assert _extract_bool({"is_shutdown": "true"}, "is_shutdown") is False
        assert _extract_bool({"is_shutdown": 1}, "is_shutdown") is False


class TestFactoryMethods:
    """Tests for component factory methods."""

    @pytest.fixture
    def backend(self) -> MoonrakerBackend:
        """Create a MoonrakerBackend with mocked client."""
        mock_config = MagicMock()
        mock_config.url = "http://localhost:7125"
        mock_client = AsyncMock()
        return MoonrakerBackend(mock_config, client=mock_client)

    def test_create_telemetry_publisher(self, backend: MoonrakerBackend) -> None:
        """create_telemetry_publisher should return a TelemetryPublisher."""
        mock_owl_config = MagicMock()
        mock_mqtt = MagicMock()

        with patch(
            "moonraker_owl.backends.moonraker.TelemetryPublisher"
        ) as MockPublisher:
            result = backend.create_telemetry_publisher(mock_owl_config, mock_mqtt)

            MockPublisher.assert_called_once_with(
                mock_owl_config, backend._client, mock_mqtt, job_registry=None
            )

    @pytest.mark.asyncio
    async def test_create_command_processor(self, backend: MoonrakerBackend) -> None:
        """create_command_processor should return a CommandProcessor."""
        mock_owl_config = MagicMock()
        # Configure camera as disabled to avoid auto-discovery path
        mock_owl_config.camera.enabled = False
        mock_mqtt = MagicMock()
        mock_telemetry = MagicMock()

        with patch(
            "moonraker_owl.backends.moonraker.CommandProcessor"
        ) as MockProcessor, patch(
            "moonraker_owl.backends.moonraker.S3UploadClient"
        ) as MockS3Client:
            result = await backend.create_command_processor(
                mock_owl_config, mock_mqtt, mock_telemetry
            )

            MockProcessor.assert_called_once_with(
                mock_owl_config,
                backend._client,
                mock_mqtt,
                telemetry=mock_telemetry,
                s3_upload=MockS3Client.return_value,
                camera=None,
                image_preprocessor=None,
                job_registry=None,
            )

    @pytest.mark.asyncio
    async def test_create_command_processor_with_auto_discovery(
        self, backend: MoonrakerBackend
    ) -> None:
        """create_command_processor should use CameraDiscovery when snapshot_url is 'auto'."""
        mock_owl_config = MagicMock()
        mock_owl_config.camera.enabled = True
        mock_owl_config.camera.snapshot_url = "auto"
        mock_owl_config.camera.camera_name = "auto"
        mock_owl_config.camera.capture_timeout_seconds = 5
        mock_owl_config.camera.max_retries = 3
        mock_owl_config.camera.preprocess_enabled = False
        mock_mqtt = MagicMock()
        mock_telemetry = MagicMock()

        with patch(
            "moonraker_owl.backends.moonraker.CommandProcessor"
        ) as MockProcessor, patch(
            "moonraker_owl.backends.moonraker.S3UploadClient"
        ), patch(
            "moonraker_owl.backends.moonraker.CameraClient"
        ) as MockCameraClient, patch(
            "moonraker_owl.backends.moonraker.CameraDiscovery"
        ) as MockCameraDiscovery:
            # Configure mock discovery to return a URL
            mock_discovery_instance = MockCameraDiscovery.return_value
            mock_discovery_instance.discover_snapshot_url = AsyncMock(
                return_value="http://localhost:8080/snapshot"
            )

            await backend.create_command_processor(
                mock_owl_config, mock_mqtt, mock_telemetry
            )

            # Verify CameraDiscovery was created and called
            MockCameraDiscovery.assert_called_once()
            # camera_name is passed through as-is (defaults to "auto" for auto-selection)
            mock_discovery_instance.discover_snapshot_url.assert_called_once_with(
                camera_name="auto"
            )

            # Verify CameraClient was created with the discovered URL
            MockCameraClient.assert_called_once_with(
                snapshot_url="http://localhost:8080/snapshot",
                timeout=5,
                max_retries=3,
            )

    @pytest.mark.asyncio
    async def test_create_command_processor_with_explicit_url(
        self, backend: MoonrakerBackend
    ) -> None:
        """create_command_processor should use explicit snapshot_url without auto-discovery."""
        mock_owl_config = MagicMock()
        mock_owl_config.camera.enabled = True
        mock_owl_config.camera.snapshot_url = "http://custom:8080/snap"
        mock_owl_config.camera.capture_timeout_seconds = 5
        mock_owl_config.camera.max_retries = 3
        mock_owl_config.camera.preprocess_enabled = False
        mock_mqtt = MagicMock()
        mock_telemetry = MagicMock()

        with patch(
            "moonraker_owl.backends.moonraker.CommandProcessor"
        ), patch(
            "moonraker_owl.backends.moonraker.S3UploadClient"
        ), patch(
            "moonraker_owl.backends.moonraker.CameraClient"
        ) as MockCameraClient, patch(
            "moonraker_owl.backends.moonraker.CameraDiscovery"
        ) as MockCameraDiscovery:
            await backend.create_command_processor(
                mock_owl_config, mock_mqtt, mock_telemetry
            )

            # Verify CameraDiscovery was NOT called
            MockCameraDiscovery.assert_not_called()

            # Verify CameraClient was created with explicit URL
            MockCameraClient.assert_called_once_with(
                snapshot_url="http://custom:8080/snap",
                timeout=5,
                max_retries=3,
            )

    def test_invalidate_camera_cache(self, backend: MoonrakerBackend) -> None:
        """invalidate_camera_cache should call discovery.invalidate() if discovery exists."""
        from moonraker_owl.adapters.camera_discovery import CameraDiscovery

        with patch.object(CameraDiscovery, "invalidate") as mock_invalidate:
            # First, no discovery exists
            backend.invalidate_camera_cache()
            mock_invalidate.assert_not_called()

            # Now set up a discovery instance
            backend._camera_discovery = MagicMock()
            backend.invalidate_camera_cache()
            backend._camera_discovery.invalidate.assert_called_once()
