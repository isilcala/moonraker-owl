"""Tests for device metadata reporter (ADR-0032)."""

import asyncio
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from moonraker_owl.metadata import (
    CameraProvider,
    KlipperProvider,
    MetadataReporter,
    MetadataReporterConfig,
    MoonrakerProvider,
    SystemInfoProvider,
)


@pytest.fixture
def reporter_config() -> MetadataReporterConfig:
    """Create a test configuration for MetadataReporter."""
    return MetadataReporterConfig(
        api_base_url="http://localhost:5000",
        moonraker_url="http://127.0.0.1:7125",
        device_id="test-device-123",
        refresh_interval_seconds=60.0,  # Shorter for tests
        initial_retry_delay=0.1,  # Faster retries for tests
        max_retry_delay=0.5,
        request_timeout=5.0,
    )


def make_token_provider(token: Optional[str] = "test-jwt-token"):
    """Create a token provider function for testing."""
    return lambda: token


class TestSystemInfoProvider:
    """Tests for SystemInfoProvider."""

    @pytest.mark.asyncio
    async def test_detect_returns_system_info(self):
        """Should return system info including agent version."""
        provider = SystemInfoProvider()
        result = await provider.detect()

        assert "system" in result
        system = result["system"]
        assert "agentVersion" in system
        assert "os" in system
        assert "hostname" in system
        assert "pythonVersion" in system

    def test_provider_name(self):
        """Should have correct provider name."""
        provider = SystemInfoProvider()
        assert provider.name == "system"


class TestMoonrakerProvider:
    """Tests for MoonrakerProvider."""

    @pytest.fixture
    def provider(self):
        """Create a MoonrakerProvider for testing."""
        return MoonrakerProvider("http://127.0.0.1:7125")

    def test_provider_name(self, provider):
        """Should have correct provider name."""
        assert provider.name == "moonraker"

    @pytest.mark.asyncio
    async def test_detect_returns_moonraker_info(self, provider):
        """Should parse Moonraker server info correctly."""
        mock_response = {
            "result": {
                "moonraker_version": "v0.9.2-10-gtest",
                "components": [
                    "klippy_connection",
                    "database",
                    "file_manager",
                    "timelapse",
                    "spoolman",
                    "update_manager",
                ],
            }
        }

        with patch.object(provider, "_ensure_session") as mock_session:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value=mock_response)

            mock_session_obj = MagicMock()
            mock_context = AsyncMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_session_obj.get = MagicMock(return_value=mock_context)
            mock_session.return_value = mock_session_obj

            result = await provider.detect()

        assert "components" in result
        assert "moonraker" in result["components"]
        moonraker = result["components"]["moonraker"]
        assert moonraker["version"] == "v0.9.2-10-gtest"
        assert moonraker["domain"] == "service"

        # Should detect timelapse and spoolman plugins
        assert "timelapse" in result["components"]
        assert "spoolman" in result["components"]

    @pytest.mark.asyncio
    async def test_detect_handles_timeout(self, provider):
        """Should raise on timeout."""
        with patch.object(provider, "_ensure_session") as mock_session:
            mock_session_obj = MagicMock()
            mock_context = AsyncMock()
            mock_context.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError())
            mock_session_obj.get = MagicMock(return_value=mock_context)
            mock_session.return_value = mock_session_obj

            with pytest.raises(asyncio.TimeoutError):
                await provider.detect()

    @pytest.mark.asyncio
    async def test_detect_handles_http_error(self, provider):
        """Should raise on HTTP error."""
        with patch.object(provider, "_ensure_session") as mock_session:
            mock_session_obj = MagicMock()
            mock_context = AsyncMock()
            mock_context.__aenter__ = AsyncMock(
                side_effect=aiohttp.ClientError("Connection refused")
            )
            mock_session_obj.get = MagicMock(return_value=mock_context)
            mock_session.return_value = mock_session_obj

            with pytest.raises(aiohttp.ClientError):
                await provider.detect()


class TestKlipperProvider:
    """Tests for KlipperProvider."""

    @pytest.fixture
    def provider(self):
        """Create a KlipperProvider for testing."""
        return KlipperProvider("http://127.0.0.1:7125")

    def test_provider_name(self, provider):
        """Should have correct provider name."""
        assert provider.name == "klipper"

    @pytest.mark.asyncio
    async def test_detect_returns_klipper_info(self, provider):
        """Should parse Klipper printer info correctly."""
        printer_info_response = {
            "result": {
                "state": "ready",
                "software_version": "v0.12.0-100-gabcdef",
                "hostname": "voron350",
            }
        }
        objects_list_response = {
            "result": {
                "objects": [
                    "gcode_move",
                    "toolhead",
                    "extruder",
                    "heater_bed",
                    "exclude_object",
                    "input_shaper",
                    "bed_mesh",
                ]
            }
        }

        call_count = 0

        async def mock_get_impl(*args, **kwargs):
            nonlocal call_count
            mock_resp = AsyncMock()
            mock_resp.status = 200
            if call_count == 0:
                mock_resp.json = AsyncMock(return_value=printer_info_response)
            else:
                mock_resp.json = AsyncMock(return_value=objects_list_response)
            call_count += 1
            return mock_resp

        with patch.object(provider, "_ensure_session") as mock_session:
            mock_session_obj = MagicMock()
            mock_context = AsyncMock()
            mock_context.__aenter__ = mock_get_impl
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_session_obj.get = MagicMock(return_value=mock_context)
            mock_session.return_value = mock_session_obj

            result = await provider.detect()

        assert "components" in result
        assert "klipper" in result["components"]
        klipper = result["components"]["klipper"]
        assert klipper["version"] == "v0.12.0-100-gabcdef"
        assert klipper["domain"] == "firmware"
        assert "exclude_object" in klipper["features"]
        assert "input_shaper" in klipper["features"]
        assert "bed_mesh" in klipper["features"]

        # Should include hostname in system info
        assert "system" in result
        assert result["system"]["klipperHostname"] == "voron350"


class TestCameraProvider:
    """Tests for CameraProvider."""

    @pytest.fixture
    def provider(self):
        """Create a CameraProvider for testing."""
        return CameraProvider("http://127.0.0.1:7125")

    def test_provider_name(self, provider):
        """Should have correct provider name."""
        assert provider.name == "camera"

    @pytest.mark.asyncio
    async def test_detect_returns_camera_info(self, provider):
        """Should parse webcam list correctly."""
        mock_response = {
            "result": {
                "webcams": [
                    {
                        "name": "main",
                        "service": "mjpegstreamer",
                        "snapshot_url": "/webcam/?action=snapshot",
                        "stream_url": "/webcam/?action=stream",
                    },
                    {
                        "name": "nozzle",
                        "service": "ustreamer",
                        "snapshot_url": "/webcam2/snapshot",
                    },
                ]
            }
        }

        with patch.object(provider, "_ensure_session") as mock_session:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value=mock_response)

            mock_session_obj = MagicMock()
            mock_context = AsyncMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_session_obj.get = MagicMock(return_value=mock_context)
            mock_session.return_value = mock_session_obj

            result = await provider.detect()

        assert "components" in result
        # First camera named "main" becomes "camera.main"
        assert "camera.main" in result["components"]
        main_cam = result["components"]["camera.main"]
        assert main_cam["domain"] == "camera"
        assert main_cam["name"] == "main"
        assert main_cam["service"] == "mjpegstreamer"
        assert "snapshot" in main_cam["features"]
        assert "stream" in main_cam["features"]

        # Second camera
        assert "camera.nozzle" in result["components"]
        nozzle_cam = result["components"]["camera.nozzle"]
        assert nozzle_cam["name"] == "nozzle"
        assert "snapshot" in nozzle_cam["features"]
        assert "stream" not in nozzle_cam["features"]

    @pytest.mark.asyncio
    async def test_detect_handles_no_webcams(self, provider):
        """Should return empty when no webcams configured."""
        mock_response = {"result": {"webcams": []}}

        with patch.object(provider, "_ensure_session") as mock_session:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value=mock_response)

            mock_session_obj = MagicMock()
            mock_context = AsyncMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_session_obj.get = MagicMock(return_value=mock_context)
            mock_session.return_value = mock_session_obj

            result = await provider.detect()

        assert result == {}


class TestMetadataReporter:
    """Tests for MetadataReporter orchestration."""

    @pytest.mark.asyncio
    async def test_collect_metadata_aggregates_providers(self, reporter_config):
        """Should aggregate results from all providers."""
        reporter = MetadataReporter(
            config=reporter_config,
            token_provider=make_token_provider(),
        )

        # Mock all providers
        mock_system_result = {
            "system": {"agentVersion": "1.0.0", "os": "Linux"}
        }
        mock_moonraker_result = {
            "components": {
                "moonraker": {"domain": "service", "version": "v0.9.2"},
                "timelapse": {"domain": "plugin", "enabled": True},
            }
        }
        mock_klipper_result = {
            "components": {
                "klipper": {"domain": "firmware", "version": "v0.12.0"}
            },
            "system": {"klipperHostname": "voron"},
        }
        mock_camera_result = {
            "components": {
                "camera.main": {"domain": "camera", "name": "main"}
            }
        }

        reporter._initialize_providers()

        # Replace providers with mocks
        reporter._providers[0].detect = AsyncMock(return_value=mock_system_result)
        reporter._providers[1].detect = AsyncMock(return_value=mock_moonraker_result)
        reporter._providers[2].detect = AsyncMock(return_value=mock_klipper_result)
        reporter._providers[3].detect = AsyncMock(return_value=mock_camera_result)

        metadata = await reporter._collect_metadata()

        # Check system info is merged
        assert metadata["system"]["agentVersion"] == "1.0.0"
        assert metadata["system"]["os"] == "Linux"
        assert metadata["system"]["klipperHostname"] == "voron"

        # Check components are merged
        assert "moonraker" in metadata["components"]
        assert "klipper" in metadata["components"]
        assert "timelapse" in metadata["components"]
        assert "camera.main" in metadata["components"]

    @pytest.mark.asyncio
    async def test_collect_metadata_handles_provider_failure(self, reporter_config):
        """Should continue if one provider fails."""
        reporter = MetadataReporter(
            config=reporter_config,
            token_provider=make_token_provider(),
        )

        reporter._initialize_providers()

        # First provider succeeds
        reporter._providers[0].detect = AsyncMock(
            return_value={"system": {"agentVersion": "1.0.0"}}
        )
        # Second provider fails
        reporter._providers[1].detect = AsyncMock(
            side_effect=asyncio.TimeoutError("timeout")
        )
        # Third provider succeeds
        reporter._providers[2].detect = AsyncMock(
            return_value={"components": {"klipper": {"version": "v0.12.0"}}}
        )
        # Fourth provider succeeds
        reporter._providers[3].detect = AsyncMock(return_value={})

        metadata = await reporter._collect_metadata()

        # Should still have results from successful providers
        assert "agentVersion" in metadata["system"]
        assert "klipper" in metadata["components"]

    @pytest.mark.asyncio
    async def test_upload_metadata_uses_jwt_token(self, reporter_config):
        """Should include JWT token in Authorization header."""
        reporter = MetadataReporter(
            config=reporter_config,
            token_provider=make_token_provider("my-secret-token"),
        )

        metadata = {"system": {"agentVersion": "1.0.0"}, "components": {}}

        with patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = AsyncMock()
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_context = AsyncMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_session.put = MagicMock(return_value=mock_context)
            mock_session_class.return_value = mock_session

            reporter._session = mock_session

            await reporter._upload_metadata(metadata)

            # Verify the Authorization header
            call_kwargs = mock_session.put.call_args
            assert call_kwargs is not None
            headers = call_kwargs.kwargs.get("headers", {})
            assert headers["Authorization"] == "Bearer my-secret-token"

    @pytest.mark.asyncio
    async def test_upload_metadata_fails_without_token(self, reporter_config):
        """Should raise error when no token available."""
        reporter = MetadataReporter(
            config=reporter_config,
            token_provider=make_token_provider(None),  # No token
        )

        metadata = {"system": {}, "components": {}}

        with pytest.raises(RuntimeError, match="No JWT token"):
            await reporter._upload_metadata(metadata)

    @pytest.mark.asyncio
    async def test_upload_sets_has_timelapse_flag(self, reporter_config):
        """Should set hasTimelapse based on timelapse component."""
        reporter = MetadataReporter(
            config=reporter_config,
            token_provider=make_token_provider(),
        )

        metadata_with_timelapse = {
            "system": {},
            "components": {"timelapse": {"domain": "plugin", "enabled": True}},
        }
        metadata_without_timelapse = {"system": {}, "components": {}}

        with patch("aiohttp.ClientSession"):
            mock_session = AsyncMock()
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_context = AsyncMock()
            mock_context.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_context.__aexit__ = AsyncMock(return_value=None)
            mock_session.put = MagicMock(return_value=mock_context)

            reporter._session = mock_session

            # Test with timelapse
            await reporter._upload_metadata(metadata_with_timelapse)
            call_kwargs = mock_session.put.call_args
            payload = call_kwargs.kwargs.get("json", {})
            assert payload["hasTimelapse"] is True

            # Test without timelapse
            await reporter._upload_metadata(metadata_without_timelapse)
            call_kwargs = mock_session.put.call_args
            payload = call_kwargs.kwargs.get("json", {})
            assert payload["hasTimelapse"] is False

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self, reporter_config):
        """Should start and stop cleanly."""
        reporter = MetadataReporter(
            config=reporter_config,
            token_provider=make_token_provider(),
        )

        # Mock _report_once to avoid actual HTTP calls
        reporter._report_once = AsyncMock()

        await reporter.start()
        assert reporter.is_running

        # Give it a moment to run
        await asyncio.sleep(0.1)

        await reporter.stop()
        assert not reporter.is_running

    @pytest.mark.asyncio
    async def test_retry_with_exponential_backoff(self, reporter_config):
        """Should retry with exponential backoff on failure."""
        # Use very short delays for testing
        reporter_config.initial_retry_delay = 0.01
        reporter_config.max_retry_delay = 0.05
        reporter_config.max_retry_attempts = 3

        reporter = MetadataReporter(
            config=reporter_config,
            token_provider=make_token_provider(),
        )

        # Track retry delays
        call_count = 0

        async def failing_report():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Upload failed")

        reporter._report_once = failing_report

        await reporter._report_with_retry()

        # Should have retried max_retry_attempts times
        assert call_count == 3
