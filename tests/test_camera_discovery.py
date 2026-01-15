"""Tests for camera auto-discovery."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from moonraker_owl.adapters.camera_discovery import CameraDiscovery, WebcamInfo


@pytest.fixture
def discovery():
    """Create a CameraDiscovery instance for testing."""
    return CameraDiscovery("http://127.0.0.1:7125")


class TestCameraDiscovery:
    """Tests for CameraDiscovery class."""

    def test_resolve_absolute_url(self, discovery):
        """Absolute URLs should be returned unchanged."""
        url = "http://192.168.1.100:8080/snapshot"
        assert discovery._resolve_webcam_url(url) == url

    def test_resolve_relative_url_with_leading_slash(self, discovery):
        """Relative URLs with leading slash should use localhost:80."""
        url = "/webcam/?action=snapshot"
        assert discovery._resolve_webcam_url(url) == "http://127.0.0.1/webcam/?action=snapshot"

    def test_resolve_relative_url_without_leading_slash(self, discovery):
        """Relative URLs without leading slash should use localhost:80."""
        url = "webcam/?action=snapshot"
        assert discovery._resolve_webcam_url(url) == "http://127.0.0.1/webcam/?action=snapshot"

    def test_resolve_empty_url(self, discovery):
        """Empty URLs should return empty string."""
        assert discovery._resolve_webcam_url("") == ""

    def test_invalidate_clears_cache(self, discovery):
        """Invalidate should clear all cached data."""
        discovery._cached_webcams = [WebcamInfo(name="test", snapshot_url="http://test")]
        discovery._cached_snapshot_url = "http://test"
        discovery._discovery_attempted = True

        discovery.invalidate()

        assert discovery._cached_webcams is None
        assert discovery._cached_snapshot_url is None
        assert discovery._discovery_attempted is False


class TestWebcamInfoParsing:
    """Tests for parsing Moonraker webcam API responses."""

    @pytest.mark.asyncio
    async def test_parse_moonraker_webcam_response(self, discovery):
        """Should correctly parse Moonraker webcam list response."""
        mock_response = {
            "result": {
                "webcams": [
                    {
                        "name": "bed_cam",
                        "location": "printer",
                        "service": "mjpegstreamer",
                        "stream_url": "/webcam/?action=stream",
                        "snapshot_url": "/webcam/?action=snapshot",
                    },
                    {
                        "name": "nozzle_cam",
                        "location": "toolhead",
                        "service": "ustreamer",
                        "stream_url": "/webcam2/stream",
                        "snapshot_url": "/webcam2/snapshot",
                    },
                ]
            }
        }

        with patch.object(discovery, "_ensure_session") as mock_session:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value=mock_response)

            mock_session_obj = MagicMock()
            mock_session_obj.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp)))
            mock_session.return_value = mock_session_obj

            webcams = await discovery._fetch_webcams_from_moonraker()

        assert len(webcams) == 2
        assert webcams[0].name == "bed_cam"
        assert webcams[0].snapshot_url == "http://127.0.0.1/webcam/?action=snapshot"
        assert webcams[1].name == "nozzle_cam"
        assert webcams[1].snapshot_url == "http://127.0.0.1/webcam2/snapshot"

    @pytest.mark.asyncio
    async def test_skip_webcam_without_snapshot_url(self, discovery):
        """Should skip webcams that have no snapshot_url."""
        mock_response = {
            "result": {
                "webcams": [
                    {
                        "name": "stream_only",
                        "stream_url": "/webcam/?action=stream",
                        # No snapshot_url
                    },
                    {
                        "name": "with_snapshot",
                        "snapshot_url": "/webcam/?action=snapshot",
                    },
                ]
            }
        }

        with patch.object(discovery, "_ensure_session") as mock_session:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json = AsyncMock(return_value=mock_response)

            mock_session_obj = MagicMock()
            mock_session_obj.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp)))
            mock_session.return_value = mock_session_obj

            webcams = await discovery._fetch_webcams_from_moonraker()

        assert len(webcams) == 1
        assert webcams[0].name == "with_snapshot"


class TestDiscoveryFlow:
    """Tests for the full discovery flow."""

    @pytest.mark.asyncio
    async def test_discover_uses_cache(self, discovery):
        """Second call should use cached result."""
        discovery._cached_snapshot_url = "http://cached/snapshot"

        result = await discovery.discover_snapshot_url()

        assert result == "http://cached/snapshot"

    @pytest.mark.asyncio
    async def test_discover_returns_none_after_failed_attempt(self, discovery):
        """Should return None if discovery already attempted and failed."""
        discovery._discovery_attempted = True
        discovery._cached_snapshot_url = None

        result = await discovery.discover_snapshot_url()

        assert result is None

    @pytest.mark.asyncio
    async def test_select_camera_by_name(self, discovery):
        """Should select camera by name when specified."""
        discovery._cached_webcams = [
            WebcamInfo(name="cam1", snapshot_url="http://cam1/snapshot"),
            WebcamInfo(name="cam2", snapshot_url="http://cam2/snapshot"),
        ]

        with patch.object(discovery, "_probe_url", return_value=True):
            result = await discovery._discover_from_moonraker("cam2")

        assert result == "http://cam2/snapshot"

    @pytest.mark.asyncio
    async def test_fallback_to_first_if_name_not_found(self, discovery):
        """Should fall back to first camera if specified name not found."""
        discovery._cached_webcams = [
            WebcamInfo(name="cam1", snapshot_url="http://cam1/snapshot"),
        ]

        with patch.object(discovery, "_probe_url", return_value=True):
            result = await discovery._discover_from_moonraker("nonexistent")

        assert result == "http://cam1/snapshot"

    @pytest.mark.asyncio
    async def test_auto_selects_first_camera(self, discovery):
        """Auto mode should select first available camera."""
        discovery._cached_webcams = [
            WebcamInfo(name="first", snapshot_url="http://first/snapshot"),
            WebcamInfo(name="second", snapshot_url="http://second/snapshot"),
        ]

        with patch.object(discovery, "_probe_url", return_value=True):
            result = await discovery._discover_from_moonraker("auto")

        assert result == "http://first/snapshot"
