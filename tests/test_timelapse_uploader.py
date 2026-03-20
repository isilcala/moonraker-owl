"""Tests for TimelapseUploader (Agent-Initiated HTTP upload)."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest

from moonraker_owl.adapters.s3_upload import S3UploadClient, UploadResult
from moonraker_owl.adapters.timelapse_uploader import (
    MAX_UPLOAD_RETRIES,
    NoJobMatchError,
    TierGatingError,
    TimelapseUploader,
    UploadAuthorization,
    _AlreadyUploadedError,
)


# ---------------------------------------------------------------------------
# Fake HTTP session
# ---------------------------------------------------------------------------


@dataclass
class FakeResponse:
    """Minimal aiohttp response stand-in."""

    status: int
    _data: Any = None
    _text: str = ""

    async def json(self) -> Any:
        return self._data

    async def text(self) -> str:
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class FakeSession:
    """Minimal aiohttp.ClientSession replacement."""

    def __init__(self) -> None:
        self.responses: List[FakeResponse] = []
        self.requests: List[Dict[str, Any]] = []
        self._index = 0
        self.closed = False

    def post(self, url: str, *, json: Any = None, headers: Any = None) -> FakeResponse:
        self.requests.append({"method": "POST", "url": url, "json": json, "headers": headers})
        if self._index < len(self.responses):
            resp = self.responses[self._index]
            self._index += 1
            return resp
        return FakeResponse(status=500, _text="No more responses configured")

    async def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# Fake S3 client
# ---------------------------------------------------------------------------


class FakeS3Client:
    """S3UploadClient replacement that records calls."""

    def __init__(self, *, success: bool = True) -> None:
        self._success = success
        self.uploads: List[Dict[str, Any]] = []

    async def upload(
        self,
        presigned_url: str,
        data: bytes,
        s3_key: str,
        content_type: str,
        timeout_override: Optional[float] = None,
    ) -> UploadResult:
        self.uploads.append({
            "presigned_url": presigned_url,
            "size": len(data),
            "s3_key": s3_key,
            "content_type": content_type,
        })
        if self._success:
            return UploadResult(
                success=True,
                s3_key=s3_key,
                file_size_bytes=len(data),
                uploaded_at="2026-01-01T00:00:00Z",
            )
        return UploadResult(
            success=False,
            s3_key=s3_key,
            file_size_bytes=0,
            error_code="S3Error",
            error_message="Upload failed",
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DEVICE_ID = "d1234567-abcd-1234-5678-abcdef012345"
BASE_URL = "https://api.owl.example.com"
VIDEO_DATA = b"\x00" * 1024  # 1KB fake video
PREVIEW_DATA = b"\xff\xd8" * 100  # fake JPEG


def _make_initiate_response(
    upload_id: str = "upload-001",
    video_url: str = "https://s3.example.com/video?sig=abc",
    preview_url: Optional[str] = "https://s3.example.com/preview?sig=def",
    print_job_id: Optional[str] = "pj-001",
) -> FakeResponse:
    data = {
        "uploadId": upload_id,
        "videoUploadUrl": video_url,
        "previewUploadUrl": preview_url,
        "printJobId": print_job_id,
        "expiresAtUtc": "2026-01-01T01:00:00Z",
    }
    return FakeResponse(status=200, _data=data)


def _make_confirm_response() -> FakeResponse:
    return FakeResponse(status=200, _data={"status": "ok"})


async def _fake_moonraker_fetch(filename: str, timeout: float = 120.0) -> Optional[bytes]:
    """Mock Moonraker file fetch."""
    if "missing" in filename:
        return None
    if "preview" in filename.lower() or filename.endswith(".jpg"):
        return PREVIEW_DATA
    return VIDEO_DATA


def _make_uploader(
    session: FakeSession,
    s3: Optional[FakeS3Client] = None,
    moonraker_fetcher=None,
    token: str = "test-jwt",
) -> TimelapseUploader:
    return TimelapseUploader(
        device_id=DEVICE_ID,
        base_url=BASE_URL,
        token_provider=lambda: token,
        s3_client=s3 or FakeS3Client(),
        moonraker_fetcher=moonraker_fetcher or _fake_moonraker_fetch,
        http_session=session,
    )


# ---------------------------------------------------------------------------
# Tests: Full success flow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_success_full_flow():
    """Full success: initiate → fetch → S3 upload → confirm."""
    session = FakeSession()
    s3 = FakeS3Client()
    session.responses = [_make_initiate_response(), _make_confirm_response()]

    uploader = _make_uploader(session, s3)
    result = await uploader._upload_with_retry(
        file_name="timelapse_001.mp4",
        file_size_bytes=1024,
        moonraker_job_id="mk-job-1",
        print_start_time=1700000000.0,
        print_end_time=1700003600.0,
        has_preview=True,
        preview_filename="timelapse_001.jpg",
    )

    assert result is True
    assert len(session.requests) == 2  # initiate + confirm
    assert len(s3.uploads) == 2  # video + preview

    # Verify initiate request
    initiate_req = session.requests[0]
    assert "/timelapse/upload" in initiate_req["url"]
    assert initiate_req["json"]["fileName"] == "timelapse_001.mp4"
    assert initiate_req["json"]["hasPreviewImage"] is True
    assert initiate_req["headers"]["Authorization"] == "Bearer test-jwt"

    # Verify confirm request
    confirm_req = session.requests[1]
    assert "/timelapse/confirm" in confirm_req["url"]
    assert confirm_req["json"]["success"] is True
    assert confirm_req["json"]["uploadId"] == "upload-001"

    await uploader.stop()


@pytest.mark.asyncio
async def test_upload_success_no_preview():
    """Success without preview image."""
    session = FakeSession()
    s3 = FakeS3Client()
    session.responses = [
        _make_initiate_response(preview_url=None),
        _make_confirm_response(),
    ]

    uploader = _make_uploader(session, s3)
    result = await uploader._upload_with_retry(
        file_name="timelapse_001.mp4",
        file_size_bytes=None,
        moonraker_job_id=None,
        print_start_time=None,
        print_end_time=None,
        has_preview=False,
        preview_filename=None,
    )

    assert result is True
    assert len(s3.uploads) == 1  # video only

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: Tier gating (403)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_tier_gating_returns_false():
    """403 tier gating → returns False immediately, no retry."""
    session = FakeSession()
    session.responses = [FakeResponse(status=403, _text="Tier gating")]

    uploader = _make_uploader(session)
    result = await uploader._upload_with_retry(
        file_name="timelapse_001.mp4",
        file_size_bytes=None,
        moonraker_job_id=None,
        print_start_time=None,
        print_end_time=None,
        has_preview=False,
        preview_filename=None,
    )

    assert result is False
    assert len(session.requests) == 1  # Only one attempt

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: No job match (404) with retry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_no_job_match_retries_then_succeeds():
    """404 → retry with backoff → succeed on second attempt."""
    session = FakeSession()
    s3 = FakeS3Client()
    session.responses = [
        FakeResponse(status=404, _text="No matching job"),
        _make_initiate_response(),
        _make_confirm_response(),
    ]

    uploader = _make_uploader(session, s3)

    # Monkey-patch sleep to avoid actual waiting
    original_sleep = asyncio.sleep
    sleep_calls = []

    async def fast_sleep(seconds):
        sleep_calls.append(seconds)
        await original_sleep(0)  # yield but don't wait

    import moonraker_owl.adapters.timelapse_uploader as mod
    original = mod.asyncio.sleep
    mod.asyncio.sleep = fast_sleep
    try:
        result = await uploader._upload_with_retry(
            file_name="timelapse_001.mp4",
            file_size_bytes=None,
            moonraker_job_id=None,
            print_start_time=None,
            print_end_time=None,
            has_preview=False,
            preview_filename=None,
        )
    finally:
        mod.asyncio.sleep = original

    assert result is True
    assert len(session.requests) == 3  # 404 + initiate + confirm
    assert len(sleep_calls) == 1  # One retry delay
    assert sleep_calls[0] == 30  # First retry: 30s base

    await uploader.stop()


@pytest.mark.asyncio
async def test_upload_no_job_match_exhausts_retries():
    """404 on all attempts → returns False."""
    session = FakeSession()
    # MAX_UPLOAD_RETRIES + 1 attempts, all 404
    session.responses = [
        FakeResponse(status=404, _text="No matching job")
        for _ in range(MAX_UPLOAD_RETRIES + 1)
    ]

    uploader = _make_uploader(session)

    import moonraker_owl.adapters.timelapse_uploader as mod
    original = mod.asyncio.sleep
    mod.asyncio.sleep = AsyncMock()
    try:
        result = await uploader._upload_with_retry(
            file_name="timelapse_001.mp4",
            file_size_bytes=None,
            moonraker_job_id=None,
            print_start_time=None,
            print_end_time=None,
            has_preview=False,
            preview_filename=None,
        )
    finally:
        mod.asyncio.sleep = original

    assert result is False
    assert len(session.requests) == MAX_UPLOAD_RETRIES + 1

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: Already uploaded (409)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_already_uploaded_returns_true():
    """409 Conflict → treated as idempotent success."""
    session = FakeSession()
    session.responses = [FakeResponse(status=409, _text="Already uploaded")]

    uploader = _make_uploader(session)
    result = await uploader._upload_with_retry(
        file_name="timelapse_001.mp4",
        file_size_bytes=None,
        moonraker_job_id=None,
        print_start_time=None,
        print_end_time=None,
        has_preview=False,
        preview_filename=None,
    )

    assert result is True
    assert len(session.requests) == 1

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: Video not found on printer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_video_not_found_confirms_failure():
    """Moonraker returns None → confirm failure, no retry."""
    session = FakeSession()
    session.responses = [
        _make_initiate_response(),
        _make_confirm_response(),  # confirm failure
    ]

    uploader = _make_uploader(session, moonraker_fetcher=_fake_moonraker_fetch)
    result = await uploader._upload_with_retry(
        file_name="missing_video.mp4",  # "missing" triggers None return
        file_size_bytes=None,
        moonraker_job_id=None,
        print_start_time=None,
        print_end_time=None,
        has_preview=False,
        preview_filename=None,
    )

    assert result is False
    assert len(session.requests) == 2
    # Second request should be a failure confirmation
    confirm_req = session.requests[1]
    assert confirm_req["json"]["success"] is False
    assert "not found" in confirm_req["json"]["error"].lower()

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: S3 upload failure with retry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_s3_failure_retries():
    """S3 upload fails → retries with fresh presigned URL."""
    session = FakeSession()
    s3_fail = FakeS3Client(success=False)
    s3_succeed = FakeS3Client(success=True)

    # First attempt: initiate OK, S3 fails → retry
    # Second attempt: initiate OK, S3 succeeds → confirm
    session.responses = [
        _make_initiate_response(upload_id="upload-001"),
        _make_initiate_response(upload_id="upload-002"),
        _make_confirm_response(),
    ]

    # Track which S3 client to use
    attempt_count = [0]
    s3_clients = [s3_fail, s3_succeed]

    class SwitchingS3:
        async def upload(self, **kwargs):
            idx = min(attempt_count[0], len(s3_clients) - 1)
            attempt_count[0] += 1
            return await s3_clients[idx].upload(**kwargs)

    uploader = _make_uploader(session, s3=SwitchingS3())

    import moonraker_owl.adapters.timelapse_uploader as mod
    original = mod.asyncio.sleep
    mod.asyncio.sleep = AsyncMock()
    try:
        result = await uploader._upload_with_retry(
            file_name="timelapse_001.mp4",
            file_size_bytes=None,
            moonraker_job_id=None,
            print_start_time=None,
            print_end_time=None,
            has_preview=False,
            preview_filename=None,
        )
    finally:
        mod.asyncio.sleep = original

    assert result is True
    # Two initiate requests + one confirm
    assert len(session.requests) == 3

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: No JWT token
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_no_token_raises():
    """No JWT token → RuntimeError."""
    session = FakeSession()

    uploader = _make_uploader(session, token=None)

    import moonraker_owl.adapters.timelapse_uploader as mod
    original = mod.asyncio.sleep
    mod.asyncio.sleep = AsyncMock()
    try:
        result = await uploader._upload_with_retry(
            file_name="timelapse_001.mp4",
            file_size_bytes=None,
            moonraker_job_id=None,
            print_start_time=None,
            print_end_time=None,
            has_preview=False,
            preview_filename=None,
        )
    finally:
        mod.asyncio.sleep = original

    # Should fail after exhausting retries
    assert result is False

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: File too large (413)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_file_too_large():
    """413 response → fails after retries (not retriable in general but
    goes through the generic exception path)."""
    session = FakeSession()
    session.responses = [
        FakeResponse(status=413, _text="File too large")
        for _ in range(MAX_UPLOAD_RETRIES + 1)
    ]

    uploader = _make_uploader(session)

    import moonraker_owl.adapters.timelapse_uploader as mod
    original = mod.asyncio.sleep
    mod.asyncio.sleep = AsyncMock()
    try:
        result = await uploader._upload_with_retry(
            file_name="timelapse_001.mp4",
            file_size_bytes=None,
            moonraker_job_id=None,
            print_start_time=None,
            print_end_time=None,
            has_preview=False,
            preview_filename=None,
        )
    finally:
        mod.asyncio.sleep = original

    assert result is False

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: schedule_upload fire-and-forget
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schedule_upload_creates_background_task():
    """schedule_upload() creates a background task that completes."""
    session = FakeSession()
    s3 = FakeS3Client()
    session.responses = [_make_initiate_response(), _make_confirm_response()]

    uploader = _make_uploader(session, s3)
    uploader.schedule_upload(file_name="timelapse_001.mp4")

    # Let the background task run
    await asyncio.sleep(0.1)

    assert len(session.requests) == 2
    assert len(s3.uploads) == 1

    await uploader.stop()


@pytest.mark.asyncio
async def test_schedule_upload_ignored_when_stopped():
    """schedule_upload() after stop() does nothing."""
    session = FakeSession()
    uploader = _make_uploader(session)
    await uploader.stop()

    uploader.schedule_upload(file_name="timelapse_001.mp4")
    await asyncio.sleep(0.1)

    assert len(session.requests) == 0


# ---------------------------------------------------------------------------
# Tests: stop() cancels active tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_cancels_active_uploads():
    """stop() cancels in-flight upload tasks."""
    session = FakeSession()
    # Never return initiate response so the task blocks
    session.responses = []

    uploader = _make_uploader(session)
    uploader.schedule_upload(file_name="timelapse_001.mp4")
    await asyncio.sleep(0)  # Let task start

    await uploader.stop()
    assert len(uploader._active_tasks) == 0


# ---------------------------------------------------------------------------
# Tests: Request body serialization
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_initiate_request_includes_timestamps():
    """Verify timestamps are serialized as ISO 8601 UTC strings."""
    session = FakeSession()
    s3 = FakeS3Client()
    session.responses = [_make_initiate_response(), _make_confirm_response()]

    uploader = _make_uploader(session, s3)
    await uploader._upload_with_retry(
        file_name="test.mp4",
        file_size_bytes=2048,
        moonraker_job_id="mk-999",
        print_start_time=1700000000.0,
        print_end_time=1700003600.0,
        has_preview=False,
        preview_filename=None,
    )

    body = session.requests[0]["json"]
    assert body["moonrakerJobId"] == "mk-999"
    assert body["fileSizeBytes"] == 2048
    # Timestamps should be ISO strings, not floats
    assert isinstance(body["printStartTime"], str)
    assert "T" in body["printStartTime"]

    await uploader.stop()


@pytest.mark.asyncio
async def test_initiate_request_omits_none_fields():
    """Optional fields should be absent when None, not null."""
    session = FakeSession()
    s3 = FakeS3Client()
    session.responses = [_make_initiate_response(), _make_confirm_response()]

    uploader = _make_uploader(session, s3)
    await uploader._upload_with_retry(
        file_name="test.mp4",
        file_size_bytes=None,
        moonraker_job_id=None,
        print_start_time=None,
        print_end_time=None,
        has_preview=False,
        preview_filename=None,
    )

    body = session.requests[0]["json"]
    assert "fileSizeBytes" not in body
    assert "moonrakerJobId" not in body
    assert "printStartTime" not in body
    assert "printEndTime" not in body

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: Preview upload failure is non-fatal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_preview_upload_failure_nonfatal():
    """Preview upload failure doesn't fail the whole upload."""
    session = FakeSession()
    session.responses = [_make_initiate_response(), _make_confirm_response()]

    async def _fetch_with_preview_error(filename, timeout=120.0):
        if filename.endswith(".jpg"):
            raise OSError("Preview fetch failed")
        return VIDEO_DATA

    s3 = FakeS3Client()
    uploader = _make_uploader(session, s3, moonraker_fetcher=_fetch_with_preview_error)

    result = await uploader._upload_with_retry(
        file_name="timelapse_001.mp4",
        file_size_bytes=None,
        moonraker_job_id=None,
        print_start_time=None,
        print_end_time=None,
        has_preview=True,
        preview_filename="timelapse_001.jpg",
    )

    assert result is True
    assert len(s3.uploads) == 1  # Only video uploaded

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: Session ownership
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_does_not_close_external_session():
    """When http_session is provided, stop() should close it (owns=False initially,
    but our constructor sets owns_session based on whether it was provided)."""
    session = FakeSession()
    uploader = _make_uploader(session)
    # Session was provided externally, but our constructor sees it as not None
    # so _owns_session should be False
    assert uploader._owns_session is False
    await uploader.stop()
    assert session.closed is False  # External session not closed


@pytest.mark.asyncio
async def test_stop_closes_owned_session():
    """When no http_session provided, stop() closes the created session."""
    uploader = TimelapseUploader(
        device_id=DEVICE_ID,
        base_url=BASE_URL,
        token_provider=lambda: "jwt",
        s3_client=FakeS3Client(),
        moonraker_fetcher=_fake_moonraker_fetch,
        # No http_session → owns_session=True
    )
    assert uploader._owns_session is True
    # Don't actually use it; just verify cleanup doesn't crash
    await uploader.stop()
