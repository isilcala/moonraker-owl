"""Tests for ThumbnailUploader (Agent-Initiated HTTP upload).

Mirrors test_timelapse_uploader.py — same FakeSession/FakeS3 infrastructure.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest

from moonraker_owl.adapters.s3_upload import UploadResult
from moonraker_owl.adapters.thumbnail_uploader import (
    MAX_UPLOAD_RETRIES,
    NoJobMatchError,
    TierGatingError,
    ThumbnailUploader,
    ThumbnailUploadAuth,
    _AlreadyUploadedError,
)


# ---------------------------------------------------------------------------
# Fake HTTP session (shared pattern with test_timelapse_uploader)
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
THUMBNAIL_DATA = b"\x89PNG" + b"\x00" * 512  # fake PNG data


def _make_initiate_response(
    upload_id: str = "upload-001",
    upload_url: str = "https://s3.example.com/thumb?sig=abc",
    print_job_id: Optional[str] = "pj-001",
) -> FakeResponse:
    data = {
        "uploadId": upload_id,
        "uploadUrl": upload_url,
        "printJobId": print_job_id,
        "expiresAtUtc": "2026-01-01T01:00:00Z",
    }
    return FakeResponse(status=200, _data=data)


def _make_confirm_response() -> FakeResponse:
    return FakeResponse(status=200, _data={"status": "ok"})


async def _fake_moonraker_fetch(
    relative_path: str, *, gcode_filename: str = "", timeout: float = 30.0,
) -> Optional[bytes]:
    """Mock Moonraker thumbnail fetch."""
    if "missing" in relative_path:
        return None
    return THUMBNAIL_DATA


def _make_uploader(
    session: FakeSession,
    s3: Optional[FakeS3Client] = None,
    moonraker_fetcher=None,
    token: str = "test-jwt",
) -> ThumbnailUploader:
    return ThumbnailUploader(
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
    """Full success: initiate → fetch from Moonraker → S3 upload → confirm."""
    session = FakeSession()
    s3 = FakeS3Client()
    session.responses = [_make_initiate_response(), _make_confirm_response()]

    uploader = _make_uploader(session, s3)
    result = await uploader._upload_with_retry(
        relative_path="/server/files/gcodes/.thumbs/benchy.png",
        gcode_filename="benchy.gcode",
        content_type="image/png",
        moonraker_job_id="mk-job-1",
    )

    assert result is True
    assert len(session.requests) == 2  # initiate + confirm
    assert len(s3.uploads) == 1

    # Verify initiate request
    initiate_req = session.requests[0]
    assert "/thumbnail/upload" in initiate_req["url"]
    assert initiate_req["json"]["gcodeFileName"] == "benchy.gcode"
    assert initiate_req["json"]["contentType"] == "image/png"
    assert initiate_req["headers"]["Authorization"] == "Bearer test-jwt"

    # Verify confirm request
    confirm_req = session.requests[1]
    assert "/thumbnail/confirm" in confirm_req["url"]
    assert confirm_req["json"]["success"] is True
    assert confirm_req["json"]["uploadId"] == "upload-001"

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
        relative_path="/thumbs/test.png",
        gcode_filename="test.gcode",
        content_type="image/png",
        moonraker_job_id=None,
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

    import moonraker_owl.adapters.thumbnail_uploader as mod
    original = mod.asyncio.sleep
    mod.asyncio.sleep = AsyncMock()
    try:
        result = await uploader._upload_with_retry(
            relative_path="/thumbs/test.png",
            gcode_filename="test.gcode",
            content_type="image/png",
            moonraker_job_id=None,
        )
    finally:
        mod.asyncio.sleep = original

    assert result is True
    assert len(session.requests) == 3  # 404 + initiate + confirm

    await uploader.stop()


@pytest.mark.asyncio
async def test_upload_no_job_match_exhausts_retries():
    """404 on all attempts → returns False."""
    session = FakeSession()
    session.responses = [
        FakeResponse(status=404, _text="No matching job")
        for _ in range(MAX_UPLOAD_RETRIES + 1)
    ]

    uploader = _make_uploader(session)

    import moonraker_owl.adapters.thumbnail_uploader as mod
    original = mod.asyncio.sleep
    mod.asyncio.sleep = AsyncMock()
    try:
        result = await uploader._upload_with_retry(
            relative_path="/thumbs/test.png",
            gcode_filename="test.gcode",
            content_type="image/png",
            moonraker_job_id=None,
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
        relative_path="/thumbs/test.png",
        gcode_filename="test.gcode",
        content_type="image/png",
        moonraker_job_id=None,
    )

    assert result is True
    assert len(session.requests) == 1

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: Thumbnail not found on printer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_thumbnail_not_found_returns_false():
    """Moonraker returns None → return False immediately."""
    session = FakeSession()

    uploader = _make_uploader(session)
    result = await uploader._upload_with_retry(
        relative_path="/thumbs/missing_file.png",  # "missing" triggers None return
        gcode_filename="test.gcode",
        content_type="image/png",
        moonraker_job_id=None,
    )

    assert result is False
    # No HTTP requests should have been made
    assert len(session.requests) == 0

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

    session.responses = [
        _make_initiate_response(upload_id="upload-001"),
        _make_initiate_response(upload_id="upload-002"),
        _make_confirm_response(),
    ]

    attempt_count = [0]
    s3_clients = [s3_fail, s3_succeed]

    class SwitchingS3:
        async def upload(self, **kwargs):
            idx = min(attempt_count[0], len(s3_clients) - 1)
            attempt_count[0] += 1
            return await s3_clients[idx].upload(**kwargs)

    uploader = _make_uploader(session, s3=SwitchingS3())

    import moonraker_owl.adapters.thumbnail_uploader as mod
    original = mod.asyncio.sleep
    mod.asyncio.sleep = AsyncMock()
    try:
        result = await uploader._upload_with_retry(
            relative_path="/thumbs/test.png",
            gcode_filename="test.gcode",
            content_type="image/png",
            moonraker_job_id=None,
        )
    finally:
        mod.asyncio.sleep = original

    assert result is True
    assert len(session.requests) == 3  # two initiates + one confirm

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: Confirm failure on exhausted retries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_confirms_failure_after_all_retries_exhausted():
    """When all retries fail, _confirm_upload is called with success=False."""
    session = FakeSession()
    s3 = FakeS3Client(success=False)

    session.responses = [
        _make_initiate_response(upload_id=f"upload-{i}")
        for i in range(MAX_UPLOAD_RETRIES + 1)
    ] + [_make_confirm_response()]  # confirm failure

    uploader = _make_uploader(session, s3)

    import moonraker_owl.adapters.thumbnail_uploader as mod
    original = mod.asyncio.sleep
    mod.asyncio.sleep = AsyncMock()
    try:
        result = await uploader._upload_with_retry(
            relative_path="/thumbs/test.png",
            gcode_filename="test.gcode",
            content_type="image/png",
            moonraker_job_id=None,
        )
    finally:
        mod.asyncio.sleep = original

    assert result is False
    # Last request should be the failure confirm
    last_req = session.requests[-1]
    assert "/thumbnail/confirm" in last_req["url"]
    assert last_req["json"]["success"] is False

    await uploader.stop()


# ---------------------------------------------------------------------------
# Tests: No JWT token
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_no_token_fails():
    """No JWT token → RuntimeError → exhausts retries."""
    session = FakeSession()

    uploader = _make_uploader(session, token=None)

    import moonraker_owl.adapters.thumbnail_uploader as mod
    original = mod.asyncio.sleep
    mod.asyncio.sleep = AsyncMock()
    try:
        result = await uploader._upload_with_retry(
            relative_path="/thumbs/test.png",
            gcode_filename="test.gcode",
            content_type="image/png",
            moonraker_job_id=None,
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
    uploader.schedule_upload(
        relative_path="/thumbs/test.png",
        gcode_filename="test.gcode",
    )

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

    uploader.schedule_upload(
        relative_path="/thumbs/test.png",
        gcode_filename="test.gcode",
    )
    await asyncio.sleep(0.1)

    assert len(session.requests) == 0


# ---------------------------------------------------------------------------
# Tests: stop() cancels active tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_cancels_active_uploads():
    """stop() cancels in-flight upload tasks."""
    session = FakeSession()
    session.responses = []  # No response so task blocks

    uploader = _make_uploader(session)
    uploader.schedule_upload(
        relative_path="/thumbs/test.png",
        gcode_filename="test.gcode",
    )
    await asyncio.sleep(0)

    await uploader.stop()
    assert len(uploader._active_tasks) == 0


# ---------------------------------------------------------------------------
# Tests: Request body includes moonrakerJobId when provided
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_initiate_request_includes_moonraker_job_id():
    """moonrakerJobId is included in the initiate body when provided."""
    session = FakeSession()
    s3 = FakeS3Client()
    session.responses = [_make_initiate_response(), _make_confirm_response()]

    uploader = _make_uploader(session, s3)
    await uploader._upload_with_retry(
        relative_path="/thumbs/test.png",
        gcode_filename="test.gcode",
        content_type="image/png",
        moonraker_job_id="mk-999",
    )

    body = session.requests[0]["json"]
    assert body["moonrakerJobId"] == "mk-999"

    await uploader.stop()


@pytest.mark.asyncio
async def test_initiate_request_omits_none_fields():
    """Optional fields should be absent when None, not null."""
    session = FakeSession()
    s3 = FakeS3Client()
    session.responses = [_make_initiate_response(), _make_confirm_response()]

    uploader = _make_uploader(session, s3)
    await uploader._upload_with_retry(
        relative_path="/thumbs/test.png",
        gcode_filename="test.gcode",
        content_type="image/png",
        moonraker_job_id=None,
    )

    body = session.requests[0]["json"]
    assert "moonrakerJobId" not in body

    await uploader.stop()
