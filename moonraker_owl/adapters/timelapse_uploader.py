"""Agent-Initiated timelapse upload via HTTP.

Replaces the Cloud-Initiated Outbox command flow (task:upload-timelapse).
The agent detects timelapse readiness locally, requests presigned URLs from
PrinterService via HTTP, uploads to S3, and confirms completion.

This module reuses the same HTTP + Device JWT pattern as CloudConfigManager
and the same S3UploadClient used by the command processor.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

import aiohttp

from .s3_upload import S3UploadClient, UploadResult

LOGGER = logging.getLogger(__name__)

# Retry configuration
MAX_UPLOAD_RETRIES = 3
RETRY_BASE_SECONDS = 30  # 30s → 60s → 120s

# HTTP request timeout
_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30.0)


class TierGatingError(Exception):
    """Raised when the user's tier prevents timelapse upload (403)."""


class NoJobMatchError(Exception):
    """Raised when no matching print job is found (404 from cloud)."""


@dataclass(slots=True)
class UploadAuthorization:
    """Response from the initiate-upload endpoint."""

    upload_id: str
    video_upload_url: str
    preview_upload_url: Optional[str]
    print_job_id: Optional[str]
    expires_at_utc: str


class TimelapseUploader:
    """Handles timelapse upload via Agent-Initiated HTTP flow.

    Replaces the existing command-based upload flow (task:upload-timelapse).
    Uses the same HTTP infrastructure as CloudConfigManager.

    Lifecycle:
        1. Agent detects timelapse ready (render:success or polling)
        2. POST /api/v1/devices/{id}/timelapse/upload → presigned URLs
        3. Fetch video from Moonraker → upload to S3 via presigned URL
        4. POST /api/v1/devices/{id}/timelapse/confirm → done

    Each retry requests fresh presigned URLs, eliminating URL expiry issues.
    """

    def __init__(
        self,
        *,
        device_id: str,
        base_url: str,
        token_provider: Callable[[], Optional[str]],
        s3_client: S3UploadClient,
        moonraker_fetcher: Callable[..., Any],
        http_session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        """Initialize the timelapse uploader.

        Args:
            device_id: Device UUID for API routing.
            base_url: PrinterService base URL (e.g., "https://api.owl.example.com").
            token_provider: Callable returning current Device JWT or None.
            s3_client: S3 upload client for presigned URL uploads.
            moonraker_fetcher: Async callable to fetch timelapse files from Moonraker.
                Signature: async (filename: str, timeout: float) -> Optional[bytes]
            http_session: Optional shared aiohttp session. If None, creates one.
        """
        self._device_id = device_id
        self._base_url = base_url.rstrip("/")
        self._token_provider = token_provider
        self._s3_client = s3_client
        self._moonraker_fetcher = moonraker_fetcher
        self._session = http_session
        self._owns_session = http_session is None
        self._stopped = False
        self._active_tasks: set[asyncio.Task[None]] = set()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=_HTTP_TIMEOUT)
            self._owns_session = True
        return self._session

    async def stop(self) -> None:
        """Cancel active uploads and close resources."""
        self._stopped = True
        for task in list(self._active_tasks):
            task.cancel()
        if self._active_tasks:
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
        self._active_tasks.clear()
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    def schedule_upload(
        self,
        *,
        file_name: str,
        file_size_bytes: Optional[int] = None,
        moonraker_job_id: Optional[str] = None,
        print_start_time: Optional[float] = None,
        print_end_time: Optional[float] = None,
        has_preview: bool = False,
        preview_filename: Optional[str] = None,
    ) -> None:
        """Schedule a timelapse upload as a background task.

        This is fire-and-forget from the caller's perspective. The upload
        runs with internal retries and doesn't block the telemetry pipeline.
        """
        if self._stopped:
            LOGGER.debug("TimelapseUploader stopped; ignoring upload request for %s", file_name)
            return

        task = asyncio.create_task(
            self._upload_with_retry(
                file_name=file_name,
                file_size_bytes=file_size_bytes,
                moonraker_job_id=moonraker_job_id,
                print_start_time=print_start_time,
                print_end_time=print_end_time,
                has_preview=has_preview,
                preview_filename=preview_filename,
            )
        )
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def _upload_with_retry(
        self,
        *,
        file_name: str,
        file_size_bytes: Optional[int],
        moonraker_job_id: Optional[str],
        print_start_time: Optional[float],
        print_end_time: Optional[float],
        has_preview: bool,
        preview_filename: Optional[str],
    ) -> bool:
        """Execute upload with exponential backoff retry.

        Each retry requests fresh presigned URLs, eliminating URL expiry issues.
        Returns True if upload succeeded, False otherwise.
        """
        auth: Optional[UploadAuthorization] = None

        for attempt in range(MAX_UPLOAD_RETRIES + 1):
            if self._stopped:
                return False
            try:
                # Step 1: Request upload authorization (fresh presigned URL each attempt)
                auth = await self._request_upload_auth(
                    file_name=file_name,
                    file_size_bytes=file_size_bytes,
                    moonraker_job_id=moonraker_job_id,
                    print_start_time=print_start_time,
                    print_end_time=print_end_time,
                    has_preview=has_preview,
                )

                # Step 2: Fetch video from Moonraker
                video_data = await self._fetch_video(file_name)
                if video_data is None:
                    # File not found on printer — confirm failure and stop
                    LOGGER.warning("Timelapse video not found on printer: %s", file_name)
                    await self._confirm_upload(
                        auth.upload_id, success=False, error=f"Video not found: {file_name}"
                    )
                    return False

                # Step 3: Upload video to S3
                video_size_mb = len(video_data) / (1024 * 1024)
                video_timeout = max(120.0, video_size_mb * 5 + 30)
                video_result = await self._s3_client.upload(
                    presigned_url=auth.video_upload_url,
                    data=video_data,
                    s3_key=f"timelapse-video-{auth.upload_id}",
                    content_type="video/mp4",
                    timeout_override=video_timeout,
                )

                if not video_result.success:
                    raise RuntimeError(
                        f"S3 upload failed: {video_result.error_message}"
                    )

                # Step 4: Upload preview image (optional, non-fatal)
                preview_size_bytes: Optional[int] = None
                if has_preview and auth.preview_upload_url and preview_filename:
                    preview_size_bytes = await self._upload_preview(
                        auth.preview_upload_url, preview_filename
                    )

                # Step 5: Confirm upload success
                await self._confirm_upload(
                    auth.upload_id,
                    success=True,
                    video_size_bytes=video_result.file_size_bytes,
                    preview_size_bytes=preview_size_bytes,
                )

                LOGGER.info(
                    "Timelapse uploaded successfully: %s (%.2f MB, job=%s)",
                    file_name,
                    video_size_mb,
                    auth.print_job_id or "none",
                )
                return True

            except _AlreadyUploadedError:
                return True

            except TierGatingError:
                LOGGER.info("Timelapse upload gated by tier — skipping")
                return False

            except NoJobMatchError:
                if attempt < MAX_UPLOAD_RETRIES:
                    delay = RETRY_BASE_SECONDS * (2 ** attempt)
                    LOGGER.info(
                        "No matching print job found (attempt %d/%d), retrying in %ds",
                        attempt + 1, MAX_UPLOAD_RETRIES + 1, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    LOGGER.warning(
                        "Timelapse upload abandoned: no matching print job after %d attempts",
                        MAX_UPLOAD_RETRIES + 1,
                    )
                    return False

            except asyncio.CancelledError:
                LOGGER.debug("Timelapse upload cancelled for %s", file_name)
                raise

            except Exception as exc:
                if attempt < MAX_UPLOAD_RETRIES:
                    delay = RETRY_BASE_SECONDS * (2 ** attempt)
                    LOGGER.warning(
                        "Timelapse upload attempt %d/%d failed, retrying in %ds: %s",
                        attempt + 1, MAX_UPLOAD_RETRIES + 1, delay, exc,
                    )
                    await asyncio.sleep(delay)
                else:
                    LOGGER.error(
                        "Timelapse upload failed after %d attempts: %s",
                        MAX_UPLOAD_RETRIES + 1, exc,
                    )
                    # Confirm failure so cloud can update status
                    if auth is not None:
                        await self._confirm_upload(
                            auth.upload_id, success=False, error=str(exc)
                        )
                    return False

        return False  # pragma: no cover — shouldn't reach here

    # ------------------------------------------------------------------
    # HTTP API calls
    # ------------------------------------------------------------------

    async def _request_upload_auth(
        self,
        *,
        file_name: str,
        file_size_bytes: Optional[int],
        moonraker_job_id: Optional[str],
        print_start_time: Optional[float],
        print_end_time: Optional[float],
        has_preview: bool,
    ) -> UploadAuthorization:
        """POST /api/v1/devices/{id}/timelapse/upload — request presigned URLs."""
        token = self._token_provider()
        if not token:
            raise RuntimeError("No JWT token available for timelapse upload")

        url = f"{self._base_url}/api/v1/devices/{self._device_id}/timelapse/upload"
        headers = {"Authorization": f"Bearer {token}"}

        body: Dict[str, Any] = {
            "fileName": file_name,
            "hasPreviewImage": has_preview,
        }
        if file_size_bytes is not None:
            body["fileSizeBytes"] = file_size_bytes
        if moonraker_job_id is not None:
            body["moonrakerJobId"] = moonraker_job_id
        if print_start_time is not None:
            body["printStartTime"] = datetime.fromtimestamp(
                print_start_time, tz=timezone.utc
            ).isoformat()
        if print_end_time is not None:
            body["printEndTime"] = datetime.fromtimestamp(
                print_end_time, tz=timezone.utc
            ).isoformat()

        session = await self._ensure_session()

        async with session.post(url, json=body, headers=headers) as resp:
            if resp.status == 403:
                raise TierGatingError(await resp.text())
            if resp.status == 404:
                raise NoJobMatchError(await resp.text())
            if resp.status == 409:
                # Already uploaded — treat as success (idempotent)
                LOGGER.info("Timelapse already uploaded (409 Conflict)")
                raise _AlreadyUploadedError()
            if resp.status == 413:
                raise RuntimeError(f"File too large: {await resp.text()}")
            if resp.status != 200:
                detail = await resp.text()
                raise RuntimeError(
                    f"Initiate upload failed (HTTP {resp.status}): {detail}"
                )

            data = await resp.json()

        return UploadAuthorization(
            upload_id=data["uploadId"],
            video_upload_url=data["videoUploadUrl"],
            preview_upload_url=data.get("previewUploadUrl"),
            print_job_id=data.get("printJobId"),
            expires_at_utc=data.get("expiresAtUtc", ""),
        )

    async def _confirm_upload(
        self,
        upload_id: str,
        *,
        success: bool,
        video_size_bytes: Optional[int] = None,
        preview_size_bytes: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        """POST /api/v1/devices/{id}/timelapse/confirm — confirm completion."""
        token = self._token_provider()
        if not token:
            LOGGER.warning("No JWT token for timelapse confirm — skipping")
            return

        url = f"{self._base_url}/api/v1/devices/{self._device_id}/timelapse/confirm"
        headers = {"Authorization": f"Bearer {token}"}

        body: Dict[str, Any] = {
            "uploadId": upload_id,
            "success": success,
        }
        if video_size_bytes is not None:
            body["videoSizeBytes"] = video_size_bytes
        if preview_size_bytes is not None:
            body["previewSizeBytes"] = preview_size_bytes
        if error is not None:
            body["error"] = error[:2048]  # Match server-side max length

        session = await self._ensure_session()

        try:
            async with session.post(url, json=body, headers=headers) as resp:
                if resp.status != 200:
                    detail = await resp.text()
                    LOGGER.warning(
                        "Timelapse confirm failed (HTTP %d): %s", resp.status, detail,
                    )
                else:
                    LOGGER.debug("Timelapse confirm success for upload %s", upload_id)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            LOGGER.warning("Timelapse confirm request failed: %s", exc)

    # ------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------

    async def _fetch_video(self, filename: str) -> Optional[bytes]:
        """Fetch timelapse video from Moonraker."""
        try:
            return await self._moonraker_fetcher(filename, 120.0)
        except (asyncio.TimeoutError, OSError) as exc:
            raise RuntimeError(f"Failed to fetch video from Moonraker: {exc}") from exc

    async def _upload_preview(
        self, presigned_url: str, preview_filename: str
    ) -> Optional[int]:
        """Fetch and upload preview image. Returns size in bytes or None on failure."""
        try:
            preview_data = await self._moonraker_fetcher(preview_filename, 30.0)
            if preview_data is None:
                LOGGER.debug("Preview image not found: %s", preview_filename)
                return None

            result = await self._s3_client.upload(
                presigned_url=presigned_url,
                data=preview_data,
                s3_key=f"timelapse-preview",
                content_type="image/jpeg",
            )

            if result.success:
                LOGGER.debug("Uploaded timelapse preview: %d bytes", result.file_size_bytes)
                return result.file_size_bytes
            else:
                LOGGER.warning("Failed to upload timelapse preview: %s", result.error_message)
                return None

        except Exception as exc:
            LOGGER.warning("Preview upload failed (non-fatal): %s", exc)
            return None


class _AlreadyUploadedError(Exception):
    """Internal: treat 409 as non-retryable success."""
