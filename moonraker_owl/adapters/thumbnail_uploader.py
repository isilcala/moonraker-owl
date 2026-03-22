"""Agent-Initiated thumbnail upload via HTTP.

Replaces the Cloud-Initiated Outbox command flow (task:upload-thumbnail).
The agent detects thumbnail availability at print start, requests a presigned
URL from PrinterService via HTTP, uploads to S3, and confirms completion.

This module mirrors the TimelapseUploader pattern — same HTTP + Device JWT
infrastructure and S3UploadClient.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import aiohttp

from .s3_upload import S3UploadClient, detect_content_type

LOGGER = logging.getLogger(__name__)

# Retry configuration
MAX_UPLOAD_RETRIES = 3
RETRY_BASE_SECONDS = 10  # 10s → 20s → 40s (thumbnails are small, retry faster)

# HTTP request timeout
_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30.0)


class TierGatingError(Exception):
    """Raised when the user's tier prevents thumbnail upload (403)."""


class NoJobMatchError(Exception):
    """Raised when no matching print job is found (404 from cloud)."""


@dataclass(slots=True)
class ThumbnailUploadAuth:
    """Response from the initiate-upload endpoint."""

    upload_id: str
    upload_url: str
    print_job_id: Optional[str]
    expires_at_utc: str


class ThumbnailUploader:
    """Handles thumbnail upload via Agent-Initiated HTTP flow.

    Replaces the existing command-based upload flow (task:upload-thumbnail).

    Lifecycle:
        1. Agent detects thumbnail at printStarted (via GCode metadata)
        2. POST /api/v1/devices/{id}/thumbnail/upload → presigned URL
        3. Fetch thumbnail from Moonraker → upload to S3
        4. POST /api/v1/devices/{id}/thumbnail/confirm → done

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
        if self._s3_client is not None:
            await self._s3_client.close()

    def schedule_upload(
        self,
        *,
        relative_path: str,
        gcode_filename: str,
        content_type: str = "image/png",
        moonraker_job_id: Optional[str] = None,
    ) -> None:
        """Schedule a thumbnail upload as a background task.

        Fire-and-forget from the caller's perspective. The upload
        runs with internal retries and doesn't block the telemetry pipeline.
        """
        if self._stopped:
            LOGGER.debug("ThumbnailUploader stopped; ignoring upload for %s", gcode_filename)
            return

        task = asyncio.create_task(
            self._upload_with_retry(
                relative_path=relative_path,
                gcode_filename=gcode_filename,
                content_type=content_type,
                moonraker_job_id=moonraker_job_id,
            )
        )
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def _upload_with_retry(
        self,
        *,
        relative_path: str,
        gcode_filename: str,
        content_type: str,
        moonraker_job_id: Optional[str],
    ) -> bool:
        """Execute upload with exponential backoff retry.

        Each retry requests fresh presigned URLs, eliminating URL expiry issues.
        """
        auth: Optional[ThumbnailUploadAuth] = None

        for attempt in range(MAX_UPLOAD_RETRIES + 1):
            if self._stopped:
                return False
            try:
                # Step 1: Fetch thumbnail from Moonraker
                thumbnail_data = await self._moonraker_fetcher(
                    relative_path, gcode_filename=gcode_filename, timeout=30.0
                )
                if thumbnail_data is None:
                    LOGGER.warning("Thumbnail not found on printer: %s", relative_path)
                    return False

                file_size = len(thumbnail_data)

                # Step 2: Request upload authorization (fresh presigned URL each attempt)
                auth = await self._request_upload_auth(
                    gcode_filename=gcode_filename,
                    file_size_bytes=file_size,
                    content_type=content_type,
                    moonraker_job_id=moonraker_job_id,
                )

                # Step 3: Upload to S3
                result = await self._s3_client.upload(
                    presigned_url=auth.upload_url,
                    data=thumbnail_data,
                    s3_key=f"thumbnail-{auth.upload_id}",
                    content_type=content_type,
                )

                if not result.success:
                    raise RuntimeError(f"S3 upload failed: {result.error_message}")

                # Step 4: Confirm success
                await self._confirm_upload(
                    auth.upload_id,
                    success=True,
                    size_bytes=result.file_size_bytes,
                )

                LOGGER.info(
                    "Thumbnail uploaded: %s (%d bytes, job=%s)",
                    gcode_filename,
                    file_size,
                    auth.print_job_id or "none",
                )
                return True

            except _AlreadyUploadedError:
                return True

            except TierGatingError:
                LOGGER.info("Thumbnail upload gated by tier — skipping")
                return False

            except NoJobMatchError:
                if attempt < MAX_UPLOAD_RETRIES:
                    delay = RETRY_BASE_SECONDS * (2 ** attempt)
                    LOGGER.info(
                        "No matching print job (attempt %d/%d), retrying in %ds",
                        attempt + 1, MAX_UPLOAD_RETRIES + 1, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    LOGGER.warning(
                        "Thumbnail upload abandoned: no matching job after %d attempts",
                        MAX_UPLOAD_RETRIES + 1,
                    )
                    return False

            except asyncio.CancelledError:
                LOGGER.debug("Thumbnail upload cancelled for %s", gcode_filename)
                raise

            except Exception as exc:
                if attempt < MAX_UPLOAD_RETRIES:
                    delay = RETRY_BASE_SECONDS * (2 ** attempt)
                    LOGGER.warning(
                        "Thumbnail upload attempt %d/%d failed, retrying in %ds: %s",
                        attempt + 1, MAX_UPLOAD_RETRIES + 1, delay, exc,
                    )
                    await asyncio.sleep(delay)
                else:
                    LOGGER.error(
                        "Thumbnail upload failed after %d attempts: %s",
                        MAX_UPLOAD_RETRIES + 1, exc,
                    )
                    if auth is not None:
                        await self._confirm_upload(
                            auth.upload_id, success=False, error=str(exc)
                        )
                    return False

        return False  # pragma: no cover

    # ------------------------------------------------------------------
    # HTTP API calls
    # ------------------------------------------------------------------

    async def _request_upload_auth(
        self,
        *,
        gcode_filename: str,
        file_size_bytes: int,
        content_type: str,
        moonraker_job_id: Optional[str],
    ) -> ThumbnailUploadAuth:
        """POST /api/v1/devices/{id}/thumbnail/upload — request presigned URL."""
        token = self._token_provider()
        if not token:
            raise RuntimeError("No JWT token available for thumbnail upload")

        url = f"{self._base_url}/api/v1/devices/{self._device_id}/thumbnail/upload"
        headers = {"Authorization": f"Bearer {token}"}

        body: Dict[str, Any] = {
            "gcodeFileName": gcode_filename,
            "fileSizeBytes": file_size_bytes,
            "contentType": content_type,
        }
        if moonraker_job_id:
            body["moonrakerJobId"] = moonraker_job_id

        session = await self._ensure_session()
        async with session.post(url, json=body, headers=headers) as resp:
            if resp.status == 403:
                raise TierGatingError(await resp.text())
            if resp.status == 404:
                raise NoJobMatchError(await resp.text())
            if resp.status == 409:
                raise _AlreadyUploadedError()
            if resp.status >= 400:
                detail = await resp.text()
                raise RuntimeError(
                    f"Thumbnail upload initiation failed ({resp.status}): {detail}"
                )

            data = await resp.json()
            return ThumbnailUploadAuth(
                upload_id=data["uploadId"],
                upload_url=data["uploadUrl"],
                print_job_id=data.get("printJobId"),
                expires_at_utc=data.get("expiresAtUtc", ""),
            )

    async def _confirm_upload(
        self,
        upload_id: str,
        *,
        success: bool,
        size_bytes: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        """POST /api/v1/devices/{id}/thumbnail/confirm — confirm upload result."""
        token = self._token_provider()
        if not token:
            LOGGER.warning("No JWT token for thumbnail confirm — skipping")
            return

        url = f"{self._base_url}/api/v1/devices/{self._device_id}/thumbnail/confirm"
        headers = {"Authorization": f"Bearer {token}"}

        body: Dict[str, Any] = {
            "uploadId": upload_id,
            "success": success,
        }
        if size_bytes is not None:
            body["sizeBytes"] = size_bytes
        if error:
            body["error"] = error

        try:
            session = await self._ensure_session()
            async with session.post(url, json=body, headers=headers) as resp:
                if resp.status >= 400:
                    detail = await resp.text()
                    LOGGER.warning(
                        "Thumbnail confirm failed (%d): %s", resp.status, detail
                    )
        except Exception as exc:
            LOGGER.warning("Thumbnail confirm request failed: %s", exc)


class _AlreadyUploadedError(Exception):
    """Sentinel for 409 Conflict (thumbnail already uploaded)."""
