"""System task command handlers (image capture, gcode download)."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import tempfile
import time as _time
from datetime import datetime as dt
from datetime import timezone
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)


class TaskCommandsMixin:
    """Mixin providing system task command handlers."""

    async def _execute_capture_image(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Handle task:capture-image command.

        This command is sent by the server to request camera capture.
        The agent captures a snapshot from the webcam and uploads it
        to the presigned S3 URL.

        Uses a semaphore to prevent overlapping captures (camera is a
        shared resource) and enforces a minimum interval between captures
        to avoid hardware contention.

        Parameters:
            frameId (str): The capture frame ID for correlation
            uploadUrl (str): Presigned URL for uploading
            blobKey (str): S3 key for the captured image
            maxFileSizeBytes (int, optional): Maximum file size allowed
            allowedContentTypes (list, optional): Allowed MIME types
        """
        try:
            await asyncio.wait_for(
                self._capture_semaphore.acquire(), timeout=5.0
            )
        except asyncio.TimeoutError:
            return {
                "success": False,
                "frameId": (message.parameters or {}).get("frameId"),
                "errorCode": "capture_busy",
                "errorMessage": "Another capture is already in progress",
            }

        try:
            # Enforce minimum interval between captures
            now = _time.monotonic()
            if self._last_capture_time is not None:
                elapsed = now - self._last_capture_time
                if elapsed < self._min_capture_interval:
                    wait = self._min_capture_interval - elapsed
                    LOGGER.debug(
                        "Capture cooldown: waiting %.1fs before next capture", wait
                    )
                    await asyncio.sleep(wait)

            result = await self._execute_capture_image_inner(message)
            self._last_capture_time = _time.monotonic()
            return result
        finally:
            self._capture_semaphore.release()

    async def _execute_capture_image_inner(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Inner implementation of capture-image, called under semaphore."""
        if self._s3_upload is None:
            raise CommandProcessingError(
                "S3 upload client unavailable",
                code="upload_unavailable",
                command_id=message.command_id,
            )

        if self._camera is None:
            return {
                "success": False,
                "frameId": message.parameters.get("frameId") if message.parameters else None,
                "errorCode": "camera_unavailable",
                "errorMessage": "Camera capture is not configured or disabled",
            }

        params = message.parameters or {}

        frame_id = params.get("frameId")
        upload_url = params.get("uploadUrl")
        blob_key = params.get("blobKey")
        max_size_bytes = params.get("maxFileSizeBytes", 5 * 1024 * 1024)  # 5 MB default

        if not frame_id:
            raise CommandProcessingError(
                "frameId parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not upload_url or not isinstance(upload_url, str):
            raise CommandProcessingError(
                "uploadUrl parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not blob_key or not isinstance(blob_key, str):
            raise CommandProcessingError(
                "blobKey parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # Capture image from webcam
        capture_result = await self._camera.capture()

        if not capture_result.success:
            LOGGER.warning(
                "Camera capture failed for frame %s: %s",
                frame_id[:8] if isinstance(frame_id, str) else frame_id,
                capture_result.error_message,
            )
            return {
                "success": False,
                "frameId": frame_id,
                "errorCode": capture_result.error_code or "capture_failed",
                "errorMessage": capture_result.error_message,
            }

        image_data = capture_result.image_data
        content_type = capture_result.content_type or "image/jpeg"
        captured_at = capture_result.captured_at or dt.now(timezone.utc)

        # Preprocess image (resize/compress) if preprocessor is available
        original_size_bytes = len(image_data) if image_data else 0
        was_resized = False
        if image_data and self._image_preprocessor:
            preprocess_result = self._image_preprocessor.preprocess(
                image_data, content_type
            )
            image_data = preprocess_result.image_data
            content_type = preprocess_result.content_type
            was_resized = preprocess_result.was_resized
            if was_resized:
                LOGGER.debug(
                    "Image preprocessed for frame %s: %dx%d -> %dx%d, %d -> %d bytes",
                    frame_id[:8] if isinstance(frame_id, str) else frame_id,
                    preprocess_result.original_size[0],
                    preprocess_result.original_size[1],
                    preprocess_result.processed_size[0],
                    preprocess_result.processed_size[1],
                    preprocess_result.original_bytes,
                    preprocess_result.processed_bytes,
                )

        # Validate size after preprocessing
        if image_data and len(image_data) > max_size_bytes:
            return {
                "success": False,
                "frameId": frame_id,
                "errorCode": "image_too_large",
                "errorMessage": f"Image size {len(image_data)} exceeds max {max_size_bytes}",
            }

        # Upload to S3
        result = await self._s3_upload.upload(
            presigned_url=upload_url,
            data=image_data,
            s3_key=blob_key,
            content_type=content_type,
        )

        if result.success:
            LOGGER.info(
                "Captured and uploaded frame %s: %d bytes to %s%s",
                frame_id[:8] if isinstance(frame_id, str) else frame_id,
                result.file_size_bytes,
                blob_key,
                f" (resized from {original_size_bytes} bytes)" if was_resized else "",
            )
            response: Dict[str, Any] = {
                "success": True,
                "frameId": frame_id,
                "capturedAt": captured_at.isoformat(),
                "fileSizeBytes": result.file_size_bytes,
            }
            if was_resized:
                response["originalSizeBytes"] = original_size_bytes
                response["wasResized"] = True
            # Include image dimensions if available
            if capture_result.image_width is not None:
                response["imageWidth"] = capture_result.image_width
            if capture_result.image_height is not None:
                response["imageHeight"] = capture_result.image_height
            return response
        else:
            LOGGER.warning(
                "Failed to upload frame %s: %s",
                frame_id[:8] if isinstance(frame_id, str) else frame_id,
                result.error_message,
            )
            return {
                "success": False,
                "frameId": frame_id,
                "errorCode": result.error_code or "upload_failed",
                "errorMessage": result.error_message,
            }

    async def _execute_download_gcode(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Handle task:download-gcode command.

        Downloads a GCode file from a CDN/presigned URL, verifies its
        checksum, and uploads it to Moonraker's local file system.

        Parameters:
            transferId (str): Transfer ID for correlation
            downloadUrl (str): CDN or presigned URL for downloading
            fileName (str): Target filename on Moonraker
            fileSizeBytes (int): Expected file size for timeout calculation
            autoStart (bool): Whether to auto-start printing
            checksumSha256 (str, optional): Expected SHA-256 hex digest
        """
        import aiohttp as _aiohttp

        params = message.parameters or {}

        transfer_id = params.get("transferId")
        download_url = params.get("downloadUrl")
        file_name = params.get("fileName")
        file_size = params.get("fileSizeBytes", 0)
        auto_start = params.get("autoStart", True)
        checksum = params.get("checksumSha256")

        if not download_url or not isinstance(download_url, str):
            raise CommandProcessingError(
                "downloadUrl parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not file_name or not isinstance(file_name, str):
            raise CommandProcessingError(
                "fileName parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # Dynamic timeout: base 30s + file_size / 100KB/s (conservative)
        min_bandwidth_bps = 100 * 1024  # 100 KB/s minimum
        download_timeout = 30.0 + (file_size / min_bandwidth_bps if file_size > 0 else 60.0)

        LOGGER.info(
            "Downloading GCode file for transfer %s: %s (%d bytes, timeout=%.0fs)",
            transfer_id,
            file_name,
            file_size,
            download_timeout,
        )

        start_time = _time.monotonic()
        tmp_path: str | None = None

        try:
            # Stream download to temp file to avoid buffering large files in memory
            async with _aiohttp.ClientSession() as session:
                async with asyncio.timeout(download_timeout):
                    async with session.get(download_url) as response:
                        if response.status >= 400:
                            detail = await response.text()
                            return {
                                "success": False,
                                "transferId": transfer_id,
                                "error": "download_failed",
                                "errorMessage": f"HTTP {response.status}: {detail[:200]}",
                            }

                        # Write to temp file
                        tmp_fd = tempfile.NamedTemporaryFile(
                            suffix=".gcode", delete=False
                        )
                        tmp_path = tmp_fd.name
                        try:
                            sha256 = hashlib.sha256()
                            total_bytes = 0
                            with tmp_fd:
                                async for chunk in response.content.iter_chunked(64 * 1024):
                                    tmp_fd.write(chunk)
                                    sha256.update(chunk)
                                    total_bytes += len(chunk)
                        except Exception:
                            raise

            download_ms = int((_time.monotonic() - start_time) * 1000)

            LOGGER.info(
                "Downloaded %d bytes in %dms for transfer %s",
                total_bytes,
                download_ms,
                transfer_id,
            )

            # Verify checksum
            checksum_verified = False
            if checksum:
                computed = sha256.hexdigest()
                if computed.lower() != checksum.lower():
                    LOGGER.error(
                        "Checksum mismatch for transfer %s: expected %s, got %s",
                        transfer_id,
                        checksum[:16],
                        computed[:16],
                    )
                    return {
                        "success": False,
                        "transferId": transfer_id,
                        "error": "checksum_mismatch",
                        "errorMessage": f"SHA-256 mismatch: expected {checksum[:16]}..., got {computed[:16]}...",
                    }
                checksum_verified = True

            # Upload to Moonraker
            upload_timeout = 30.0 + (total_bytes / min_bandwidth_bps)
            local_path = await self._moonraker.upload_gcode_file(
                filename=file_name,
                file_path=tmp_path,
                timeout=upload_timeout,
            )

            LOGGER.info(
                "GCode transfer %s complete: %s uploaded to Moonraker as %s",
                transfer_id,
                file_name,
                local_path,
            )

            return {
                "success": True,
                "transferId": transfer_id,
                "localPath": local_path,
                "autoStart": auto_start,
                "downloadDurationMs": download_ms,
                "fileSizeBytes": total_bytes,
                "checksumVerified": checksum_verified,
            }

        except asyncio.TimeoutError:
            LOGGER.error(
                "GCode download timed out for transfer %s after %.0fs",
                transfer_id,
                download_timeout,
            )
            return {
                "success": False,
                "transferId": transfer_id,
                "error": "download_timeout",
                "errorMessage": f"Download timed out after {download_timeout:.0f}s",
            }
        except Exception as exc:
            LOGGER.error(
                "GCode download failed for transfer %s: %s",
                transfer_id,
                exc,
            )
            return {
                "success": False,
                "transferId": transfer_id,
                "error": "download_failed",
                "errorMessage": str(exc),
            }
        finally:
            # Clean up temp file
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    LOGGER.warning("Failed to clean up temp file: %s", tmp_path)
