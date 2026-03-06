"""System task command handlers (thumbnail upload, image capture, timelapse upload)."""

from __future__ import annotations

import asyncio
import logging
import time as _time
from datetime import datetime as dt
from datetime import timezone
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)


class TaskCommandsMixin:
    """Mixin providing system task command handlers."""

    async def _execute_upload_thumbnail(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Handle task:upload-thumbnail command.

        This command is sent by the server to request thumbnail upload.
        The agent fetches the thumbnail from Moonraker and uploads it
        to the presigned S3 URL.

        Parameters:
            jobId (str): The print job ID
            uploadUrl (str): Presigned URL for uploading
            thumbnailKey (str): S3 key for the thumbnail
            contentType (str): Expected content type (e.g., "image/png")
            maxSizeBytes (int, optional): Maximum file size allowed
            expiresAt (str, optional): When the presigned URL expires
        """
        if self._s3_upload is None:
            raise CommandProcessingError(
                "S3 upload client unavailable",
                code="upload_unavailable",
                command_id=message.command_id,
            )

        if self._telemetry is None:
            raise CommandProcessingError(
                "Telemetry publisher unavailable",
                code="telemetry_unavailable",
                command_id=message.command_id,
            )

        params = message.parameters or {}

        job_id = params.get("jobId")
        upload_url = params.get("uploadUrl")
        thumbnail_key = params.get("thumbnailKey")
        content_type = params.get("contentType", "image/png")
        max_size_bytes = params.get("maxSizeBytes", 5 * 1024 * 1024)  # 5 MB default

        if not upload_url or not isinstance(upload_url, str):
            raise CommandProcessingError(
                "uploadUrl parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not thumbnail_key or not isinstance(thumbnail_key, str):
            raise CommandProcessingError(
                "thumbnailKey parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        # Get current job thumbnail info from telemetry state
        thumbnail_info = self._telemetry.get_current_thumbnail_info()
        if thumbnail_info is None:
            LOGGER.warning(
                "No thumbnail available for job %s",
                job_id[:8] if job_id else "unknown",
            )
            return {
                "success": False,
                "jobId": job_id,
                "error": "no_thumbnail_available",
                "errorMessage": "No thumbnail path available from current print job",
            }

        relative_path = thumbnail_info.get("relative_path")
        gcode_filename = thumbnail_info.get("gcode_filename")

        if not relative_path:
            return {
                "success": False,
                "jobId": job_id,
                "error": "no_thumbnail_path",
                "errorMessage": "No thumbnail relative path available",
            }

        # Fetch thumbnail from Moonraker
        try:
            thumbnail_data = await self._moonraker.fetch_thumbnail(
                relative_path, gcode_filename=gcode_filename, timeout=30.0
            )
        except Exception as exc:
            LOGGER.error("Failed to fetch thumbnail: %s", exc)
            return {
                "success": False,
                "jobId": job_id,
                "error": "fetch_failed",
                "errorMessage": str(exc),
            }

        if thumbnail_data is None:
            return {
                "success": False,
                "jobId": job_id,
                "error": "thumbnail_not_found",
                "errorMessage": f"Thumbnail not found at {relative_path}",
            }

        # Validate size
        if len(thumbnail_data) > max_size_bytes:
            return {
                "success": False,
                "jobId": job_id,
                "error": "thumbnail_too_large",
                "errorMessage": f"Thumbnail size {len(thumbnail_data)} exceeds max {max_size_bytes}",
            }

        # Upload to S3
        result = await self._s3_upload.upload(
            presigned_url=upload_url,
            data=thumbnail_data,
            s3_key=thumbnail_key,
            content_type=content_type,
        )

        if result.success:
            LOGGER.info(
                "Uploaded thumbnail for job %s: %d bytes to %s",
                job_id[:8] if job_id else "unknown",
                result.file_size_bytes,
                thumbnail_key,
            )

            # Note: We no longer set thumbnailUrl in telemetry status.
            # The server will push the URL via SignalR after processing the ACK.

            return {
                "success": True,
                "jobId": job_id,
                "thumbnailKey": thumbnail_key,
                "sizeBytes": result.file_size_bytes,
            }
        else:
            LOGGER.warning(
                "Failed to upload thumbnail for job %s: %s",
                job_id[:8] if job_id else "unknown",
                result.error_message,
            )
            return {
                "success": False,
                "jobId": job_id,
                "error": result.error_code or "upload_failed",
                "errorMessage": result.error_message,
            }

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

    async def _execute_upload_timelapse(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Handle task:upload-timelapse command.

        This command is sent by the server to request timelapse video upload.
        The agent fetches the video and preview image from Moonraker's
        timelapse directory and uploads them to the presigned S3 URLs.

        Parameters:
            printJobId (str): The print job ID
            videoUploadUrl (str): Presigned URL for uploading video
            videoKey (str): S3 key for the video
            previewUploadUrl (str, optional): Presigned URL for uploading preview
            previewKey (str, optional): S3 key for the preview
            videoFilename (str): Video filename from timelapse event
            previewFilename (str, optional): Preview image filename
            maxVideoSizeBytes (int, optional): Maximum video size allowed
            maxPreviewSizeBytes (int, optional): Maximum preview size allowed
        """
        if self._s3_upload is None:
            raise CommandProcessingError(
                "S3 upload client unavailable",
                code="upload_unavailable",
                command_id=message.command_id,
            )

        params = message.parameters or {}

        print_job_id = params.get("printJobId")
        video_upload_url = params.get("videoUploadUrl")
        video_key = params.get("videoKey")
        video_filename = params.get("videoFilename")
        preview_upload_url = params.get("previewUploadUrl")
        preview_key = params.get("previewKey")
        preview_filename = params.get("previewFilename")
        max_video_size = params.get("maxVideoSizeBytes", 500 * 1024 * 1024)  # 500 MB default
        max_preview_size = params.get("maxPreviewSizeBytes", 5 * 1024 * 1024)  # 5 MB default

        # Validate required parameters
        if not video_upload_url or not isinstance(video_upload_url, str):
            raise CommandProcessingError(
                "videoUploadUrl parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not video_key or not isinstance(video_key, str):
            raise CommandProcessingError(
                "videoKey parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        if not video_filename or not isinstance(video_filename, str):
            raise CommandProcessingError(
                "videoFilename parameter is required",
                code="invalid_parameters",
                command_id=message.command_id,
            )

        result_data: Dict[str, Any] = {
            "printJobId": print_job_id,
        }

        # Fetch and upload video
        video_uploaded = False
        try:
            LOGGER.info(
                "Fetching timelapse video %s for job %s",
                video_filename,
                print_job_id[:8] if print_job_id else "unknown",
            )
            video_data = await self._moonraker.fetch_timelapse_file(
                video_filename, timeout=120.0
            )
        except Exception as exc:
            LOGGER.error("Failed to fetch timelapse video: %s", exc)
            return {
                "success": False,
                "printJobId": print_job_id,
                "errorCode": "video_fetch_failed",
                "errorMessage": str(exc),
            }

        if video_data is None:
            return {
                "success": False,
                "printJobId": print_job_id,
                "errorCode": "video_not_found",
                "errorMessage": f"Timelapse video not found: {video_filename}",
            }

        # Validate video size
        if len(video_data) > max_video_size:
            return {
                "success": False,
                "printJobId": print_job_id,
                "errorCode": "video_too_large",
                "errorMessage": f"Video size {len(video_data)} exceeds max {max_video_size}",
            }

        # Determine content type
        video_content_type = "video/mp4"
        if video_filename.endswith(".webm"):
            video_content_type = "video/webm"

        # Calculate upload timeout based on file size
        # Conservative estimate for cross-region uploads: minimum 120s, 5s per MB + buffer
        # 9.1MB video -> max(120, 9.1*5+30) = max(120, 75.5) = 120s
        # 50MB video -> max(120, 50*5+30) = max(120, 280) = 280s
        video_size_mb = len(video_data) / (1024 * 1024)
        video_upload_timeout = max(120.0, video_size_mb * 5 + 30)  # 5s per MB + 30s buffer
        LOGGER.info(
            "Uploading timelapse video: %.2f MB, timeout %.0fs",
            video_size_mb,
            video_upload_timeout,
        )

        # Upload video to S3
        video_result = await self._s3_upload.upload(
            presigned_url=video_upload_url,
            data=video_data,
            s3_key=video_key,
            content_type=video_content_type,
            timeout_override=video_upload_timeout,
        )

        if video_result.success:
            video_uploaded = True
            result_data["videoKey"] = video_key
            result_data["videoSizeBytes"] = video_result.file_size_bytes
            LOGGER.info(
                "Uploaded timelapse video for job %s: %d bytes to %s",
                print_job_id[:8] if print_job_id else "unknown",
                video_result.file_size_bytes,
                video_key,
            )
        else:
            LOGGER.warning(
                "Failed to upload timelapse video for job %s: %s",
                print_job_id[:8] if print_job_id else "unknown",
                video_result.error_message,
            )
            return {
                "success": False,
                "printJobId": print_job_id,
                "errorCode": video_result.error_code or "video_upload_failed",
                "errorMessage": video_result.error_message,
            }

        # Fetch and upload preview (optional)
        preview_uploaded = False
        if preview_upload_url and preview_key and preview_filename:
            try:
                preview_data = await self._moonraker.fetch_timelapse_file(
                    preview_filename, timeout=30.0
                )

                if preview_data:
                    # Validate preview size
                    if len(preview_data) > max_preview_size:
                        LOGGER.warning(
                            "Preview image too large: %d > %d, skipping",
                            len(preview_data),
                            max_preview_size,
                        )
                    else:
                        preview_result = await self._s3_upload.upload(
                            presigned_url=preview_upload_url,
                            data=preview_data,
                            s3_key=preview_key,
                            content_type="image/jpeg",
                        )

                        if preview_result.success:
                            preview_uploaded = True
                            result_data["previewKey"] = preview_key
                            result_data["previewSizeBytes"] = preview_result.file_size_bytes
                            LOGGER.info(
                                "Uploaded timelapse preview for job %s: %d bytes to %s",
                                print_job_id[:8] if print_job_id else "unknown",
                                preview_result.file_size_bytes,
                                preview_key,
                            )
                        else:
                            LOGGER.warning(
                                "Failed to upload timelapse preview: %s",
                                preview_result.error_message,
                            )
                else:
                    LOGGER.debug("Preview image not found: %s", preview_filename)
            except Exception as exc:
                LOGGER.warning("Failed to fetch timelapse preview: %s", exc)

        result_data["success"] = video_uploaded
        result_data["previewUploaded"] = preview_uploaded
        return result_data
