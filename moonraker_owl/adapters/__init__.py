"""Adapter modules for external integrations."""

from .camera import CameraClient, CaptureResult
from .moonraker import MoonrakerClient
from .mqtt import MQTTClient, MQTTConnectionError
from .s3_upload import S3UploadClient, UploadResult, detect_content_type

__all__ = [
    "CameraClient",
    "CaptureResult",
    "MoonrakerClient",
    "MQTTClient",
    "MQTTConnectionError",
    "S3UploadClient",
    "UploadResult",
    "detect_content_type",
]
