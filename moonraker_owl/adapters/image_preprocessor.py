"""Image preprocessing for upload optimization.

This module provides image resizing and compression to reduce upload bandwidth
and storage costs while preserving enough detail for YOLO inference.

The strategy is:
1. Resize images wider than TARGET_WIDTH to TARGET_WIDTH (preserving aspect ratio)
2. Re-encode as JPEG with quality setting (default 85%)
3. YOLO will letterbox the result to 1024x1024 for inference

Using 1280px target width gives YOLO more detail to work with than uploading
the original 1024px, while still achieving significant file size reduction.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Optional, Tuple

LOGGER = logging.getLogger(__name__)

# Try to import Pillow, gracefully degrade if not available
try:
    from PIL import Image

    PILLOW_AVAILABLE = True
except ImportError:
    PILLOW_AVAILABLE = False
    LOGGER.warning(
        "Pillow not installed. Image preprocessing disabled. "
        "Install with: pip install Pillow"
    )


@dataclass(slots=True)
class PreprocessResult:
    """Result of image preprocessing."""

    image_data: bytes
    """Processed image bytes (or original if no processing needed)."""

    original_size: Tuple[int, int]
    """Original image dimensions (width, height)."""

    processed_size: Tuple[int, int]
    """Processed image dimensions (width, height)."""

    was_resized: bool
    """Whether the image was resized."""

    original_bytes: int
    """Original file size in bytes."""

    processed_bytes: int
    """Processed file size in bytes."""

    content_type: str
    """MIME type of the output image."""


class ImagePreprocessor:
    """Preprocesses images for optimal upload and AI inference.

    This class handles:
    - Resizing images that exceed target width (preserving aspect ratio)
    - Re-encoding as JPEG with configurable quality
    - Graceful fallback when Pillow is not available

    The default target width of 1280px is chosen because:
    - YOLO uses 1024x1024 input, so 1280px provides extra detail margin
    - Most webcams capture at 1280x720 or higher
    - 1280px width typically reduces file size by 40-60%
    """

    def __init__(
        self,
        target_width: int = 1280,
        jpeg_quality: int = 85,
        *,
        enabled: bool = True,
    ) -> None:
        """Initialize the image preprocessor.

        Args:
            target_width: Maximum width for output images. Images wider than
                this will be resized (preserving aspect ratio).
            jpeg_quality: JPEG quality setting (1-100). Higher = better quality,
                larger file. 85 is a good balance.
            enabled: Whether preprocessing is enabled. If False, images pass
                through unchanged.
        """
        self._target_width = target_width
        self._jpeg_quality = jpeg_quality
        self._enabled = enabled

    @property
    def target_width(self) -> int:
        """Get the target width for resizing."""
        return self._target_width

    @property
    def jpeg_quality(self) -> int:
        """Get the JPEG quality setting."""
        return self._jpeg_quality

    @property
    def enabled(self) -> bool:
        """Whether preprocessing is enabled."""
        return self._enabled

    @property
    def available(self) -> bool:
        """Whether Pillow is available for preprocessing."""
        return PILLOW_AVAILABLE

    def preprocess(
        self,
        image_data: bytes,
        content_type: Optional[str] = None,
    ) -> PreprocessResult:
        """Preprocess an image for upload.

        If the image is wider than target_width, it will be resized
        (preserving aspect ratio) and re-encoded as JPEG.

        Args:
            image_data: Raw image bytes.
            content_type: Optional MIME type hint.

        Returns:
            PreprocessResult with processed image data and metadata.
        """
        original_bytes = len(image_data)

        # Early exit if disabled or Pillow unavailable
        if not self._enabled or not PILLOW_AVAILABLE:
            return PreprocessResult(
                image_data=image_data,
                original_size=(0, 0),  # Unknown without Pillow
                processed_size=(0, 0),
                was_resized=False,
                original_bytes=original_bytes,
                processed_bytes=original_bytes,
                content_type=content_type or "image/jpeg",
            )

        try:
            # Load image
            img = Image.open(io.BytesIO(image_data))
            original_width, original_height = img.size

            # Check if resizing is needed
            if original_width <= self._target_width:
                # No resize needed, but still return metadata
                return PreprocessResult(
                    image_data=image_data,
                    original_size=(original_width, original_height),
                    processed_size=(original_width, original_height),
                    was_resized=False,
                    original_bytes=original_bytes,
                    processed_bytes=original_bytes,
                    content_type=content_type or "image/jpeg",
                )

            # Calculate new dimensions preserving aspect ratio
            ratio = self._target_width / original_width
            new_height = int(original_height * ratio)
            new_size = (self._target_width, new_height)

            # Resize with high-quality resampling
            # Convert to RGB if necessary (handles RGBA, palette modes)
            if img.mode in ("RGBA", "LA", "P"):
                # Create white background for transparency
                background = Image.new("RGB", img.size, (255, 255, 255))
                if img.mode == "P":
                    img = img.convert("RGBA")
                background.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
                img = background
            elif img.mode != "RGB":
                img = img.convert("RGB")

            img_resized = img.resize(new_size, Image.Resampling.LANCZOS)

            # Encode as JPEG
            buffer = io.BytesIO()
            img_resized.save(buffer, format="JPEG", quality=self._jpeg_quality, optimize=True)
            processed_data = buffer.getvalue()
            processed_bytes = len(processed_data)

            compression_ratio = (1 - processed_bytes / original_bytes) * 100
            LOGGER.debug(
                "Image preprocessed: %dx%d -> %dx%d, %d -> %d bytes (%.1f%% reduction)",
                original_width,
                original_height,
                new_size[0],
                new_size[1],
                original_bytes,
                processed_bytes,
                compression_ratio,
            )

            return PreprocessResult(
                image_data=processed_data,
                original_size=(original_width, original_height),
                processed_size=new_size,
                was_resized=True,
                original_bytes=original_bytes,
                processed_bytes=processed_bytes,
                content_type="image/jpeg",
            )

        except Exception as e:
            LOGGER.warning(
                "Image preprocessing failed, using original: %s",
                str(e),
            )
            return PreprocessResult(
                image_data=image_data,
                original_size=(0, 0),
                processed_size=(0, 0),
                was_resized=False,
                original_bytes=original_bytes,
                processed_bytes=original_bytes,
                content_type=content_type or "image/jpeg",
            )
