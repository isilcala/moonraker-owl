"""Tests for image preprocessing module."""

from __future__ import annotations

import io
from typing import Tuple

import pytest

# Skip all tests if Pillow is not available
PIL = pytest.importorskip("PIL")
from PIL import Image

from moonraker_owl.adapters.image_preprocessor import (
    ImagePreprocessor,
    PreprocessResult,
    PILLOW_AVAILABLE,
)


def create_test_image(width: int, height: int, color: Tuple[int, int, int] = (255, 0, 0)) -> bytes:
    """Create a test JPEG image with the given dimensions."""
    img = Image.new("RGB", (width, height), color)
    buffer = io.BytesIO()
    img.save(buffer, format="JPEG", quality=95)
    return buffer.getvalue()


def create_test_png_rgba(width: int, height: int) -> bytes:
    """Create a test PNG image with alpha channel."""
    img = Image.new("RGBA", (width, height), (255, 0, 0, 128))
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    return buffer.getvalue()


class TestImagePreprocessor:
    """Tests for ImagePreprocessor class."""

    def test_pillow_available(self):
        """Verify Pillow is available for tests."""
        assert PILLOW_AVAILABLE is True

    def test_default_settings(self):
        """Test default target_width and jpeg_quality."""
        preprocessor = ImagePreprocessor()
        assert preprocessor.target_width == 1280
        assert preprocessor.jpeg_quality == 85
        assert preprocessor.enabled is True
        assert preprocessor.available is True

    def test_custom_settings(self):
        """Test custom configuration."""
        preprocessor = ImagePreprocessor(
            target_width=640,
            jpeg_quality=75,
            enabled=True,
        )
        assert preprocessor.target_width == 640
        assert preprocessor.jpeg_quality == 75

    def test_no_resize_when_smaller_than_target(self):
        """Image smaller than target should pass through unchanged."""
        preprocessor = ImagePreprocessor(target_width=800)
        
        # Create 640x480 image (smaller than 800px target)
        original_data = create_test_image(640, 480)
        
        result = preprocessor.preprocess(original_data)
        
        assert result.was_resized is False
        assert result.original_size == (640, 480)
        assert result.processed_size == (640, 480)
        assert result.image_data == original_data  # Unchanged
        assert result.original_bytes == result.processed_bytes

    def test_no_resize_when_equal_to_target(self):
        """Image exactly at target width should pass through unchanged."""
        preprocessor = ImagePreprocessor(target_width=800)
        
        original_data = create_test_image(800, 600)
        
        result = preprocessor.preprocess(original_data)
        
        assert result.was_resized is False
        assert result.original_size == (800, 600)
        assert result.image_data == original_data

    def test_resize_when_larger_than_target(self):
        """Image larger than target should be resized."""
        preprocessor = ImagePreprocessor(target_width=800, jpeg_quality=85)
        
        # Create 1280x720 image (larger than 800px target)
        original_data = create_test_image(1280, 720)
        
        result = preprocessor.preprocess(original_data)
        
        assert result.was_resized is True
        assert result.original_size == (1280, 720)
        assert result.processed_size == (800, 450)  # Aspect ratio preserved: 720 * (800/1280) = 450
        assert result.original_bytes == len(original_data)
        assert result.processed_bytes == len(result.image_data)
        assert result.content_type == "image/jpeg"
        
        # Verify the output is a valid JPEG
        output_img = Image.open(io.BytesIO(result.image_data))
        assert output_img.size == (800, 450)
        assert output_img.format == "JPEG"

    def test_aspect_ratio_preserved(self):
        """Verify aspect ratio is preserved during resize."""
        preprocessor = ImagePreprocessor(target_width=800)
        
        # Create 1920x1080 image (16:9 aspect ratio)
        original_data = create_test_image(1920, 1080)
        
        result = preprocessor.preprocess(original_data)
        
        assert result.was_resized is True
        assert result.processed_size == (800, 450)  # 1080 * (800/1920) = 450
        
        # Check aspect ratio
        original_ratio = 1920 / 1080
        processed_ratio = 800 / 450
        assert abs(original_ratio - processed_ratio) < 0.01

    def test_tall_image_aspect_ratio(self):
        """Verify tall images preserve aspect ratio correctly."""
        preprocessor = ImagePreprocessor(target_width=800)
        
        # Create 1200x1600 portrait image
        original_data = create_test_image(1200, 1600)
        
        result = preprocessor.preprocess(original_data)
        
        assert result.was_resized is True
        expected_height = int(1600 * (800 / 1200))  # ~1066
        assert result.processed_size == (800, expected_height)

    def test_rgba_to_rgb_conversion(self):
        """Test that RGBA images are converted to RGB (JPEG doesn't support alpha)."""
        preprocessor = ImagePreprocessor(target_width=800)
        
        # Create RGBA PNG larger than target
        original_data = create_test_png_rgba(1200, 900)
        
        result = preprocessor.preprocess(original_data, content_type="image/png")
        
        assert result.was_resized is True
        assert result.content_type == "image/jpeg"
        
        # Verify output is valid JPEG in RGB mode
        output_img = Image.open(io.BytesIO(result.image_data))
        assert output_img.mode == "RGB"
        assert output_img.format == "JPEG"

    def test_disabled_preprocessor(self):
        """Test that disabled preprocessor passes through unchanged."""
        preprocessor = ImagePreprocessor(target_width=800, enabled=False)
        
        original_data = create_test_image(1280, 720)
        
        result = preprocessor.preprocess(original_data)
        
        assert result.was_resized is False
        assert result.image_data == original_data
        # Size info not available when disabled
        assert result.original_size == (0, 0)

    def test_file_size_reduction(self):
        """Verify that resizing reduces file size."""
        preprocessor = ImagePreprocessor(target_width=800, jpeg_quality=85)
        
        # Create large image
        original_data = create_test_image(1920, 1080)
        
        result = preprocessor.preprocess(original_data)
        
        assert result.was_resized is True
        # Processed should be smaller (both dimension reduction and recompression)
        assert result.processed_bytes < result.original_bytes

    def test_invalid_image_data_graceful_fallback(self):
        """Test graceful handling of invalid image data."""
        preprocessor = ImagePreprocessor(target_width=800)
        
        invalid_data = b"not an image"
        
        result = preprocessor.preprocess(invalid_data, content_type="image/jpeg")
        
        # Should return original data on error
        assert result.was_resized is False
        assert result.image_data == invalid_data
        assert result.original_bytes == len(invalid_data)
        assert result.processed_bytes == len(invalid_data)

    def test_empty_image_data(self):
        """Test handling of empty image data."""
        preprocessor = ImagePreprocessor(target_width=800)
        
        result = preprocessor.preprocess(b"")
        
        assert result.was_resized is False
        assert result.image_data == b""

    def test_jpeg_quality_affects_output_size(self):
        """Test that lower JPEG quality produces smaller files."""
        high_quality = ImagePreprocessor(target_width=800, jpeg_quality=95)
        low_quality = ImagePreprocessor(target_width=800, jpeg_quality=50)
        
        original_data = create_test_image(1280, 720)
        
        result_high = high_quality.preprocess(original_data)
        result_low = low_quality.preprocess(original_data)
        
        assert result_high.was_resized is True
        assert result_low.was_resized is True
        # Lower quality should produce smaller file
        assert result_low.processed_bytes < result_high.processed_bytes


class TestPreprocessResult:
    """Tests for PreprocessResult dataclass."""

    def test_result_attributes(self):
        """Test PreprocessResult has all expected attributes."""
        result = PreprocessResult(
            image_data=b"test",
            original_size=(1920, 1080),
            processed_size=(800, 450),
            was_resized=True,
            original_bytes=100000,
            processed_bytes=25000,
            content_type="image/jpeg",
        )
        
        assert result.image_data == b"test"
        assert result.original_size == (1920, 1080)
        assert result.processed_size == (800, 450)
        assert result.was_resized is True
        assert result.original_bytes == 100000
        assert result.processed_bytes == 25000
        assert result.content_type == "image/jpeg"
