"""Tests for telemetry compression."""

import gzip
import json
from configparser import ConfigParser
from pathlib import Path

import pytest

from moonraker_owl.config import (
    CloudConfig,
    CommandConfig,
    CompressionConfig,
    LoggingConfig,
    MoonrakerConfig,
    OwlConfig,
    ResilienceConfig,
    TelemetryConfig,
    TelemetryCadenceConfig,
)


def test_compression_config_defaults():
    """Test CompressionConfig default values."""
    config = CompressionConfig()
    
    assert config.enabled is True
    assert config.channels == ["sensors"]
    assert config.min_size_bytes == 1024


def test_compression_config_is_disabled_by_env(monkeypatch):
    """Test environment variable override for compression."""
    # Test enabled by default
    monkeypatch.delenv("OWL_COMPRESSION_DISABLED", raising=False)
    assert CompressionConfig.is_disabled_by_env() is False
    
    # Test disabled by 'true'
    monkeypatch.setenv("OWL_COMPRESSION_DISABLED", "true")
    assert CompressionConfig.is_disabled_by_env() is True
    
    # Test disabled by '1'
    monkeypatch.setenv("OWL_COMPRESSION_DISABLED", "1")
    assert CompressionConfig.is_disabled_by_env() is True
    
    # Test disabled by 'yes'
    monkeypatch.setenv("OWL_COMPRESSION_DISABLED", "yes")
    assert CompressionConfig.is_disabled_by_env() is True
    
    # Test case-insensitive
    monkeypatch.setenv("OWL_COMPRESSION_DISABLED", "TRUE")
    assert CompressionConfig.is_disabled_by_env() is True
    
    # Test not disabled by other values
    monkeypatch.setenv("OWL_COMPRESSION_DISABLED", "false")
    assert CompressionConfig.is_disabled_by_env() is False


def test_compression_config_custom_values():
    """Test CompressionConfig with custom values."""
    config = CompressionConfig(
        enabled=False,
        channels=["sensors", "status"],
        min_size_bytes=2048,
    )
    
    assert config.enabled is False
    assert config.channels == ["sensors", "status"]
    assert config.min_size_bytes == 2048


def test_load_config_parses_compression_section(tmp_path):
    """Test that load_config correctly parses compression section."""
    from moonraker_owl.config import load_config
    
    config_content = """
[cloud]
device_id = test-device
broker_host = test.broker.com

[compression]
enabled = false
channels = sensors,status
min_size_bytes = 512
"""
    config_file = tmp_path / "owl.cfg"
    config_file.write_text(config_content)
    
    config = load_config(config_file)
    
    assert config.compression.enabled is False
    assert config.compression.channels == ["sensors", "status"]
    assert config.compression.min_size_bytes == 512


def test_load_config_respects_env_override(tmp_path, monkeypatch):
    """Test that environment variable overrides config file for compression."""
    from moonraker_owl.config import load_config
    
    config_content = """
[cloud]
device_id = test-device
broker_host = test.broker.com

[compression]
enabled = true
"""
    config_file = tmp_path / "owl.cfg"
    config_file.write_text(config_content)
    
    # Environment variable should override config file
    monkeypatch.setenv("OWL_COMPRESSION_DISABLED", "true")
    
    config = load_config(config_file)
    
    assert config.compression.enabled is False


def test_load_config_uses_defaults_when_compression_section_missing(tmp_path):
    """Test that load_config uses defaults when compression section is missing."""
    from moonraker_owl.config import load_config
    
    config_content = """
[cloud]
device_id = test-device
broker_host = test.broker.com
"""
    config_file = tmp_path / "owl.cfg"
    config_file.write_text(config_content)
    
    config = load_config(config_file)
    
    assert config.compression.enabled is True
    assert config.compression.channels == ["sensors"]
    assert config.compression.min_size_bytes == 1024
