"""Logging configuration helpers."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


# Default log rotation settings
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB per file
DEFAULT_BACKUP_COUNT = 3  # Keep 3 backup files (total ~40 MB max)


def configure_logging(
    level: str = "INFO",
    *,
    log_path: Optional[Path] = None,
    log_network: bool = False,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
) -> None:
    """Configure root logging handlers.

    Parameters
    ----------
    level:
        Log level name, e.g. "INFO".
    log_path:
        Optional filesystem path for a rotating file handler.
        When absent, only console logging is configured.
    log_network:
        When true, lower the logging level for verbose third-party libraries to aid diagnostics.
    max_bytes:
        Maximum size in bytes for each log file before rotation.
        Default is 10 MB.
    backup_count:
        Number of backup files to keep. Default is 5.
        Total disk usage will be approximately (backup_count + 1) * max_bytes.
    """

    logging.captureWarnings(True)

    for handler in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(handler)

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        # Use RotatingFileHandler to prevent unbounded log growth
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logging.getLogger().addHandler(file_handler)

    if not log_network:
        logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("paho").setLevel(logging.WARNING)
