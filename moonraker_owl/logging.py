"""Logging configuration helpers."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


def configure_logging(
    level: str = "INFO", *, log_path: Optional[Path] = None, log_network: bool = False
) -> None:
    """Configure root logging handlers.

    Parameters
    ----------
    level:
        Log level name, e.g. "INFO".
    log_path:
        Optional filesystem path for a rotating file handler. When absent, only console logging is configured.
    log_network:
        When true, lower the logging level for verbose third-party libraries to aid diagnostics.
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
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logging.getLogger().addHandler(file_handler)

    if not log_network:
        logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("paho").setLevel(logging.WARNING)
