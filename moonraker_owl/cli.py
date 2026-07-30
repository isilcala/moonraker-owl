"""Command-line interface for moonraker-owl."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from . import constants
from .app import MoonrakerOwlApp
from .config import ConfigurationError, load_config, validate_runtime_config
from .link import DeviceLinkingError, perform_linking

LOGGER = logging.getLogger(__name__)

# Keys whose values must never be printed by `show-config`. Audit A-04 / Q-1:
# credentials and API keys are sometimes pasted into TOML during testing, and
# operators frequently capture `show-config` output into bug reports / logs.
# Redact at the point of display rather than rely on operators to scrub.
_REDACTED_KEYS: dict[str, set[str]] = {
    "cloud": {"password", "device_private_key"},
    "moonraker": {"api_key"},
}
_REDACTED_PLACEHOLDER = "***redacted***"


def _format_config_value(section: str, key: str, value: object) -> str:
    if key in _REDACTED_KEYS.get(section, set()) and value not in (None, ""):
        return _REDACTED_PLACEHOLDER
    return repr(value) if isinstance(value, str) else str(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="moonraker-owl", description="Owl Cloud companion for Moonraker"
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=constants.DEFAULT_CONFIG_PATH,
        help=f"Path to configuration file (default: {constants.DEFAULT_CONFIG_PATH})",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("start", help="Start the moonraker-owl service")

    link_parser = subparsers.add_parser("link", help="Link this printer to Owl Cloud")
    link_parser.add_argument(
        "--force", action="store_true", help="Force re-link even if credentials exist"
    )

    subparsers.add_parser(
        "show-config", help="Print the resolved configuration and exit"
    )

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)

    if args.command == "start":
        try:
            validate_runtime_config(config)
        except ConfigurationError as exc:
            LOGGER.error("%s", exc)
            return 2
        MoonrakerOwlApp.start(config)
        return 0

    if args.command == "link":
        try:
            validate_runtime_config(config)
        except ConfigurationError as exc:
            LOGGER.error("%s", exc)
            return 2
        try:
            perform_linking(config, force=args.force)
        except DeviceLinkingError as exc:
            LOGGER.error("Linking failed: %s", exc)
            return 1
        return 0

    if args.command == "show-config":
        print(f"Configuration loaded from {config.path!s}\n")
        for section, values in config.raw.items():
            if isinstance(values, dict):
                print(f"[{section}]")
                for key, value in values.items():
                    print(f"{key} = {_format_config_value(section, key, value)}")
                print()
        return 0

    LOGGER.error("Unknown command: %s", args.command)
    return 1


if __name__ == "__main__":
    sys.exit(main())
