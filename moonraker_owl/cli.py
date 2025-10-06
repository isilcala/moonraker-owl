"""Command-line interface for moonraker-owl."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from . import constants
from .app import MoonrakerOwlApp
from .config import load_config
from .link import DeviceLinkingError, perform_linking

LOGGER = logging.getLogger(__name__)


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
        MoonrakerOwlApp.start(config)
        return 0

    if args.command == "link":
        try:
            perform_linking(config, force=args.force)
        except DeviceLinkingError as exc:
            LOGGER.error("Linking failed: %s", exc)
            return 1
        return 0

    if args.command == "show-config":
        print(f"Configuration loaded from {config.path!s}\n")
        for section in config.raw.sections():
            print(f"[{section}]")
            for key, value in config.raw[section].items():
                print(f"{key} = {value}")
            print()
        return 0

    LOGGER.error("Unknown command: %s", args.command)
    return 1


if __name__ == "__main__":
    sys.exit(main())
