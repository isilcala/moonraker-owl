"""Utility for inspecting live Moonraker websocket payloads."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from ..adapters.moonraker import MoonrakerClient
from ..config import OwlConfig, load_config
from ..telemetry import build_subscription_manifest

LOGGER = logging.getLogger(__name__)


async def _monitor(config: OwlConfig, *, method: Optional[str], raw: bool) -> None:
    moonraker = MoonrakerClient(
        config.moonraker,
        reconnect_initial=config.resilience.reconnect_initial_seconds,
        reconnect_max=config.resilience.reconnect_max_seconds,
    )

    subscription_objects = build_subscription_manifest(
        tuple(config.include_fields), tuple(config.exclude_fields)
    )
    moonraker.set_subscription_objects(subscription_objects)

    print("Subscription manifest:")
    print(json.dumps(subscription_objects, indent=2, sort_keys=True))
    print("=" * 60)

    finished = asyncio.Event()

    async def _callback(payload: Dict[str, Any]) -> None:
        if method and payload.get("method") != method:
            return

        if raw:
            formatted = json.dumps(payload, indent=2, sort_keys=True)
            print(formatted)
            print("-" * 60)
            return

        event = payload.get("method", "<no method>")
        print(f"event: {event}")
        params = payload.get("params")
        if isinstance(params, list):
            for index, item in enumerate(params):
                print(
                    f"  params[{index}]: {json.dumps(item, indent=2, sort_keys=True)}"
                )
        result = payload.get("result")
        if result is not None:
            print(f"  result: {json.dumps(result, indent=2, sort_keys=True)}")
        print("-" * 60)

    await moonraker.start(_callback)

    try:
        snapshot = await moonraker.fetch_printer_state(subscription_objects)
    except Exception as exc:  # pragma: no cover - diagnostics helper
        LOGGER.warning("Failed to fetch initial printer state: %s", exc)
    else:
        print("Initial printer snapshot:")
        print(json.dumps(snapshot, indent=2, sort_keys=True))
        print("=" * 60)

    print("Listening for Moonraker websocket events (Ctrl+C to stop)...")
    try:
        await finished.wait()
    except asyncio.CancelledError:  # pragma: no cover - cooperative shutdown
        raise
    finally:
        await moonraker.aclose()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tail Moonraker websocket payloads using the owl.cfg configuration",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("owl.cfg"),
        help="Path to the moonraker-owl configuration file (default: owl.cfg)",
    )
    parser.add_argument(
        "--method",
        help="Only display websocket payloads whose method matches this value",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw payloads without any helper formatting",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    config_path = args.config
    if not config_path.exists():
        raise SystemExit(f"Configuration file not found: {config_path}")

    config = load_config(config_path)

    try:
        asyncio.run(_monitor(config, method=args.method, raw=args.raw))
    except KeyboardInterrupt:
        print("Stopping websocket monitor...")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
