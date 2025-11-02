"""Utility to capture a raw Moonraker websocket stream to a JSONL file."""

from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

try:
    import aiohttp
except ImportError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "The aiohttp package is required. Install it via 'python -m pip install aiohttp'."
    ) from exc


DEFAULT_OBJECTS: Mapping[str, Optional[list[str]]] = {
    "print_stats": [
        "state",
        "message",
        "info",
        "filename",
        "print_duration",
        "total_duration",
        "progress",
    ],
    "display_status": ["message", "progress"],
    "virtual_sdcard": [
        "is_active",
        "is_printing",
        "is_paused",
        "file_path",
        "progress",
    ],
    "idle_timeout": ["state"],
    "history_event": ["job", "status", "action"],
    "gcode_move": ["gcode_position"],
}


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


async def _capture_stream(args: argparse.Namespace) -> None:
    uri = _build_uri(args)
    output_path = _resolve_output_path(args.output)

    headers: dict[str, str] = {}
    if args.api_key:
        headers["X-Api-Key"] = args.api_key

    subscribe_payload = {
        "jsonrpc": "2.0",
        "method": "printer.objects.subscribe",
        "params": {"objects": DEFAULT_OBJECTS},
        "id": 1,
    }

    bootstrap_calls = [
        {"jsonrpc": "2.0", "method": "server.info", "id": 2},
        {"jsonrpc": "2.0", "method": "printer.info", "id": 3},
    ]

    print(f"Connecting to {uri}")
    print(f"Writing stream to {output_path}")

    session_timeout = aiohttp.ClientTimeout(total=None, sock_read=None, sock_connect=args.timeout)


    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        async with session.ws_connect(
            uri,
            headers=headers or None,
            receive_timeout=None,
            heartbeat=30.0,
            max_msg_size=0,
        ) as websocket:
            await websocket.send_json(subscribe_payload)
            for payload in bootstrap_calls:
                await websocket.send_json(payload)

            with output_path.open("a", encoding="utf-8") as handle:
                try:
                    async for message in websocket:
                        record = _handle_ws_message(message)
                        if record is not None:
                            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                            handle.flush()
                except asyncio.CancelledError:  # pragma: no cover - cooperative shutdown
                    raise
                except KeyboardInterrupt:  # pragma: no cover - user interruption
                    print("Interrupted by user; closing connection...")


def _handle_ws_message(message: WSMessage) -> Optional[Mapping[str, Any]]:
    if message.type == aiohttp.WSMsgType.TEXT:
        return _make_record(message.data)
    if message.type == aiohttp.WSMsgType.BINARY:
        return {
            "captured_at": _utc_timestamp(),
            "raw_binary": list(message.data),
        }
    if message.type == aiohttp.WSMsgType.ERROR:
        print(f"Websocket error: {message.data}", file=sys.stderr)
    if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
        print("Websocket closed by server; stopping capture.")
    return None


def _build_uri(args: argparse.Namespace) -> str:
    port_segment = f":{args.port}" if args.port else ""
    return f"{args.scheme}://{args.host}{port_segment}/websocket"


def _resolve_output_path(output: Optional[str]) -> pathlib.Path:
    if output:
        path = pathlib.Path(output)
    else:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = pathlib.Path(f"moonraker-stream-{timestamp}.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _make_record(raw_message: str) -> Mapping[str, Any]:
    base: dict[str, Any] = {"captured_at": _utc_timestamp(), "raw": raw_message}
    try:
        base["payload"] = json.loads(raw_message)
    except json.JSONDecodeError:
        base["payload_parse_error"] = True
    return base


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="Moonraker host or IP (e.g. 192.168.1.50)")
    parser.add_argument("--port", type=int, default=None, help="Moonraker port (defaults to 7125)")
    parser.add_argument(
        "--scheme",
        default="ws",
        choices=("ws", "wss"),
        help="Websocket scheme to use (ws or wss)",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Moonraker API key if authentication is required",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to write the JSONL stream (defaults to ./moonraker-stream-<timestamp>.jsonl)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Socket connect timeout in seconds (default: 10)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv or sys.argv[1:])

    if args.port is None:
        args.port = 7125

    try:
        asyncio.run(_capture_stream(args))
    except KeyboardInterrupt:
        return 0
    except Exception as exc:  # pragma: no cover - defensive
        print(f"Capture failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
