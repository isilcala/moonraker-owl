"""Query command handlers that return data via ACK result payload."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)

GCODE_EXTENSIONS = frozenset({".gcode", ".g", ".gco"})

_MAX_PAGE_SIZE = 200


class QueryCommandsMixin:
    """Mixin providing query-type command handlers."""

    async def _handle_query_file_list(self, message: CommandMessage) -> Dict[str, Any]:
        """Handle query:file-list — return tree-based or flat file listing.

        Tree mode (filter empty): returns current directory's direct files
        + collapsed subdirectories.
        Search mode (filter non-empty): ignores path, returns flat list
        matching filter across all levels.
        """
        params = message.parameters or {}
        path = str(params.get("path", "/")).strip() or "/"
        page = int(params.get("page", 0))
        page_size = min(int(params.get("pageSize", 100)), _MAX_PAGE_SIZE)
        filter_text = str(params.get("filter", "")).strip()

        if page < 0:
            page = 0
        if page_size < 1:
            page_size = 1

        try:
            all_files = await self._moonraker.list_gcode_files(timeout=15.0)
        except Exception as exc:
            raise CommandProcessingError(
                f"Failed to list files from Moonraker: {exc}",
                code="moonraker_error",
                command_id=message.command_id,
            ) from exc

        # Global filter: exclude .thumbs/ and non-GCode extensions
        all_files = [
            f for f in all_files
            if not f.get("path", "").startswith(".thumbs/")
            and _is_gcode_file(f.get("path", ""))
        ]

        if filter_text:
            return _build_flat_search_result(all_files, filter_text, page, page_size)
        else:
            return _build_tree_result(all_files, path, page, page_size)


def _is_gcode_file(path: str) -> bool:
    """Check if filename has a recognized GCode extension."""
    lower = path.lower()
    return any(lower.endswith(ext) for ext in GCODE_EXTENSIONS)


def _build_tree_result(
    all_files: List[dict], path: str, page: int, page_size: int
) -> Dict[str, Any]:
    """Build tree-level result: direct files + collapsed sub-directories."""
    prefix = ""
    if path and path != "/":
        prefix = path.strip("/") + "/"

    files_here: List[Dict[str, Any]] = []
    dir_counts: Dict[str, int] = {}

    for f in all_files:
        rel = f.get("path", "")
        if prefix and not rel.startswith(prefix):
            continue
        remainder = rel[len(prefix):]
        parts = remainder.split("/")
        if len(parts) == 1:
            files_here.append({
                "type": "file",
                "path": f["path"],
                "fileName": parts[0],
                "size": f.get("size", 0),
                "modified": f.get("modified", 0),
            })
        else:
            dir_name = parts[0]
            dir_counts[dir_name] = dir_counts.get(dir_name, 0) + 1

    # Directory entries (sorted alphabetically, before files)
    dir_items: List[Dict[str, Any]] = [
        {
            "type": "directory",
            "path": f"{prefix}{d}" if prefix else d,
            "fileName": d,
            "fileCount": count,
        }
        for d, count in sorted(dir_counts.items())
    ]

    # Files sorted by modified time descending
    files_here.sort(key=lambda f: f.get("modified", 0), reverse=True)

    all_items = dir_items + files_here

    total_count = len(all_items)
    offset = page * page_size
    page_items = all_items[offset: offset + page_size]

    return {
        "items": page_items,
        "currentPath": path if path else "/",
        "totalCount": total_count,
        "page": page,
        "pageSize": page_size,
        "hasMore": (offset + page_size) < total_count,
    }


def _build_flat_search_result(
    all_files: List[dict], filter_text: str, page: int, page_size: int
) -> Dict[str, Any]:
    """Build flat search result across all directories."""
    filter_lower = filter_text.lower()
    matched: List[Dict[str, Any]] = [
        {
            "type": "file",
            "path": f["path"],
            "fileName": f["path"],  # Full path in search mode
            "size": f.get("size", 0),
            "modified": f.get("modified", 0),
        }
        for f in all_files
        if filter_lower in f.get("path", "").lower()
    ]

    matched.sort(key=lambda f: f.get("modified", 0), reverse=True)

    total_count = len(matched)
    offset = page * page_size
    page_items = matched[offset: offset + page_size]

    return {
        "items": page_items,
        "currentPath": "/",
        "totalCount": total_count,
        "page": page,
        "pageSize": page_size,
        "hasMore": (offset + page_size) < total_count,
    }
