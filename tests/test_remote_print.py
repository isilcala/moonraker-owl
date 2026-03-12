"""Tests for print:start and query:file-list command handlers."""

import asyncio
import json

import pytest

from moonraker_owl.commands import CommandProcessor
from moonraker_owl.commands.handlers.query_commands import (
    _build_flat_search_result,
    _build_tree_result,
)


# ---------------------------------------------------------------------------
# Shared fixtures — reuse FakeMoonraker / FakeMQTT from test_commands.py
# ---------------------------------------------------------------------------

from test_commands import FakeMoonraker, FakeMQTT


@pytest.fixture
def config():
    from helpers import build_config
    return build_config()


SAMPLE_FILES = [
    {"path": "benchy.gcode", "modified": 1710000000.0, "size": 1048576},
    {"path": "parts/bracket-v2.gcode", "modified": 1709900000.0, "size": 5242880},
    {"path": "parts/mount.gcode", "modified": 1709950000.0, "size": 2097152},
    {"path": "cases/phone-case.gcode", "modified": 1709800000.0, "size": 3145728},
    {"path": "cases/tablet-case.g", "modified": 1709850000.0, "size": 4194304},
    {"path": ".thumbs/benchy-32x32.png", "modified": 1710000001.0, "size": 2048},
    {"path": "readme.txt", "modified": 1700000000.0, "size": 512},
]


# ============================================================================
# Unit tests for tree/search result builders
# ============================================================================


class TestBuildTreeResult:
    def test_root_level_shows_files_and_directories(self):
        result = _build_tree_result(SAMPLE_FILES[:5], "/", 0, 100)

        assert result["currentPath"] == "/"
        dirs = [i for i in result["items"] if i["type"] == "directory"]
        files = [i for i in result["items"] if i["type"] == "file"]

        dir_names = {d["fileName"] for d in dirs}
        assert dir_names == {"parts", "cases"}

        file_names = {f["fileName"] for f in files}
        assert file_names == {"benchy.gcode"}

    def test_directories_sorted_alphabetically_before_files(self):
        result = _build_tree_result(SAMPLE_FILES[:5], "/", 0, 100)
        types = [i["type"] for i in result["items"]]
        # Directories first, then files
        assert types == ["directory", "directory", "file"]

    def test_directory_file_count(self):
        result = _build_tree_result(SAMPLE_FILES[:5], "/", 0, 100)
        dirs = {d["fileName"]: d for d in result["items"] if d["type"] == "directory"}
        assert dirs["parts"]["fileCount"] == 2
        assert dirs["cases"]["fileCount"] == 2

    def test_subdirectory_listing(self):
        result = _build_tree_result(SAMPLE_FILES[:5], "parts", 0, 100)
        assert result["currentPath"] == "parts"
        assert len(result["items"]) == 2
        assert all(i["type"] == "file" for i in result["items"])
        names = {i["fileName"] for i in result["items"]}
        assert names == {"bracket-v2.gcode", "mount.gcode"}

    def test_files_sorted_by_modified_descending(self):
        result = _build_tree_result(SAMPLE_FILES[:5], "parts", 0, 100)
        files = result["items"]
        assert files[0]["fileName"] == "mount.gcode"  # 1709950000 > 1709900000
        assert files[1]["fileName"] == "bracket-v2.gcode"

    def test_pagination(self):
        result = _build_tree_result(SAMPLE_FILES[:5], "/", 0, 2)
        assert len(result["items"]) == 2
        assert result["totalCount"] == 3
        assert result["hasMore"] is True
        assert result["page"] == 0

        result2 = _build_tree_result(SAMPLE_FILES[:5], "/", 1, 2)
        assert len(result2["items"]) == 1
        assert result2["hasMore"] is False

    def test_empty_directory(self):
        result = _build_tree_result(SAMPLE_FILES[:5], "nonexistent", 0, 100)
        assert result["items"] == []
        assert result["totalCount"] == 0

    def test_directory_path_construction(self):
        result = _build_tree_result(SAMPLE_FILES[:5], "/", 0, 100)
        dirs = {d["fileName"]: d for d in result["items"] if d["type"] == "directory"}
        assert dirs["parts"]["path"] == "parts"
        assert dirs["cases"]["path"] == "cases"


class TestBuildFlatSearchResult:
    def test_filter_matches_filename(self):
        result = _build_flat_search_result(SAMPLE_FILES[:5], "bracket", 0, 100)
        assert len(result["items"]) == 1
        assert result["items"][0]["path"] == "parts/bracket-v2.gcode"

    def test_filter_case_insensitive(self):
        result = _build_flat_search_result(SAMPLE_FILES[:5], "BENCHY", 0, 100)
        assert len(result["items"]) == 1
        assert result["items"][0]["path"] == "benchy.gcode"

    def test_filter_matches_path_components(self):
        result = _build_flat_search_result(SAMPLE_FILES[:5], "parts/", 0, 100)
        assert len(result["items"]) == 2

    def test_search_shows_full_path_as_filename(self):
        result = _build_flat_search_result(SAMPLE_FILES[:5], "bracket", 0, 100)
        assert result["items"][0]["fileName"] == "parts/bracket-v2.gcode"

    def test_search_pagination(self):
        result = _build_flat_search_result(SAMPLE_FILES[:5], "gcode", 0, 2)
        assert result["totalCount"] == 4  # All .gcode files match
        assert len(result["items"]) == 2
        assert result["hasMore"] is True

    def test_no_matches(self):
        result = _build_flat_search_result(SAMPLE_FILES[:5], "zzz_no_match", 0, 100)
        assert result["items"] == []
        assert result["totalCount"] == 0


# ============================================================================
# Integration tests via CommandProcessor
# ============================================================================


@pytest.mark.asyncio
async def test_query_file_list_tree_mode(config):
    """query:file-list with no filter returns tree-structured result."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    moonraker.gcode_files = list(SAMPLE_FILES)

    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    message = {
        "$id": "cmd-qfl-1",
        "payload": {
            "command": "query:file-list",
            "parameters": {"path": "/", "page": 0, "pageSize": 100},
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/query:file-list", message)
    await asyncio.sleep(0.01)

    # Should have accepted + completed ACKs
    assert len(mqtt.published) == 2

    completed_topic, completed_payload, _, _ = mqtt.published[1]
    ack = json.loads(completed_payload.decode("utf-8"))
    assert ack["payload"]["status"] == "completed"

    result = ack["payload"].get("result", {})
    assert result["currentPath"] == "/"
    # Should have directories and files, no .thumbs or .txt
    items = result["items"]
    assert any(i["type"] == "directory" for i in items)
    assert all(
        not i.get("path", "").startswith(".thumbs/") for i in items
    )

    await processor.stop()


@pytest.mark.asyncio
async def test_query_file_list_search_mode(config):
    """query:file-list with filter returns flat search results."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    moonraker.gcode_files = list(SAMPLE_FILES)

    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    message = {
        "$id": "cmd-qfl-2",
        "payload": {
            "command": "query:file-list",
            "parameters": {"filter": "bracket", "page": 0, "pageSize": 100},
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/query:file-list", message)
    await asyncio.sleep(0.01)

    assert len(mqtt.published) == 2
    ack = json.loads(mqtt.published[1][1].decode("utf-8"))
    result = ack["payload"]["result"]
    assert result["totalCount"] == 1
    assert result["items"][0]["path"] == "parts/bracket-v2.gcode"

    await processor.stop()


@pytest.mark.asyncio
async def test_query_file_list_filters_thumbs_and_non_gcode(config):
    """query:file-list excludes .thumbs/ files and non-GCode extensions."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    moonraker.gcode_files = list(SAMPLE_FILES)

    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    message = {
        "$id": "cmd-qfl-3",
        "payload": {
            "command": "query:file-list",
            "parameters": {"path": "/", "page": 0, "pageSize": 200},
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/query:file-list", message)
    await asyncio.sleep(0.01)

    ack = json.loads(mqtt.published[1][1].decode("utf-8"))
    result = ack["payload"]["result"]
    all_paths = [i["path"] for i in result["items"]]
    # Flatten: directories have no path with .thumbs
    for item in result["items"]:
        assert not item.get("path", "").startswith(".thumbs/")
        if item["type"] == "file":
            assert not item["path"].endswith(".txt")

    await processor.stop()


@pytest.mark.asyncio
async def test_print_start_success(config):
    """print:start command starts printing when printer is idle."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    moonraker.printer_state = "standby"

    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    message = {
        "$id": "cmd-ps-1",
        "payload": {
            "command": "print:start",
            "parameters": {"filename": "benchy.gcode"},
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/print:start", message)
    await asyncio.sleep(0.01)

    assert moonraker.started_prints == ["benchy.gcode"]

    # Check completed ACK
    assert len(mqtt.published) == 2
    ack = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert ack["payload"]["status"] == "completed"
    assert ack["payload"]["result"]["filename"] == "benchy.gcode"

    await processor.stop()


@pytest.mark.asyncio
async def test_print_start_rejects_when_printing(config):
    """print:start fails if printer is currently printing."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    moonraker.printer_state = "printing"

    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    message = {
        "$id": "cmd-ps-2",
        "payload": {
            "command": "print:start",
            "parameters": {"filename": "benchy.gcode"},
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/print:start", message)
    await asyncio.sleep(0.01)

    assert moonraker.started_prints == []

    # Check failed ACK
    ack = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert ack["payload"]["status"] == "failed"
    assert ack["payload"]["reason"]["code"] == "printer_busy"

    await processor.stop()


@pytest.mark.asyncio
async def test_print_start_rejects_when_paused(config):
    """print:start fails if printer is paused."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    moonraker.printer_state = "paused"

    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    message = {
        "$id": "cmd-ps-3",
        "payload": {
            "command": "print:start",
            "parameters": {"filename": "model.gcode"},
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/print:start", message)
    await asyncio.sleep(0.01)

    assert moonraker.started_prints == []
    ack = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert ack["payload"]["status"] == "failed"
    assert ack["payload"]["reason"]["code"] == "printer_busy"

    await processor.stop()


@pytest.mark.asyncio
async def test_print_start_rejects_path_traversal(config):
    """print:start rejects filenames containing '..' path traversal."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()

    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    message = {
        "$id": "cmd-ps-4",
        "payload": {
            "command": "print:start",
            "parameters": {"filename": "../../../etc/passwd"},
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/print:start", message)
    await asyncio.sleep(0.01)

    assert moonraker.started_prints == []
    ack = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert ack["payload"]["status"] == "failed"
    assert ack["payload"]["reason"]["code"] == "invalid_parameters"

    await processor.stop()


@pytest.mark.asyncio
async def test_print_start_rejects_absolute_path(config):
    """print:start rejects absolute filenames."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()

    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    message = {
        "$id": "cmd-ps-5",
        "payload": {
            "command": "print:start",
            "parameters": {"filename": "/etc/passwd"},
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/print:start", message)
    await asyncio.sleep(0.01)

    assert moonraker.started_prints == []
    ack = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert ack["payload"]["status"] == "failed"
    assert ack["payload"]["reason"]["code"] == "invalid_parameters"

    await processor.stop()


@pytest.mark.asyncio
async def test_print_start_rejects_empty_filename(config):
    """print:start rejects empty or missing filename."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()

    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    message = {
        "$id": "cmd-ps-6",
        "payload": {
            "command": "print:start",
            "parameters": {"filename": ""},
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/print:start", message)
    await asyncio.sleep(0.01)

    assert moonraker.started_prints == []
    ack = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert ack["payload"]["status"] == "failed"
    assert ack["payload"]["reason"]["code"] == "invalid_parameters"

    await processor.stop()


@pytest.mark.asyncio
async def test_print_start_allows_subdirectory_files(config):
    """print:start accepts filenames in subdirectories."""
    moonraker = FakeMoonraker()
    mqtt = FakeMQTT()
    moonraker.printer_state = "standby"

    processor = CommandProcessor(config, moonraker, mqtt)
    await processor.start()

    message = {
        "$id": "cmd-ps-7",
        "payload": {
            "command": "print:start",
            "parameters": {"filename": "parts/bracket-v2.gcode"},
        },
    }

    await mqtt.emit("owl/printers/device-123/commands/print:start", message)
    await asyncio.sleep(0.01)

    assert moonraker.started_prints == ["parts/bracket-v2.gcode"]
    ack = json.loads(mqtt.published[1][1].decode("utf-8"))
    assert ack["payload"]["status"] == "completed"

    await processor.stop()
