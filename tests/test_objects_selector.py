"""Tests for the ObjectsSelector (ADR-0016: Exclude Object)."""

from datetime import datetime, timezone

import pytest

from moonraker_owl.telemetry.selectors import ObjectsSelector
from moonraker_owl.telemetry.state_store import MoonrakerStateStore


def make_exclude_object_notification(
    objects: list[dict],
    excluded_objects: list[str] | None = None,
    current_object: str | None = None,
) -> dict:
    """Create a notify_status_update message with exclude_object data."""
    return {
        "jsonrpc": "2.0",
        "method": "notify_status_update",
        "params": [
            {
                "exclude_object": {
                    "objects": objects,
                    "excluded_objects": excluded_objects or [],
                    "current_object": current_object,
                }
            }
        ],
    }


# =============================================================================
# ObjectsSelector.build() Tests
# =============================================================================


def test_objects_selector_returns_none_when_no_exclude_object() -> None:
    """Test that build() returns None when exclude_object is not in state."""
    store = MoonrakerStateStore()
    selector = ObjectsSelector()
    observed_at = datetime.now(timezone.utc)

    # Ingest some unrelated data
    store.ingest(
        {
            "jsonrpc": "2.0",
            "method": "notify_status_update",
            "params": [{"print_stats": {"state": "printing"}}],
        }
    )

    result = selector.build(store, observed_at=observed_at)
    assert result is None


def test_objects_selector_returns_none_when_objects_empty() -> None:
    """Test that build() returns None when objects list is empty."""
    store = MoonrakerStateStore()
    selector = ObjectsSelector()
    observed_at = datetime.now(timezone.utc)

    store.ingest(make_exclude_object_notification(objects=[]))

    result = selector.build(store, observed_at=observed_at)
    assert result is None


def test_objects_selector_builds_payload_with_objects() -> None:
    """Test that build() returns correct payload with object definitions."""
    store = MoonrakerStateStore()
    selector = ObjectsSelector()
    observed_at = datetime.now(timezone.utc)

    store.ingest(
        make_exclude_object_notification(
            objects=[
                {
                    "name": "cube_1",
                    "center": [100.0, 100.0],
                    "polygon": [[50.0, 50.0], [150.0, 50.0], [150.0, 150.0], [50.0, 150.0]],
                },
                {
                    "name": "cube_2",
                    "center": [200.0, 100.0],
                    "polygon": [[150.0, 50.0], [250.0, 50.0], [250.0, 150.0], [150.0, 150.0]],
                },
            ],
            excluded_objects=[],
            current_object="cube_1",
        )
    )

    result = selector.build(store, observed_at=observed_at)

    assert result is not None
    assert len(result["definitions"]) == 2
    assert result["definitions"][0]["name"] == "cube_1"
    assert result["definitions"][0]["center"] == [100.0, 100.0]
    assert result["definitions"][0]["polygon"] == [
        [50.0, 50.0],
        [150.0, 50.0],
        [150.0, 150.0],
        [50.0, 150.0],
    ]
    assert result["definitions"][1]["name"] == "cube_2"
    assert result["excluded"] == []
    assert result["current"] == "cube_1"


def test_objects_selector_includes_excluded_objects() -> None:
    """Test that build() includes excluded objects list."""
    store = MoonrakerStateStore()
    selector = ObjectsSelector()
    observed_at = datetime.now(timezone.utc)

    store.ingest(
        make_exclude_object_notification(
            objects=[
                {"name": "cube_1"},
                {"name": "cube_2"},
                {"name": "cube_3"},
            ],
            excluded_objects=["cube_1", "cube_3"],
            current_object="cube_2",
        )
    )

    result = selector.build(store, observed_at=observed_at)

    assert result is not None
    assert result["excluded"] == ["cube_1", "cube_3"]
    assert result["current"] == "cube_2"


def test_objects_selector_handles_objects_without_polygon() -> None:
    """Test that build() handles objects without polygon/center data."""
    store = MoonrakerStateStore()
    selector = ObjectsSelector()
    observed_at = datetime.now(timezone.utc)

    store.ingest(
        make_exclude_object_notification(
            objects=[
                {"name": "simple_object"},  # No center or polygon
            ],
            current_object="simple_object",
        )
    )

    result = selector.build(store, observed_at=observed_at)

    assert result is not None
    assert len(result["definitions"]) == 1
    assert result["definitions"][0]["name"] == "simple_object"
    assert "center" not in result["definitions"][0]
    assert "polygon" not in result["definitions"][0]


def test_objects_selector_handles_null_current_object() -> None:
    """Test that build() handles null current_object (between objects)."""
    store = MoonrakerStateStore()
    selector = ObjectsSelector()
    observed_at = datetime.now(timezone.utc)

    store.ingest(
        make_exclude_object_notification(
            objects=[{"name": "cube_1"}],
            current_object=None,
        )
    )

    result = selector.build(store, observed_at=observed_at)

    assert result is not None
    assert result["current"] is None


# =============================================================================
# ObjectsSelector.should_publish() Tests
# =============================================================================


def test_should_publish_returns_true_on_first_appearance() -> None:
    """Test that should_publish() returns True when objects first appear."""
    selector = ObjectsSelector()

    previous = None
    current = {"definitions": [{"name": "cube_1"}], "excluded": [], "current": "cube_1"}

    assert selector.should_publish(previous, current) is True


def test_should_publish_returns_true_when_objects_cleared() -> None:
    """Test that should_publish() returns True when objects are cleared."""
    selector = ObjectsSelector()

    previous = {"definitions": [{"name": "cube_1"}], "excluded": [], "current": "cube_1"}
    current = None

    assert selector.should_publish(previous, current) is True


def test_should_publish_returns_false_when_both_none() -> None:
    """Test that should_publish() returns False when no objects on either side."""
    selector = ObjectsSelector()

    assert selector.should_publish(None, None) is False


def test_should_publish_returns_true_on_excluded_change() -> None:
    """Test that should_publish() returns True when excluded list changes."""
    selector = ObjectsSelector()

    previous = {"definitions": [{"name": "cube_1"}], "excluded": [], "current": "cube_1"}
    current = {"definitions": [{"name": "cube_1"}], "excluded": ["cube_1"], "current": "cube_1"}

    assert selector.should_publish(previous, current) is True


def test_should_publish_returns_true_on_current_change() -> None:
    """Test that should_publish() returns True when current object changes."""
    selector = ObjectsSelector()

    previous = {"definitions": [{"name": "cube_1"}], "excluded": [], "current": "cube_1"}
    current = {"definitions": [{"name": "cube_1"}], "excluded": [], "current": "cube_2"}

    assert selector.should_publish(previous, current) is True


def test_should_publish_returns_false_when_unchanged() -> None:
    """Test that should_publish() returns False when nothing changed."""
    selector = ObjectsSelector()

    state = {"definitions": [{"name": "cube_1"}], "excluded": [], "current": "cube_1"}

    assert selector.should_publish(state, state) is False


def test_should_publish_returns_true_on_definitions_change() -> None:
    """Test that should_publish() returns True when definitions change."""
    selector = ObjectsSelector()

    previous = {"definitions": [{"name": "cube_1"}], "excluded": [], "current": None}
    current = {
        "definitions": [{"name": "cube_1"}, {"name": "cube_2"}],
        "excluded": [],
        "current": None,
    }

    assert selector.should_publish(previous, current) is True


# =============================================================================
# ObjectsSelector.needs_reset() Tests
# =============================================================================


def test_needs_reset_returns_true_when_objects_cleared() -> None:
    """Test that needs_reset() returns True when objects were published but now gone."""
    selector = ObjectsSelector()

    assert selector.needs_reset(had_objects=True, has_objects=False) is True


def test_needs_reset_returns_false_when_never_had_objects() -> None:
    """Test that needs_reset() returns False when objects never existed."""
    selector = ObjectsSelector()

    assert selector.needs_reset(had_objects=False, has_objects=False) is False


def test_needs_reset_returns_false_when_still_has_objects() -> None:
    """Test that needs_reset() returns False when objects still exist."""
    selector = ObjectsSelector()

    assert selector.needs_reset(had_objects=True, has_objects=True) is False


# =============================================================================
# ObjectsSelector.reset() Tests
# =============================================================================


def test_reset_clears_state() -> None:
    """Test that reset() clears internal state."""
    selector = ObjectsSelector()
    selector._previous_payload = {"test": "data"}
    selector._has_published = True

    selector.reset()

    assert selector._previous_payload is None
    assert selector._has_published is False


# =============================================================================
# Edge Cases
# =============================================================================


def test_objects_selector_handles_malformed_objects() -> None:
    """Test that build() handles malformed object entries gracefully."""
    store = MoonrakerStateStore()
    selector = ObjectsSelector()
    observed_at = datetime.now(timezone.utc)

    store.ingest(
        make_exclude_object_notification(
            objects=[
                {"name": "valid_object"},
                {},  # Missing name
                {"name": ""},  # Empty name
                "not_a_dict",  # Not a dict at all
                {"name": "another_valid"},
            ],
        )
    )

    result = selector.build(store, observed_at=observed_at)

    assert result is not None
    # Should only include valid objects
    assert len(result["definitions"]) == 2
    assert result["definitions"][0]["name"] == "valid_object"
    assert result["definitions"][1]["name"] == "another_valid"


def test_objects_selector_handles_malformed_polygon() -> None:
    """Test that build() handles malformed polygon data gracefully."""
    store = MoonrakerStateStore()
    selector = ObjectsSelector()
    observed_at = datetime.now(timezone.utc)

    store.ingest(
        make_exclude_object_notification(
            objects=[
                {
                    "name": "object_with_bad_polygon",
                    "polygon": [[1.0, 2.0], "not_a_point", [3.0]],  # Mixed valid/invalid
                },
            ],
        )
    )

    result = selector.build(store, observed_at=observed_at)

    assert result is not None
    # Should only include valid polygon points
    polygon = result["definitions"][0].get("polygon", [])
    assert len(polygon) == 1  # Only [1.0, 2.0] is valid
    assert polygon[0] == [1.0, 2.0]
