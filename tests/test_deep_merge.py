"""Verification of the ADR-0003 incremental status merge primitive.

Moonraker pushes *partial* ``notify_status_update`` deltas; the agent must
accumulate them into a full status snapshot via :func:`deep_merge` rather than
overwriting whole sub-trees. These tests pin that incremental-merge contract.
"""

import copy

from moonraker_owl.core import deep_merge


def test_nested_partial_updates_accumulate() -> None:
    target = {"a": 1, "b": {"x": 10, "y": 20}}
    deep_merge(target, {"b": {"x": 100, "z": 30}, "c": 3})
    assert target == {"a": 1, "b": {"x": 100, "y": 20, "z": 30}, "c": 3}


def test_sequential_deltas_build_full_snapshot() -> None:
    # Simulate Moonraker pushing incremental print_stats / heater deltas.
    state: dict = {}
    deep_merge(state, {"print_stats": {"state": "printing", "filename": "a.gcode"}})
    deep_merge(state, {"extruder": {"temperature": 200.0}})
    deep_merge(state, {"print_stats": {"state": "paused"}})

    assert state == {
        "print_stats": {"state": "paused", "filename": "a.gcode"},
        "extruder": {"temperature": 200.0},
    }


def test_scalar_overwrites_dict_and_vice_versa() -> None:
    target = {"k": {"nested": 1}}
    deep_merge(target, {"k": 5})
    assert target == {"k": 5}

    target2 = {"k": 5}
    deep_merge(target2, {"k": {"nested": 1}})
    assert target2 == {"k": {"nested": 1}}


def test_list_values_are_replaced_not_merged() -> None:
    target = {"objects": [1, 2, 3]}
    deep_merge(target, {"objects": [4]})
    assert target == {"objects": [4]}


def test_merge_deep_copies_nested_updates() -> None:
    updates = {"b": {"x": [1, 2]}}
    target: dict = {}
    deep_merge(target, updates)

    # Mutating the source must not leak into the merged target.
    updates["b"]["x"].append(3)
    assert target["b"]["x"] == [1, 2]


def test_merge_does_not_mutate_updates_argument() -> None:
    updates = {"b": {"x": 1}}
    snapshot = copy.deepcopy(updates)
    deep_merge({"b": {"y": 2}}, updates)
    assert updates == snapshot
