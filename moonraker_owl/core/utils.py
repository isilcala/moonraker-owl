"""Core utility functions shared across modules."""

from __future__ import annotations

import copy
from typing import Any, Dict


def deep_merge(target: Dict[str, Any], updates: Dict[str, Any]) -> None:
    """Recursively merge updates into target dict, modifying target in-place.

    For each key in updates:
    - If both target[key] and updates[key] are dicts, recursively merge them
    - Otherwise, overwrite target[key] with a deep copy of updates[key]

    Args:
        target: Dictionary to be updated (modified in-place)
        updates: Dictionary containing new values to merge

    Examples:
        >>> target = {"a": 1, "b": {"x": 10, "y": 20}}
        >>> updates = {"b": {"x": 100, "z": 30}, "c": 3}
        >>> deep_merge(target, updates)
        >>> target
        {"a": 1, "b": {"x": 100, "y": 20, "z": 30}, "c": 3}
    """
    for key, value in updates.items():
        existing = target.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            deep_merge(existing, value)
        else:
            target[key] = copy.deepcopy(value)
