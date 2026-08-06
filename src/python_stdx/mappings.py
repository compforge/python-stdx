"""Small, dependency-free helpers for mapping-shaped data."""

import operator
from collections.abc import Callable, Iterable, Mapping
from typing import Any, TypeVar

Key = TypeVar("Key")
Value = TypeVar("Value")
MappedValue = TypeVar("MappedValue")
Default = TypeVar("Default")


def get_in(data: object, *path: object, default: Default | None = None) -> Any | Default | None:
    """Read a nested item path, returning ``default`` when it cannot be followed."""
    if not path:
        return data

    current: Any = data
    try:
        for key in path:
            current = operator.getitem(current, key)
    except (KeyError, IndexError, TypeError):
        return default
    return current


def map_values(mapping: Mapping[Key, Value], function: Callable[[Value], MappedValue]) -> dict[Key, MappedValue]:
    """Return a dictionary with ``function`` applied to every value."""
    return {key: function(value) for key, value in mapping.items()}


def without_keys(mapping: Mapping[Key, Value], excluded: Iterable[Key]) -> dict[Key, Value]:
    """Return a shallow copy without the specified keys."""
    excluded_keys = set(excluded)
    return {key: value for key, value in mapping.items() if key not in excluded_keys}
