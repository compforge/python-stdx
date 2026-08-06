"""Small, dependency-free helpers for working with iterables."""

from collections import defaultdict
from collections.abc import Callable, Hashable, Iterable, Iterator
from typing import TypeVar

Item = TypeVar("Item")
Default = TypeVar("Default")
GroupKey = TypeVar("GroupKey", bound=Hashable)


def first(iterable: Iterable[Item], default: Default | None = None) -> Item | Default | None:
    """Return the first item, or ``default`` when the iterable is empty."""
    return next(iter(iterable), default)


def first_where(
    iterable: Iterable[Item],
    predicate: Callable[[Item], bool],
    default: Default | None = None,
) -> Item | Default | None:
    """Return the first matching item, or ``default`` when none match."""
    return next((item for item in iterable if predicate(item)), default)


def group_by(iterable: Iterable[Item], key: Callable[[Item], GroupKey]) -> dict[GroupKey, list[Item]]:
    """Group items by a derived key while preserving input order within groups."""
    groups: defaultdict[GroupKey, list[Item]] = defaultdict(list)
    for item in iterable:
        groups[key(item)].append(item)
    return dict(groups)


def unique(
    iterable: Iterable[Item],
    *,
    key: Callable[[Item], Hashable] | None = None,
) -> Iterator[Item]:
    """Yield the first item for each identity while preserving input order."""
    seen: set[object] = set()
    for item in iterable:
        identity = key(item) if key is not None else item
        if identity in seen:
            continue
        seen.add(identity)
        yield item


def has_duplicates(
    iterable: Iterable[Item],
    *,
    key: Callable[[Item], Hashable] | None = None,
) -> bool:
    """Return whether two items share the same identity."""
    seen: set[object] = set()
    for item in iterable:
        identity = key(item) if key is not None else item
        if identity in seen:
            return True
        seen.add(identity)
    return False
